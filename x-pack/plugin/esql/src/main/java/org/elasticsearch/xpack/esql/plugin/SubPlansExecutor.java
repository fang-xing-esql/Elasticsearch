/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.SubPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plugin.ComputeService.LOCAL_CLUSTER;

/**
 * Executes an immutable nested-subplan topology. Merge segments own their local exchanges and run outside the branch-parallel limit;
 * up to {@code branchParallelDegree} leaf producer plans run concurrently using a self-refilling dispatch loop.
 */
final class SubPlansExecutor {
    private static final Logger LOGGER = LogManager.getLogger(SubPlansExecutor.class);

    private final ComputeService computeService;
    private final ExchangeService exchangeService;
    private final Executor searchExecutor;
    private final String sessionId;
    // Unique per executor instance. ComputeService.execute runs multiple times with the same sessionId within one
    // query (once per subquery round plus the main plan), while sink deregistration on the success path is deferred
    // until the handler drains. Deriving child exchange ids from a fresh child session prevents a later round from
    // colliding with a not-yet-deregistered handler of an earlier round.
    private final String sessionPrefix;
    private final CancellableTask rootTask;
    private final EsqlFlags flags;
    private final Configuration configuration;
    private final FoldContext foldContext;
    private final EsqlExecutionInfo execInfo;
    private final Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses;
    private final QueryPragmas queryPragmas;
    /**
     * Rollback ledger for phase 1: every {@link MergeContext} whose {@link ExchangeSourceHandler} has been registered with the
     * {@link ExchangeService}, in registration order.
     * <p>
     * Phase 1 registers exchanges as a side effect while the tree is still half-built, so when {@link #buildSubPlanContext} throws
     * partway through there is no tree to walk to find what was registered - this list is how {@link #cleanupUnstarted} finds it.
     * Emptied as soon as phase 1 succeeds: from then on the tree owns these contexts and the abort and terminal paths release them,
     * so a second reference would only pin them for the life of the query.
     */
    private final List<MergeContext> unstartedMergeContexts = new ArrayList<>();
    /**
     * Rollback ledger for phase 1, the {@link ParentSink} counterpart of {@link #unstartedMergeContexts}: one entry per child of
     * every merge node, in creation order. Each entry holds whatever that child has already acquired - a registered
     * {@link ExchangeSinkHandler} for a nested merge, or a keep-alive ref on the parent's exchange source for a leaf. The root merge
     * is absent, since it writes into {@code collectedPages} rather than into a sink. Same lifecycle as
     * {@link #unstartedMergeContexts}: read only by {@link #cleanupUnstarted}, emptied once phase 1 succeeds.
     */
    private final List<ParentSink> unstartedParentSinks = new ArrayList<>();
    // Flat list of leaves populated during startMerge; dispatched after all merges are wired.
    private final List<ScheduledLeaf> scheduledLeaves = new ArrayList<>();
    private final AtomicInteger nextLeafIndex = new AtomicInteger();

    SubPlansExecutor(
        ComputeService computeService,
        ExchangeService exchangeService,
        Executor searchExecutor,
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses
    ) {
        this.computeService = computeService;
        this.exchangeService = exchangeService;
        this.searchExecutor = searchExecutor;
        this.sessionId = sessionId;
        this.sessionPrefix = computeService.newChildSession(sessionId);
        this.rootTask = rootTask;
        this.flags = flags;
        this.configuration = configuration;
        this.foldContext = foldContext;
        this.execInfo = execInfo;
        this.initialClusterStatuses = initialClusterStatuses;
        this.queryPragmas = configuration.pragmas();
    }

    /**
     * Executes a nested {@link SubPlan.Merge} topology in three sequential phases.
     * <p>
     * <b>Phase 1 – register exchanges ({@code buildSubPlanContext}):</b> recursively walks the {@link SubPlan} tree and registers an
     * {@link ExchangeSourceHandler} for every {@link SubPlan.Merge} node and an {@link ExchangeSinkHandler} for every nested-merge
     * child. Leaf children only reserve a keep-alive ref on their parent source; their sink handlers are created lazily at dispatch
     * time (see {@link ParentSink}), so leaves queued behind {@code branchParallelDegree} are invisible to the exchange service's
     * inactive-sink reaper. This phase is synchronous and has no async side effects, so any exception partway through is caught and
     * rolled back by {@link #cleanupUnstarted} before propagating to {@code listener}.
     * <p>
     * <b>Phase 2 – wire merge segments ({@code startMerge}):</b> top-down recursive walk that calls
     * {@code ComputeService.runCompute} for each merge node and accumulates leaves into {@link #scheduledLeaves}. All merge segments
     * are wired before any leaf is dispatched, so no leaf can complete and attempt to read from an exchange source that has not yet
     * been set up.
     * <p>
     * <b>Phase 3 – dispatch leaves:</b> launches {@code min(branchParallelDegree, leafCount)} initial workers. Each worker calls
     * {@link #tryExecuteNextLeaf}, which atomically claims the next leaf from {@link #scheduledLeaves} via {@link #nextLeafIndex}
     * and re-invokes itself on completion, so the concurrency level stays at most {@code branchParallelDegree} throughout.
     * <p>
     * Example — two-leaf topology with {@code branchParallelDegree=1}:
     * <pre>
     * SubPlan.Merge
     * ├─ Leaf(LeafA)
     * └─ Leaf(LeafB)
     *
     * Phase 1: registers ExchangeSourceHandler for the root merge; reserves keep-alive refs for LeafA and LeafB
     *          (their sink handlers are created when they are dispatched in phase 3).
     * Phase 2: startMerge starts runCompute for the root merge; adds LeafA and LeafB to scheduledLeaves.
     * Phase 3: dispatches 1 initial worker → claims LeafA (index 0) → on completion claims LeafB (index 1)
     *          → on completion tryExecuteNextLeaf() sees index 2 ≥ size and exits.
     * </pre>
     */
    void execute(SubPlan.Merge executionPlan, PlanTimeProfile planTimeProfile, ActionListener<Result> listener) {
        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());

        // Phase 1: register all exchanges. If buildSubPlanContext throws partway, some ExchangeSourceHandlers
        // and ExchangeSinkHandlers may already be registered; cleanupUnstarted rolls them back before failing.
        final MergeContext root;
        try {
            root = buildSubPlanContext(executionPlan, null, null, collectedPages);
        } catch (Exception e) {
            cleanupUnstarted(e);
            listener.onFailure(e);
            return;
        }

        // Phase 1 succeeded. cleanupUnstarted is no longer reachable; release the references it held.
        int mergeCount = unstartedMergeContexts.size();
        unstartedMergeContexts.clear();
        unstartedParentSinks.clear();

        // On failure, release any pages already collected to avoid memory leaks.
        ActionListener<DriverCompletionInfo> completionListener = ActionListener.wrap(profiles -> {
            execInfo.markEndQuery();
            listener.onResponse(new Result(root.plan.output(), collectedPages, null, configuration, profiles, execInfo));
        }, e -> {
            collectedPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
            listener.onFailure(e);
        });

        // One RunOnce shared across the entire query: cancelTaskAndDescendants must fire at most once
        // regardless of how many merge nodes fail. Creating one per merge node (depth D) would fire
        // any non-idempotent side-effect D times on a cascading failure.
        final Runnable cancelOnFailure = computeService.cancelQueryOnFailure(rootTask);

        // Phase 2: wire all merge segments and collect leaves into scheduledLeaves.
        startMerge(root, planTimeProfile, completionListener, cancelOnFailure);

        LOGGER.debug(
            "topology built: [{}] merge nodes, [{}] leaves, branchParallelDegree=[{}]",
            mergeCount,
            scheduledLeaves.size(),
            queryPragmas.branchParallelDegree()
        );

        // Phase 3: dispatch the initial wave; each worker self-refills its slot on completion.
        int initial = Math.min(queryPragmas.branchParallelDegree(), scheduledLeaves.size());
        for (int i = 0; i < initial; i++) {
            tryExecuteNextLeaf();
        }
    }

    /**
     * Recursively converts a {@link SubPlan.Merge} tree into a {@link MergeContext} tree, registering all merge exchanges as a side
     * effect. For each {@link SubPlan.Merge} node it creates a {@link MergeContext} with an {@link ExchangeSourceHandler} (the
     * consumer side), and for each child it creates a {@link ParentSink} (the producer side): eager, backed by a registered
     * {@link ExchangeSinkHandler} wired into the parent's source, for nested-merge children; lazy, holding only a keep-alive ref on
     * the parent's source, for leaf children (see {@link ParentSink}). A child that is a {@link SubPlan.Leaf} becomes a
     * {@link LeafContext}; a child that is a {@link SubPlan.Merge} recurses. The {@code emptySink} keeps the source alive while
     * children are being wired, preventing premature completion.
     * <p>
     * Example — input {@link SubPlan} tree and the resulting {@link MergeContext} tree (sessionId = "s", sessionPrefix = "s/1",
     * path = null for root):
     * <pre>
     * Input SubPlan:
     *   Merge(plan=LimitExec→ExchangeSourceExec)
     *   ├─ Leaf(ExchangeSinkExec→LeafA)
     *   └─ Merge(plan=ExchangeSinkExec→ExchangeSourceExec)
     *      ├─ Leaf(ExchangeSinkExec→LeafB)
     *      └─ Leaf(ExchangeSinkExec→LeafC)
     *
     * Output MergeContext tree:
     *   MergeContext(path=null, exchangeId="s", plan=OutputExec→LimitExec→ExchangeSourceExec,
     *                exchangeSource=src0, parentSink=null)
     *   ├─ LeafContext(path="subplan-0", parentSink=ParentSink(id="s/1/subplan-0", lazy→src0))
     *   └─ MergeContext(path="subplan-1", exchangeId="s/1/subplan-1/merge",
     *                   plan=ExchangeSinkExec→ExchangeSourceExec,
     *                   exchangeSource=src1, parentSink=ParentSink(id="s/1/subplan-1", sink1→src0))
     *      ├─ LeafContext(path="subplan-1.subplan-0", parentSink=ParentSink(id="s/1/subplan-1.subplan-0", lazy→src1))
     *      └─ LeafContext(path="subplan-1.subplan-1", parentSink=ParentSink(id="s/1/subplan-1.subplan-1", lazy→src1))
     *
     * Side effects registered in ExchangeService:
     *   src0  registered under "s"                (root merge source; bare sessionId so finishSessionEarly finds it)
     *   sink1 registered under "s/1/subplan-1"    (inner merge's sink → src0)
     *   src1  registered under "s/1/subplan-1/merge" (inner merge source)
     * Leaf sinks ("s/1/subplan-0", "s/1/subplan-1.subplan-0", "s/1/subplan-1.subplan-1") are registered lazily
     * when each leaf is dispatched in phase 3.
     * </pre>
     * <p>
     * The root node wraps its plan in an {@code OutputExec} to collect final pages into {@code collectedPages}. All other merge nodes
     * use their plan as-is (already contains an {@code ExchangeSinkExec} at the top that feeds the parent source).
     */
    private MergeContext buildSubPlanContext(SubPlan.Merge executionPlan, String path, ParentSink parentSink, List<Page> collectedPages) {
        boolean root = path == null;
        // The root source must stay registered under the bare sessionId: ExchangeService.finishSessionEarly (async stop)
        // looks it up by that key. All other ids derive from the per-executor sessionPrefix to stay unique across rounds.
        String exchangeId = root ? sessionId : nodeSessionId(path) + "/merge";
        String computeSessionId = root ? sessionPrefix : exchangeId;
        ExchangeSourceHandler exchangeSource = new ExchangeSourceHandler(queryPragmas.exchangeBufferSize(), searchExecutor);
        exchangeService.addExchangeSourceHandler(exchangeId, exchangeSource);
        // Root segment collects final pages via OutputExec; nested segments use their plan as-is.
        PhysicalPlan segmentPlan = root ? new OutputExec(executionPlan.plan(), collectedPages::add) : executionPlan.plan();
        var context = new MergeContext(segmentPlan, path, exchangeId, computeSessionId, exchangeSource, parentSink, new ArrayList<>());
        unstartedMergeContexts.add(context);

        // emptySink keeps the source alive while children are being wired.
        try (var emptySink = exchangeSource.addEmptySink()) {
            for (int i = 0; i < executionPlan.children().size(); i++) {
                buildChildContext(executionPlan.children().get(i), childPath(path, i), exchangeSource, context, collectedPages);
            }
        }
        return context;
    }

    /**
     * Creates the {@link SubPlanContext} for one child of a merge node and appends it to the parent's {@code children} list.
     * A nested-merge child gets an eager {@link ParentSink} wired into the parent's {@link ExchangeSourceHandler}; a leaf child
     * gets a lazy {@link ParentSink} that registers nothing until the leaf is dispatched (see {@link ParentSink}).
     * <p>
     * Each {@link ParentSink} is recorded in {@link #unstartedParentSinks} immediately after it is created, so an exception anywhere
     * later in phase 1 is rolled back by {@link #cleanupUnstarted}. Only an {@link Error} between creating a sink and recording it
     * can escape that, and phase 1's caller does not recover from those either.
     */
    private void buildChildContext(
        SubPlan child,
        String childPath,
        ExchangeSourceHandler parentSource,
        MergeContext parent,
        List<Page> collectedPages
    ) {
        String childSessionId = nodeSessionId(childPath);
        if (child instanceof SubPlan.Merge merge) {
            var sinkHandler = exchangeService.createSinkHandler(childSessionId, queryPragmas.exchangeBufferSize());
            var childSink = new ParentSink(childSessionId, sinkHandler);
            unstartedParentSinks.add(childSink);
            parentSource.addRemoteSink(sinkHandler::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
            parent.children.add(buildSubPlanContext(merge, childPath, childSink, collectedPages));
        } else {
            var childSink = new ParentSink(childSessionId, parentSource);
            unstartedParentSinks.add(childSink);
            parent.children.add(new LeafContext(child.plan(), childPath, childSink));
        }
    }

    /**
     * Starts execution of a merge segment and recurses synchronously into child merges before returning. Leaves are not dispatched here;
     * they are collected into {@link #scheduledLeaves} and dispatched by the caller after all merges are wired.
     * <p>
     * For each merge node this method:
     * <ol>
     *   <li>Calls {@code ComputeService.runCompute} to start the local coordinator segment (reads from {@code exchangeSource}, writes
     *       into {@code parentSink}).</li>
     *   <li>Recurses into child {@link MergeContext} nodes via {@link #startChildContext}.</li>
     *   <li>Collects child {@link LeafContext} nodes into {@link #scheduledLeaves} via {@link #startChildContext}.</li>
     * </ol>
     * <p>
     * <b>Guard-ref idiom.</b> {@link ComputeListener} tracks how many outstanding refs exist and fires {@code terminalListener} when
     * the count reaches zero. This method acquires one ref per concern: a {@code guard} ref it holds for its entire body, a
     * {@code segmentListener} ref for {@code runCompute}, and one {@code childListener} ref per child. When everything succeeds,
     * {@code guard.onResponse(null)} releases the guard and the count drops as each async operation completes. When something fails
     * synchronously, the inner catch block completes all acquired refs before releasing the guard in a {@code finally}, guaranteeing
     * that {@code terminalListener} fires exactly once with the correct failure.
     * <p>
     * {@code segmentListener} and {@code childListeners} are acquired inside the inner try block so that a mid-loop
     * {@link ComputeListener#acquireCompute()} failure is caught by the inner catch, which has the already-acquired refs in scope
     * and can release them before releasing the guard. Only {@code guard} is acquired outside the inner try; if
     * {@link ComputeListener#acquireAvoid()} itself fails, the outer backstop fires (at that point no inner refs exist).
     * <p>
     * Try-with-resources {@code close()} runs before the enclosing {@code catch} clause (Java Language Specification §14.20.3.2), so
     * {@link ComputeListener#close()} releases the listener's own initial ref before the catch can record anything. The {@code guard}
     * is therefore the only ref this method provably owns throughout the body.
     * <p>
     * Example — two-child merge (one leaf, one nested merge) with ref counts:
     * <pre>
     * MergeContext(root)
     * ├─ LeafContext(LeafA)         → childListeners[0]
     * └─ MergeContext(inner)        → childListeners[1]
     *    └─ LeafContext(LeafB)      → inner.childListeners[0]
     *
     * startMerge(root):
     *   ComputeListener refs: 1 (initial) + guard + segmentListener + childListeners[0] + childListeners[1] = 5
     *   runCompute(root.plan) → segmentListener completes async
     *   startChildContext(LeafA,  childListeners[0]) → scheduledLeaves.add(ScheduledLeaf(LeafA, childListeners[0]))
     *   startChildContext(inner,  childListeners[1]) → startMerge(inner, childListeners[1])  [recursive]
     *     ComputeListener refs (inner): 1 + guard + segmentListener + childListeners[0] = 4
     *     runCompute(inner.plan) → segmentListener completes async
     *     startChildContext(LeafB, childListeners[0]) → scheduledLeaves.add(ScheduledLeaf(LeafB, childListeners[0]))
     *     guard.onResponse(null) → inner refs: 3
     *   guard.onResponse(null) → root refs: 4
     *
     * scheduledLeaves = [LeafA, LeafB]  (ready for phase-3 dispatch)
     * </pre>
     */
    private void startMerge(
        MergeContext mergeContext,
        PlanTimeProfile planTimeProfile,
        ActionListener<DriverCompletionInfo> completionListener,
        Runnable cancelOnFailure
    ) {
        LOGGER.debug(
            "starting merge segment [{}] with [{}] children",
            mergeContext.path == null ? "main" : mergeContext.path,
            mergeContext.children.size()
        );
        // The merge tree is started synchronously from execute(), before any leaf is dispatched. The CAS must run outside
        // the assert: abortUnstartedSubPlanContext relies on `started` being set to skip merges that are already running,
        // in production builds (assertions disabled) as much as in tests.
        boolean firstStart = mergeContext.started.compareAndSet(false, true);
        assert firstStart : "merge [" + mergeContext.path + "] started twice";
        final ActionListener<DriverCompletionInfo> terminalListener = mergeTerminalListener(mergeContext, completionListener);
        try (var computeListener = new ComputeListener(cancelOnFailure, terminalListener)) {
            final ActionListener<Void> guard = ActionListener.notifyOnce(computeListener.acquireAvoid());
            // segmentListener and childListeners are acquired inside the inner try so that a mid-loop acquireCompute() failure
            // is caught by the inner catch, which has all already-acquired refs in scope and can release them.
            ActionListener<DriverCompletionInfo> segmentListener = null;
            final List<ActionListener<DriverCompletionInfo>> childListeners = new ArrayList<>(mergeContext.children.size());
            try {
                segmentListener = ActionListener.notifyOnce(computeListener.acquireCompute());
                for (int i = 0; i < mergeContext.children.size(); i++) {
                    childListeners.add(ActionListener.notifyOnce(computeListener.acquireCompute()));
                }
                // The root segment is this query's coordinator segment, so it reports the caller's PlanTimeProfile: that
                // instance holds the query-level logical/physical optimization time, and attaching it to a segment's
                // PlanProfile is the only way that time reaches the PROFILE output. This mirrors the single-plan
                // (SubPlan.Leaf) path in ComputeService.execute, which forwards the same instance.
                // Each nested merge segment gets its own PlanTimeProfile so that the PlanProfile attached to its
                // DriverCompletionInfo captures only that segment's local-optimization timing. A single shared
                // instance would accumulate every segment's timings, making all PlanProfiles show identical,
                // over-counted numbers in the PROFILE output.
                final PlanTimeProfile segmentProfile;
                if (mergeContext.path == null) {
                    segmentProfile = planTimeProfile;
                } else {
                    segmentProfile = planTimeProfile != null ? new PlanTimeProfile() : null;
                }
                // The plan ends in an ExchangeSourceExec that polls exchangeSource; output goes into parentSink (null for root).
                computeService.runCompute(
                    rootTask,
                    new ComputeContext(
                        mergeContext.computeSessionId,
                        mergeContext.path == null ? "main.final" : computeService.profileDescription(mergeContext.path, "merge"),
                        LOCAL_CLUSTER,
                        flags,
                        EmptyIndexedByShardId.instance(),
                        configuration,
                        foldContext,
                        mergeContext.exchangeSource::createExchangeSource,
                        mergeContext.parentSink == null ? null : () -> mergeContext.parentSink.handler.createExchangeSink(() -> {}),
                        false
                    ),
                    mergeContext.plan,
                    computeService.plannerSettings().get(),
                    LocalPhysicalOptimization.ENABLED,
                    segmentProfile,
                    segmentListener
                );

                for (int i = 0; i < mergeContext.children.size(); i++) {
                    startChildContext(mergeContext.children.get(i), childListeners.get(i), planTimeProfile, cancelOnFailure);
                }
                guard.onResponse(null);
            } catch (Exception e) {
                LOGGER.debug("synchronous failure starting merge segment [{}]", mergeContext.path == null ? "main" : mergeContext.path, e);
                try {
                    // 1. Complete acquired refs first: notifyOnce makes these no-ops if runCompute or a concurrent completion already
                    // absorbed them. segmentListener may be null if acquireCompute() itself failed; childListeners contains only the
                    // refs that were successfully acquired before the failure. Guard remains held, preventing premature termination.
                    if (segmentListener != null) {
                        segmentListener.onFailure(e);
                    }
                    childListeners.forEach(l -> l.onFailure(e));
                    // 2. Side-effecting cleanup — may throw; abortChildren is idempotent via atomic boolean guards.
                    abortChildren(mergeContext, e);
                } catch (Exception cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                    assert false : cleanupFailure;
                } finally {
                    // 3. Releasing the guard latches FailureCollector and fires terminalListener (removeExchangeSource, finishEarly,
                    // finishParentSink, user-visible completion) and cancelQueryOnFailure.
                    guard.onFailure(e);
                }
            }
        } catch (Exception e) {
            // Backstop: ComputeListener construction or acquireAvoid() (guard) failed before any inner ref was acquired.
            // mergeContext.started is already set, so the parent's abortChildren would no-op — clean up this subtree explicitly.
            LOGGER.debug(
                "failure initialising ComputeListener for merge segment [{}]",
                mergeContext.path == null ? "main" : mergeContext.path,
                e
            );
            cancelOnFailure.run();
            abortChildren(mergeContext, e);
            // terminalListener is notifyOnce'd; it performs removeExchangeSource + finishEarly + finishParentSink.
            terminalListener.onFailure(e);
        }
    }

    /** Recurses into a child {@link MergeContext} via {@link #startMerge}, or adds a child {@link LeafContext} to
     * {@link #scheduledLeaves}. */
    private void startChildContext(
        SubPlanContext child,
        ActionListener<DriverCompletionInfo> childListener,
        PlanTimeProfile planTimeProfile,
        Runnable cancelOnFailure
    ) {
        switch (child) {
            case MergeContext merge -> startMerge(merge, planTimeProfile, childListener, cancelOnFailure);
            case LeafContext leaf -> scheduledLeaves.add(new ScheduledLeaf(leaf, childListener));
            default -> throw new IllegalStateException("unexpected SubPlanContext type: " + child.getClass());
        }
    }

    /**
     * Builds the {@link ComputeListener} terminal listener for a merge segment. When the listener fires (either success or failure),
     * it deregisters the merge's exchange source and signals the merge's parent sink, then forwards to {@code completionListener}.
     * <p>
     * On success: {@link #removeExchangeSource} deregisters the source; {@link #finishParentSink} signals the parent sink that all
     * data has been written (the parent's {@link ExchangeSourceHandler} will see EOF after the sink drains).
     * <p>
     * On failure: the exchange source is also drained via {@link ExchangeSourceHandler#finishEarly} before the sink is signalled,
     * so any reader blocked on this source is unblocked and receives the failure.
     * <p>
     * Wrapped in {@link ActionListener#notifyOnce} so that concurrent completion paths (e.g. a race between the merge segment
     * completing normally and a child failing) fire the downstream listener at most once.
     *
     * @param mergeContext    the merge node whose resources should be released on completion
     * @param completionListener the listener to forward the final {@link DriverCompletionInfo} (or failure) to
     * @return a notifyOnce-wrapped terminal listener
     */
    private ActionListener<DriverCompletionInfo> mergeTerminalListener(
        MergeContext mergeContext,
        ActionListener<DriverCompletionInfo> completionListener
    ) {
        return ActionListener.notifyOnce(ActionListener.wrap(completionInfo -> {
            removeExchangeSource(mergeContext);
            finishParentSink(mergeContext.parentSink, null);
            completionListener.onResponse(completionInfo);
        }, e -> {
            removeExchangeSource(mergeContext);
            mergeContext.exchangeSource.finishEarly(true, ActionListener.noop());
            finishParentSink(mergeContext.parentSink, e);
            completionListener.onFailure(e);
        }));
    }

    /**
     * Atomically claims the next leaf from {@link #scheduledLeaves} and executes it. If the index is beyond the list (all leaves
     * claimed), returns immediately. Each executing leaf calls this method as its {@code onDone} callback, so the number of
     * concurrently running leaves stays at most {@code branchParallelDegree} throughout the query.
     */
    private void tryExecuteNextLeaf() {
        int index = nextLeafIndex.getAndIncrement();
        if (index >= scheduledLeaves.size()) {
            return;
        }
        executeLeaf(scheduledLeaves.get(index), this::tryExecuteNextLeaf);
    }

    /**
     * Dispatches a single leaf to {@link ComputeService#executePlan}. If the root task has already been cancelled, or the leaf's
     * merge segment already aborted it during phase 2, skips dispatch. On completion (success or failure), {@code onDone} is
     * invoked so the caller can claim the next leaf.
     * <p>
     * The leaf's exchange sink is created here, via {@link ParentSink#attach}, not in phase 1: an idle registered sink handler
     * would be reaped by the exchange service's inactive-sink reaper while the leaf waits behind {@code branchParallelDegree}.
     * <p>
     * A synchronous throw from {@code attach} or {@code executePlan} is routed to {@link #finishLeaf}: refill dispatches run on
     * the search executor, where an escaping exception would be swallowed and the leaf's listener — a {@code ComputeListener}
     * ref — would never complete, hanging the query. {@code finishLeaf} is safe to call from the catch even if {@code executePlan}
     * already notified its listener before throwing: the leaf listener is notifyOnce-wrapped and {@link #finishParentSink} is
     * CAS-guarded. The refill itself is wrapped in a {@link RunOnce} so the slot cannot be refilled twice by one leaf.
     *
     * @param scheduledLeaf the leaf to dispatch, containing its plan and parent sink
     * @param onDone        callback invoked after the leaf finishes (used for self-refilling dispatch)
     */
    private void executeLeaf(ScheduledLeaf scheduledLeaf, Runnable onDone) {
        LeafContext leafContext = scheduledLeaf.leafContext;
        ParentSink parentSink = leafContext.parentSink;
        LOGGER.debug("dispatching leaf [{}]", leafContext.path);
        Runnable onDoneOnce = new RunOnce(() -> submitOnDone(onDone));
        if (rootTask.notifyIfCancelled(ActionListener.wrap(ignored -> {}, e -> {
            finishLeaf(scheduledLeaf, null, e);
            // All paths (cancellation, skip, success, failure) use submitOnDone rather than calling
            // onDone inline. If executePlan or notifyIfCancelled completes before returning, the
            // listener fires on the current thread, and a direct onDone.run() would recurse through
            // the entire remaining queue (tryExecuteNextLeaf → executeLeaf → … → onDone.run() → …),
            // overflowing the stack when many leaves are queued.
            onDoneOnce.run();
        }))) {
            return;
        }
        if (parentSink.finished.get()) {
            // The leaf's merge segment failed and aborted this sink during phase 2, which also completed the
            // leaf's listener. Skip dispatch and just refill the slot.
            onDoneOnce.run();
            return;
        }
        try {
            Supplier<ExchangeSink> exchangeSinkSupplier = parentSink.attach();
            computeService.executePlan(
                parentSink.sessionId,
                rootTask,
                flags,
                leafContext.plan,
                configuration,
                foldContext,
                execInfo,
                leafContext.path,
                ActionListener.wrap(result -> {
                    finishLeaf(scheduledLeaf, result.completionInfo(), null);
                    onDoneOnce.run();
                }, e -> {
                    finishLeaf(scheduledLeaf, null, e);
                    onDoneOnce.run();
                }),
                exchangeSinkSupplier,
                initialClusterStatuses,
                configuration.profile() ? new PlanTimeProfile() : null
            );
        } catch (Exception e) {
            finishLeaf(scheduledLeaf, null, e);
            onDoneOnce.run();
        }
    }

    /**
     * Submits {@code onDone} to the search executor, breaking the call chain that would otherwise
     * recurse synchronously through the entire remaining leaf queue when a leaf completes inline.
     * All dispatch contexts — cancellation, skip, success, and failure — use this method so that
     * each subsequent leaf is dispatched on a fresh stack frame.
     * <p>
     * The refill is force-executed: dropping it under transient queue pressure would either strand
     * every leaf still queued behind it (hang) or require failing them all (failing the query for a
     * momentarily full queue). One short dispatch task per completed leaf is bounded work. Rejection
     * therefore only happens on executor shutdown, where draining the remaining leaves via
     * {@link #failRemainingLeaves} is the right response: the query terminates with an error rather
     * than hanging indefinitely.
     */
    private void submitOnDone(Runnable onDone) {
        var refill = new AbstractRunnable() {
            @Override
            protected void doRun() {
                onDone.run();
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }

            @Override
            public void onRejection(Exception e) {
                failRemainingLeaves(e);
            }

            @Override
            public void onFailure(Exception e) {
                // executeLeaf routes synchronous throws to finishLeaf, so onDone must not throw; drain as a backstop.
                assert false : e;
                failRemainingLeaves(e);
            }
        };
        try {
            searchExecutor.execute(refill);
        } catch (Exception e) {
            // EsThreadPoolExecutor routes rejection to onRejection above; this covers plain executors that throw
            // synchronously from execute(). failRemainingLeaves claims leaves exclusively, so double entry is harmless.
            failRemainingLeaves(e);
        }
    }

    /**
     * Atomically claims every undispatched leaf and reports {@code cause} to its listener,
     * allowing the {@link ComputeListener} to reach zero and the terminal listener to fire.
     * Called only when the search executor is shutting down (or a non-standard executor threw)
     * and the slot's self-refilling dispatch chain would otherwise be permanently broken.
     */
    private void failRemainingLeaves(Exception cause) {
        int index;
        while ((index = nextLeafIndex.getAndIncrement()) < scheduledLeaves.size()) {
            ScheduledLeaf scheduledLeaf = scheduledLeaves.get(index);
            try {
                finishLeaf(scheduledLeaf, null, cause);
            } catch (Exception e) {
                // Keep draining: a throw for one leaf must not strand the leaves after it, whose
                // ComputeListener refs would never release and would hang the query.
                LOGGER.warn("failed to fail leaf [{}]", scheduledLeaf.leafContext.path, e);
                assert false : e;
            }
        }
    }

    /**
     * Signals the leaf's parent sink and then forwards the result to the leaf's {@link ScheduledLeaf#listener}.
     * Always called after {@link ComputeService#executePlan} completes, whether successfully or not.
     *
     * @param scheduledLeaf  the leaf that just finished
     * @param completionInfo profiling data from the leaf's drivers; {@code null} on failure
     * @param failure        the exception if the leaf failed; {@code null} on success
     */
    private void finishLeaf(ScheduledLeaf scheduledLeaf, DriverCompletionInfo completionInfo, Exception failure) {
        if (failure == null) {
            finishParentSink(scheduledLeaf.leafContext.parentSink, null);
            scheduledLeaf.listener.onResponse(completionInfo);
        } else {
            finishParentSink(scheduledLeaf.leafContext.parentSink, failure);
            scheduledLeaf.listener.onFailure(failure);
        }
    }

    /**
     * Aborts a merge subtree that has not yet been started by {@link #startMerge}. Idempotent: a CAS on {@link MergeContext#started}
     * ensures at most one caller performs the abort. If the merge has already started, its own terminal listener will clean up its
     * resources, so this method returns immediately.
     * <p>
     * When the CAS succeeds, recursively aborts all children via {@link #abortChildren}, deregisters the exchange source, drains the
     * source via {@link ExchangeSourceHandler#finishEarly}, and signals the parent sink with the failure.
     *
     * @param mergeContext the merge node to abort
     * @param failure      the exception to propagate to the parent sink and any child sinks
     */
    private void abortUnstartedSubPlanContext(MergeContext mergeContext, Exception failure) {
        if (mergeContext.started.compareAndSet(false, true) == false) {
            return;
        }
        LOGGER.debug("aborting unstarted merge subtree [{}]", mergeContext.path);
        abortChildren(mergeContext, failure);
        removeExchangeSource(mergeContext);
        mergeContext.exchangeSource.finishEarly(true, ActionListener.noop());
        finishParentSink(mergeContext.parentSink, failure);
    }

    /**
     * Finishes the exchange sinks this merge registered for its children. Idempotent: nested merges that have already started are
     * skipped by their {@code started} CAS and will clean themselves up through their own terminal listener; leaves are skipped by
     * {@code ParentSink.finished}.
     */
    private void abortChildren(MergeContext mergeContext, Exception failure) {
        for (SubPlanContext child : mergeContext.children) {
            switch (child) {
                case MergeContext merge -> abortUnstartedSubPlanContext(merge, failure);
                case LeafContext leaf -> finishParentSink(leaf.parentSink, failure);
                default -> throw new IllegalStateException("unexpected SubPlanContext type: " + child.getClass());
            }
        }
    }

    /**
     * Deregisters the {@link ExchangeSourceHandler} for {@code mergeContext} from the {@link ExchangeService}. Idempotent via a CAS on
     * {@link MergeContext#sourceRemoved}: only the first caller performs the removal. Called from both the normal terminal listener and
     * the abort path so that the exchange service does not hold stale handlers after the query ends.
     */
    private void removeExchangeSource(MergeContext mergeContext) {
        if (mergeContext.sourceRemoved.compareAndSet(false, true)) {
            exchangeService.removeExchangeSourceHandler(mergeContext.exchangeId);
        }
    }

    /**
     * Signals a {@link ParentSink} that its producer has finished. Idempotent via a CAS on {@link ParentSink#finished}: only the first
     * caller performs the signal. A {@code null} {@code parentSink} (root merge, which writes directly into {@code collectedPages}) is
     * treated as a no-op.
     * <p>
     * For a lazy leaf sink, releases the keep-alive ref on the parent source; if the leaf was never dispatched (no handler was ever
     * created), nothing is registered in the {@link ExchangeService} and there is nothing further to do. Otherwise: on success, the
     * sink handler is finished asynchronously after all in-flight pages have drained ({@link
     * ExchangeSinkHandler#addCompletionListener}). On failure, the sink handler is finished immediately so the parent's
     * {@link ExchangeSourceHandler} receives the error without waiting for pages that will never arrive.
     *
     * @param parentSink the sink to signal; {@code null} for the root merge
     * @param failure    the exception if the producer failed; {@code null} on success
     */
    private void finishParentSink(ParentSink parentSink, Exception failure) {
        if (parentSink == null || parentSink.finished.compareAndSet(false, true) == false) {
            return;
        }
        // Null for eager (merge) sinks; releaseOnce makes this idempotent with the release in attach().
        Releasables.close(parentSink.pendingRef);
        ExchangeSinkHandler handler = parentSink.handler;
        if (handler == null) {
            return;
        }
        if (failure == null) {
            handler.addCompletionListener(ActionListener.running(() -> exchangeService.finishSinkHandler(parentSink.sessionId, null)));
        } else {
            exchangeService.finishSinkHandler(parentSink.sessionId, failure);
        }
    }

    /**
     * Rolls back all exchange registrations performed during a failed {@link #buildSubPlanContext} call. Iterates
     * {@link #unstartedMergeContexts} and {@link #unstartedParentSinks} in reverse registration order (innermost first) so that
     * exchange handlers are removed before their parents are signalled. Called only from the {@link #execute} catch block when
     * phase 1 fails before any merge has been started.
     *
     * @param failure the exception that caused the build to fail, propagated to each parent sink
     */
    private void cleanupUnstarted(Exception failure) {
        for (MergeContext mergeContext : unstartedMergeContexts.reversed()) {
            removeExchangeSource(mergeContext);
            mergeContext.exchangeSource.finishEarly(true, ActionListener.noop());
        }
        for (ParentSink parentSink : unstartedParentSinks.reversed()) {
            finishParentSink(parentSink, failure);
        }
    }

    private String nodeSessionId(String path) {
        return sessionPrefix + "/" + path;
    }

    private static String childPath(String parentPath, int child) {
        String childName = "subplan-" + child;
        return parentPath == null ? childName : parentPath + "." + childName;
    }

    /**
     * Common base for the two kinds of execution node that {@link #buildSubPlanContext} produces. Every node has a physical plan to
     * execute, a path that identifies it within the query (used for session IDs and profiling), and a {@link ParentSink} that the node
     * writes its output into ({@code null} for the root merge, which writes directly into {@code collectedPages}).
     */
    private abstract static class SubPlanContext {
        final PhysicalPlan plan;
        final String path;
        final ParentSink parentSink;

        private SubPlanContext(PhysicalPlan plan, String path, ParentSink parentSink) {
            this.plan = plan;
            this.path = path;
            this.parentSink = parentSink;
        }
    }

    /**
     * An immutable descriptor for a leaf producer plan. Carries the three arguments that {@link #executeLeaf} needs to call
     * {@code ComputeService.executePlan}: the physical plan, the path, and the lazy {@link ParentSink} the leaf writes into.
     * Has no mutable state of its own; lifecycle is managed externally through {@link ScheduledLeaf}.
     */
    private static final class LeafContext extends SubPlanContext {
        private LeafContext(PhysicalPlan plan, String path, ParentSink parentSink) {
            super(plan, path, parentSink);
        }
    }

    /**
     * The stateful execution node for a coordinator merge segment. Built by {@link #buildSubPlanContext} and consumed by
     * {@link #startMerge}. Holds the wired exchange infrastructure and mutable lifecycle guards:
     * <ul>
     *   <li>{@code exchangeSource} — the {@link ExchangeSourceHandler} this segment reads from; children write into it via their
     *       {@link ParentSink}s.</li>
     *   <li>{@code children} — direct children, each either a {@link MergeContext} (another coordinator segment) or a
     *       {@link LeafContext} (a producer to dispatch); iterated by {@code startMerge} and {@code abortChildren}.</li>
     *   <li>{@code started} — atomic boolean guard: {@code compareAndSet(false, true)} ensures {@code startMerge} runs at most once
     *       per node even if concurrent failure paths race to abort it.</li>
     *   <li>{@code sourceRemoved} — atomic boolean guard: {@code compareAndSet(false, true)} ensures {@code removeExchangeSource} runs
     *       at most once per node.</li>
     * </ul>
     */
    private static final class MergeContext extends SubPlanContext {
        private final String exchangeId;
        private final String computeSessionId;
        private final ExchangeSourceHandler exchangeSource;
        private final List<SubPlanContext> children;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean sourceRemoved = new AtomicBoolean();

        private MergeContext(
            PhysicalPlan plan,
            String path,
            String exchangeId,
            String computeSessionId,
            ExchangeSourceHandler exchangeSource,
            ParentSink parentSink,
            List<SubPlanContext> children
        ) {
            super(plan, path, parentSink);
            this.exchangeId = exchangeId;
            this.computeSessionId = computeSessionId;
            this.exchangeSource = exchangeSource;
            this.children = children;
        }
    }

    /**
     * The producer-side of an exchange: the {@link ExchangeSinkHandler} that a child node (leaf or nested merge) writes its output
     * into, together with the session ID under which it is registered in {@link ExchangeService}. Created in
     * {@link #buildSubPlanContext} for every child of a merge node and stored in the child's {@link SubPlanContext#parentSink}.
     * <p>
     * Nested-merge sinks are <b>eager</b>: their handler is registered in phase 1 and their {@code runCompute} attaches an
     * {@link ExchangeSink} to it synchronously in phase 2, so the {@code InactiveSinksReaper} sees them as active. Leaf sinks are
     * <b>lazy</b>: a leaf can sit in {@link #scheduledLeaves} behind {@code branchParallelDegree} for longer than the reaper's
     * inactive interval, and an idle registered handler (no attached sink, empty buffer) would be reaped, silently dropping or
     * failing that branch. A lazy sink therefore registers nothing in phase 1; it only holds the parent source open through
     * {@code pendingRef} (an {@link ExchangeSourceHandler#addEmptySink} ref, playing the role the eagerly-registered remote sink
     * used to play) and creates the handler in {@link #attach} when the leaf is actually dispatched.
     * <p>
     * {@link #finishParentSink} uses the {@code finished} guard to ensure the handler is deregistered exactly once regardless of
     * whether the child completed successfully, failed, or was aborted. On success it waits for the handler to drain (via
     * {@link ExchangeSinkHandler#addCompletionListener}) before calling {@link ExchangeService#finishSinkHandler}; on failure it
     * deregisters immediately.
     */
    private final class ParentSink {
        private final String sessionId;
        private final AtomicBoolean finished = new AtomicBoolean();
        @Nullable
        private final ExchangeSourceHandler parentSource; // non-null only for lazy (leaf) sinks
        @Nullable
        private final Releasable pendingRef; // non-null only for lazy (leaf) sinks
        // Set in the constructor for eager (merge) sinks; published by attach() for lazy (leaf) sinks.
        private volatile ExchangeSinkHandler handler;

        /** Eager (nested merge): the handler is registered in phase 1 and wired into the parent source by the caller. */
        private ParentSink(String sessionId, ExchangeSinkHandler handler) {
            this.sessionId = sessionId;
            this.handler = handler;
            this.parentSource = null;
            this.pendingRef = null;
        }

        /** Lazy (leaf): no handler yet; {@code pendingRef} keeps the parent source open until {@link #attach} or abort. */
        private ParentSink(String sessionId, ExchangeSourceHandler parentSource) {
            this.sessionId = sessionId;
            this.parentSource = parentSource;
            this.pendingRef = Releasables.releaseOnce(parentSource.addEmptySink());
        }

        /**
         * Registers this leaf's {@link ExchangeSinkHandler} and wires it into the parent source. Called only from
         * {@link #executeLeaf}, on the thread that exclusively claimed the leaf, and only while {@code finished} is false
         * (aborts happen synchronously in phase 2, before any leaf is dispatched).
         *
         * @return the exchange-sink supplier to pass to {@code ComputeService.executePlan}
         */
        private Supplier<ExchangeSink> attach() {
            assert handler == null : "sink [" + sessionId + "] attached twice";
            assert finished.get() == false : "sink [" + sessionId + "] already finished";
            ExchangeSinkHandler attached = exchangeService.createSinkHandler(sessionId, queryPragmas.exchangeBufferSize());
            // Publish before addRemoteSink so every later finishParentSink call sees and deregisters the handler.
            handler = attached;
            parentSource.addRemoteSink(attached::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
            // addRemoteSink holds its own keep-alive ref on the parent source until the remote sink completes
            // (ExchangeSourceHandler wraps the fetcher in releaseAfter(..., addEmptySink())), so the phase-1
            // pending ref is no longer needed. releaseOnce makes this idempotent with finishParentSink.
            pendingRef.close();
            return () -> attached.createExchangeSink(() -> {});
        }
    }

    /**
     * Pairs a {@link LeafContext} with the {@link ActionListener} that must be notified when the leaf finishes. Populated into
     * {@link #scheduledLeaves} during {@link #startMerge} (phase 2) and consumed by {@link #tryExecuteNextLeaf} (phase 3).
     */
    private record ScheduledLeaf(LeafContext leafContext, ActionListener<DriverCompletionInfo> listener) {}

}
