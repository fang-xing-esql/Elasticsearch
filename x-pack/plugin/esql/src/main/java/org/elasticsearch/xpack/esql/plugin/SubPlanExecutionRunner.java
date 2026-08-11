/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.SubPlanExecutionPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.plugin.ComputeService.LOCAL_CLUSTER;

/**
 * Executes an immutable nested-subplan topology. Merge segments own their local exchanges and run without consuming branch permits;
 * one query-wide scheduler limits the leaf producer plans that feed those exchanges.
 */
final class SubPlanExecutionRunner {
    private final ComputeService computeService;
    private final ExchangeService exchangeService;
    private final Executor searchExecutor;
    private final String sessionId;
    private final CancellableTask rootTask;
    private final EsqlFlags flags;
    private final Configuration configuration;
    private final FoldContext foldContext;
    private final EsqlExecutionInfo execInfo;
    private final Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses;
    private final QueryPragmas queryPragmas;
    private final SubPlanLeafScheduler<ScheduledLeaf> scheduler;
    private final List<MergeRuntime> mergeRuntimes = new ArrayList<>();
    private final List<ParentSink> parentSinks = new ArrayList<>();

    SubPlanExecutionRunner(
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
        this.rootTask = rootTask;
        this.flags = flags;
        this.configuration = configuration;
        this.foldContext = foldContext;
        this.execInfo = execInfo;
        this.initialClusterStatuses = initialClusterStatuses;
        this.queryPragmas = configuration.pragmas();
        this.scheduler = new SubPlanLeafScheduler<>(
            queryPragmas.branchParallelDegree(),
            searchExecutor,
            this::executeLeaf,
            this::skipLeaf,
            this::rejectLeaf
        );
    }

    void execute(SubPlanExecutionPlan.Merge executionPlan, PlanTimeProfile planTimeProfile, ActionListener<Result> listener) {
        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        ActionListener<Result> finalListener = listener.delegateResponse((l, e) -> {
            collectedPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
            l.onFailure(e);
        });
        final MergeRuntime root;
        try {
            root = buildMergeRuntime(executionPlan, null, null, collectedPages);
        } catch (Exception e) {
            cleanupUnstarted(e);
            finalListener.onFailure(e);
            return;
        }

        rootTask.addListener(() -> scheduler.fail(rootTask.getTaskCancelledException()));
        ActionListener<DriverCompletionInfo> completionListener = finalListener.map(profiles -> {
            execInfo.markEndQuery();
            return new Result(root.plan.output(), collectedPages, null, configuration, profiles, execInfo);
        });
        startMerge(root, planTimeProfile, completionListener);
        scheduler.start();
    }

    private MergeRuntime buildMergeRuntime(
        SubPlanExecutionPlan.Merge executionPlan,
        String path,
        ParentSink parentSink,
        List<Page> collectedPages
    ) {
        boolean root = path == null;
        String exchangeId = root ? sessionId : nodeSessionId(path) + "/merge";
        String computeSessionId = root ? computeService.newChildSession(sessionId) : exchangeId;
        ExchangeSourceHandler exchangeSource = new ExchangeSourceHandler(queryPragmas.exchangeBufferSize(), searchExecutor);
        exchangeService.addExchangeSourceHandler(exchangeId, exchangeSource);
        PhysicalPlan segmentPlan = root ? new OutputExec(executionPlan.plan(), collectedPages::add) : executionPlan.plan();
        var runtime = new MergeRuntime(
            segmentPlan,
            path,
            exchangeId,
            computeSessionId,
            exchangeSource,
            parentSink,
            new ArrayList<>(),
            new ArrayList<>()
        );
        mergeRuntimes.add(runtime);

        try (var emptySink = exchangeSource.addEmptySink()) {
            for (int i = 0; i < executionPlan.children().size(); i++) {
                String childPath = childPath(path, i);
                String childSessionId = nodeSessionId(childPath);
                ExchangeSinkHandler sinkHandler = exchangeService.createSinkHandler(childSessionId, queryPragmas.exchangeBufferSize());
                var childSink = new ParentSink(childSessionId, sinkHandler);
                parentSinks.add(childSink);
                exchangeSource.addRemoteSink(sinkHandler::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());

                SubPlanExecutionPlan child = executionPlan.children().get(i);
                Runtime childRuntime;
                if (child instanceof SubPlanExecutionPlan.Merge merge) {
                    childRuntime = buildMergeRuntime(merge, childPath, childSink, collectedPages);
                    runtime.leaves.addAll(((MergeRuntime) childRuntime).leaves);
                } else {
                    var leafRuntime = new LeafRuntime(child.plan(), childPath, childSink);
                    childRuntime = leafRuntime;
                    runtime.leaves.add(leafRuntime);
                }
                runtime.children.add(childRuntime);
            }
        }
        return runtime;
    }

    private void startMerge(
        MergeRuntime runtime,
        PlanTimeProfile planTimeProfile,
        ActionListener<DriverCompletionInfo> completionListener
    ) {
        Exception schedulerFailure = scheduler.failure();
        if (schedulerFailure != null) {
            abortUnstarted(runtime, schedulerFailure, completionListener);
            return;
        }
        runtime.started.set(true);
        ActionListener<DriverCompletionInfo> terminalListener = mergeTerminalListener(runtime, completionListener);
        try (var computeListener = new ComputeListener(computeService.cancelQueryOnFailure(rootTask), terminalListener)) {
            ActionListener<DriverCompletionInfo> segmentListener = computeListener.acquireCompute();
            List<ActionListener<DriverCompletionInfo>> childListeners = new ArrayList<>(runtime.children.size());
            for (int i = 0; i < runtime.children.size(); i++) {
                childListeners.add(computeListener.acquireCompute());
            }

            ActionListener<DriverCompletionInfo> guardedSegmentListener = ActionListener.wrap(completionInfo -> {
                scheduler.finishPending(leaf -> runtime.leaves.contains(leaf.runtime));
                segmentListener.onResponse(completionInfo);
            }, e -> {
                scheduler.fail(e);
                segmentListener.onFailure(e);
            });
            computeService.runCompute(
                rootTask,
                new ComputeContext(
                    runtime.computeSessionId,
                    runtime.path == null ? "main.final" : computeService.profileDescription(runtime.path, "merge"),
                    LOCAL_CLUSTER,
                    flags,
                    EmptyIndexedByShardId.instance(),
                    configuration,
                    foldContext,
                    runtime.exchangeSource::createExchangeSource,
                    runtime.parentSink == null ? null : () -> runtime.parentSink.handler.createExchangeSink(() -> {}),
                    false
                ),
                runtime.plan,
                computeService.plannerSettings().get(),
                LocalPhysicalOptimization.ENABLED,
                planTimeProfile,
                guardedSegmentListener
            );

            for (int i = 0; i < runtime.children.size(); i++) {
                Runtime child = runtime.children.get(i);
                ActionListener<DriverCompletionInfo> childListener = childListeners.get(i);
                if (child instanceof MergeRuntime merge) {
                    startMerge(merge, configuration.profile() ? new PlanTimeProfile() : null, childListener);
                } else {
                    scheduler.submit(new ScheduledLeaf((LeafRuntime) child, childListener));
                }
            }
        } catch (Exception e) {
            scheduler.fail(e);
            terminalListener.onFailure(e);
        }
    }

    private ActionListener<DriverCompletionInfo> mergeTerminalListener(
        MergeRuntime runtime,
        ActionListener<DriverCompletionInfo> completionListener
    ) {
        return ActionListener.notifyOnce(ActionListener.wrap(completionInfo -> {
            removeExchangeSource(runtime);
            finishParentSink(runtime.parentSink, null);
            completionListener.onResponse(completionInfo);
        }, e -> {
            removeExchangeSource(runtime);
            runtime.exchangeSource.finishEarly(true, ActionListener.noop());
            finishParentSink(runtime.parentSink, e);
            completionListener.onFailure(e);
        }));
    }

    private void executeLeaf(ScheduledLeaf scheduledLeaf, ActionListener<Void> schedulerListener) {
        LeafRuntime runtime = scheduledLeaf.runtime;
        if (rootTask.notifyIfCancelled(ActionListener.wrap(ignored -> {}, e -> finishLeaf(scheduledLeaf, schedulerListener, null, e)))) {
            return;
        }
        computeService.executePlan(
            runtime.parentSink.sessionId,
            rootTask,
            flags,
            runtime.plan,
            configuration,
            foldContext,
            execInfo,
            runtime.path,
            ActionListener.wrap(
                result -> finishLeaf(scheduledLeaf, schedulerListener, result.completionInfo(), null),
                e -> finishLeaf(scheduledLeaf, schedulerListener, null, e)
            ),
            () -> runtime.parentSink.handler.createExchangeSink(() -> {}),
            initialClusterStatuses,
            configuration.profile() ? new PlanTimeProfile() : null
        );
    }

    private void finishLeaf(
        ScheduledLeaf scheduledLeaf,
        ActionListener<Void> schedulerListener,
        DriverCompletionInfo completionInfo,
        Exception failure
    ) {
        try {
            if (failure == null) {
                finishParentSink(scheduledLeaf.runtime.parentSink, null);
                scheduledLeaf.listener.onResponse(completionInfo);
            } else {
                finishParentSink(scheduledLeaf.runtime.parentSink, failure);
                scheduledLeaf.listener.onFailure(failure);
            }
        } finally {
            if (failure == null) {
                schedulerListener.onResponse(null);
            } else {
                schedulerListener.onFailure(failure);
            }
        }
    }

    private void skipLeaf(ScheduledLeaf scheduledLeaf) {
        try {
            ExchangeSink sink = scheduledLeaf.runtime.parentSink.handler.createExchangeSink(() -> {});
            sink.finish();
            finishParentSink(scheduledLeaf.runtime.parentSink, null);
            scheduledLeaf.listener.onResponse(DriverCompletionInfo.EMPTY);
        } catch (Exception e) {
            finishParentSink(scheduledLeaf.runtime.parentSink, e);
            scheduledLeaf.listener.onFailure(e);
        }
    }

    private void rejectLeaf(ScheduledLeaf scheduledLeaf, Exception failure) {
        finishParentSink(scheduledLeaf.runtime.parentSink, failure);
        scheduledLeaf.listener.onFailure(failure);
    }

    private void abortUnstarted(MergeRuntime runtime, Exception failure, ActionListener<DriverCompletionInfo> completionListener) {
        if (runtime.started.compareAndSet(false, true) == false) {
            completionListener.onFailure(failure);
            return;
        }
        for (Runtime child : runtime.children) {
            if (child instanceof MergeRuntime merge) {
                abortUnstartedRuntime(merge, failure);
            } else {
                finishParentSink(((LeafRuntime) child).parentSink, failure);
            }
        }
        removeExchangeSource(runtime);
        runtime.exchangeSource.finishEarly(true, ActionListener.noop());
        finishParentSink(runtime.parentSink, failure);
        completionListener.onFailure(failure);
    }

    private void abortUnstartedRuntime(MergeRuntime runtime, Exception failure) {
        if (runtime.started.compareAndSet(false, true) == false) {
            return;
        }
        for (Runtime child : runtime.children) {
            if (child instanceof MergeRuntime merge) {
                abortUnstartedRuntime(merge, failure);
            } else {
                finishParentSink(((LeafRuntime) child).parentSink, failure);
            }
        }
        removeExchangeSource(runtime);
        runtime.exchangeSource.finishEarly(true, ActionListener.noop());
        finishParentSink(runtime.parentSink, failure);
    }

    private void removeExchangeSource(MergeRuntime runtime) {
        if (runtime.sourceRemoved.compareAndSet(false, true)) {
            exchangeService.removeExchangeSourceHandler(runtime.exchangeId);
        }
    }

    private void finishParentSink(ParentSink parentSink, Exception failure) {
        if (parentSink == null || parentSink.finished.compareAndSet(false, true) == false) {
            return;
        }
        if (failure == null) {
            parentSink.handler.addCompletionListener(
                ActionListener.running(() -> exchangeService.finishSinkHandler(parentSink.sessionId, null))
            );
        } else {
            exchangeService.finishSinkHandler(parentSink.sessionId, failure);
        }
    }

    private void cleanupUnstarted(Exception failure) {
        for (MergeRuntime runtime : mergeRuntimes.reversed()) {
            removeExchangeSource(runtime);
            runtime.exchangeSource.finishEarly(true, ActionListener.noop());
        }
        for (ParentSink parentSink : parentSinks.reversed()) {
            finishParentSink(parentSink, failure);
        }
    }

    private String nodeSessionId(String path) {
        return sessionId + "/" + path;
    }

    private static String childPath(String parentPath, int child) {
        String childName = "subplan-" + child;
        return parentPath == null ? childName : parentPath + "." + childName;
    }

    private sealed interface Runtime permits LeafRuntime, MergeRuntime {}

    private static final class LeafRuntime implements Runtime {
        private final PhysicalPlan plan;
        private final String path;
        private final ParentSink parentSink;

        private LeafRuntime(PhysicalPlan plan, String path, ParentSink parentSink) {
            this.plan = plan;
            this.path = path;
            this.parentSink = parentSink;
        }
    }

    private static final class MergeRuntime implements Runtime {
        private final PhysicalPlan plan;
        private final String path;
        private final String exchangeId;
        private final String computeSessionId;
        private final ExchangeSourceHandler exchangeSource;
        private final ParentSink parentSink;
        private final List<Runtime> children;
        private final List<LeafRuntime> leaves;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean sourceRemoved = new AtomicBoolean();

        private MergeRuntime(
            PhysicalPlan plan,
            String path,
            String exchangeId,
            String computeSessionId,
            ExchangeSourceHandler exchangeSource,
            ParentSink parentSink,
            List<Runtime> children,
            List<LeafRuntime> leaves
        ) {
            this.plan = plan;
            this.path = path;
            this.exchangeId = exchangeId;
            this.computeSessionId = computeSessionId;
            this.exchangeSource = exchangeSource;
            this.parentSink = parentSink;
            this.children = children;
            this.leaves = leaves;
        }
    }

    private static final class ParentSink {
        private final String sessionId;
        private final ExchangeSinkHandler handler;
        private final AtomicBoolean finished = new AtomicBoolean();

        private ParentSink(String sessionId, ExchangeSinkHandler handler) {
            this.sessionId = sessionId;
            this.handler = handler;
        }
    }

    private record ScheduledLeaf(LeafRuntime runtime, ActionListener<DriverCompletionInfo> listener) {}

}
