/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class UnionAll extends Fork {

    public UnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children, output);
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new UnionAll(source(), newChildren, output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, UnionAll::new, children(), output());
    }

    @Override
    public UnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new UnionAll(source(), subPlans, output());
    }

    @Override
    public UnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new UnionAll(source(), subPlans, output);
    }

    @Override
    public UnionAll refreshOutput() {
        return new UnionAll(source(), children(), refreshedOutput());
    }

    /**
     * Override of {@link Fork#pruneEmptyBranches(Predicate)} that returns a {@link UnionAll}
     * (rather than letting the base implementation produce whatever {@link #replaceChildren}
     * would). Mirrors the base behaviour otherwise: this primitive preserves single-survivor
     * wrappers, which the logical optimizer's {@code FlattenNestedSubqueries} rule later removes
     * for plain {@link UnionAll} nodes.
     * <p>
     * Unlike the base, the all-empty case does not produce a branchless node: a union of no
     * branches is the empty relation, so it collapses to an empty {@link LocalRelation} carrying
     * this node's {@link #output()}. Keeping the output attributes (same names, same ids) is what
     * lets the enclosing {@code Project}/{@code Eval} of the surrounding branch stay resolved.
     * <p>
     * The caller that can produce the all-empty case is {@code Analyzer.PruneEmptyUnionAllBranch},
     * when every branch of a union resolves to
     * {@link org.elasticsearch.xpack.esql.index.IndexResolution#EMPTY_SUBQUERY}. Returning a
     * branchless {@code UnionAll} instead would throw {@code NoSuchElementException} out of
     * {@link Fork#expressionsResolved()} during analysis - a 500 - so this collapse keeps the
     * invariant that no branchless plain union is ever handed back.
     * <p>
     * Reachability caveat: no end-to-end query currently drives a union to zero branches, so treat this as an invariant guard rather
     * than a fix for a reproduced request. Instrumenting {@code CrossClusterSubqueryIT} showed why: a remote pattern that matches no
     * index still resolves <em>valid</em> (empty) rather than {@code notFound}, so the {@code hasInvalid && hasValid} gate in
     * {@code EsqlSession} never fires and no pattern is ever replaced with {@code EMPTY_SUBQUERY}. Valid resolutions in turn mean no
     * {@link org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation} survives into
     * {@code Analyzer.PruneEmptyUnionAllBranch}, whose predicate only inspects those - so it prunes nothing and the empty branches
     * simply contribute zero rows. The unit coverage in {@code AnalyzerSubqueryTests} injects
     * {@code EMPTY_SUBQUERY} directly to reach the shape this method guards.
     * <p>
     * {@link ViewUnionAll} overrides this method without delegating here, so it keeps the
     * branchless-wrapper behaviour and the {@code Fork.checkBranchCount} verification that goes
     * with it.
     */
    @Override
    public LogicalPlan pruneEmptyBranches(Predicate<LogicalPlan> isEmpty) {
        List<LogicalPlan> kept = new ArrayList<>(children().size());
        for (LogicalPlan child : children()) {
            if (isEmpty.test(child) == false) {
                kept.add(child);
            }
        }
        if (kept.size() == children().size()) {
            return this;
        }
        if (kept.isEmpty()) {
            return new LocalRelation(source(), output(), EmptyLocalSupplier.EMPTY);
        }
        return new UnionAll(source(), kept, output());
    }

    @Override
    public int hashCode() {
        return Objects.hash(UnionAll.class, children());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnionAll other = (UnionAll) o;

        return Objects.equals(children(), other.children());
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return UnionAll::checkUnionAll;
    }

    private static void checkUnionAll(LogicalPlan plan, Failures failures) {
        // Check that all UnionAll branches have compatible data types for each column
        if (plan instanceof UnionAll unionAll) {
            if (plan.children().isEmpty()) {
                failures.add(Failure.fail(plan, "{} requires at least one branch", plan.getClass().getSimpleName()));
            }

            Map<String, DataType> outputTypes = unionAll.output().stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));

            unionAll.children().forEach(subPlan -> {
                for (Attribute attr : subPlan.output()) {
                    var expected = outputTypes.get(attr.name());

                    // UnionAll with unsupported types should not be allowed, otherwise runtime couldn't handle it
                    // Verifier checkUnresolvedAttributes should have caught it already, this check is similar to Fork
                    if (expected == null || expected == DataType.UNSUPPORTED) {
                        continue;
                    }

                    var actual = attr.dataType();
                    if (actual != expected) {
                        failures.add(
                            Failure.fail(
                                attr,
                                "Column [{}] has conflicting data types in subqueries: [{}] and [{}]",
                                attr.name(),
                                actual,
                                expected
                            )
                        );
                    }
                }
            });
        }
    }

    /** Checks both branching limits independently for the main query and every {@code IN} subquery. */
    public static void checkNestedSubqueryLimits(LogicalPlan optimizedPlan, int maxBranches, int maxLevels, Failures failures) {
        BranchingStats stats = branchingStats(optimizedPlan);
        checkTotalBranchCount(stats, maxBranches, failures);
        checkMaxNestingLevel(stats, maxLevels, failures);
        checkInSubqueryLimits(optimizedPlan, maxBranches, maxLevels, failures);
    }

    private static void checkInSubqueryLimits(LogicalPlan plan, int maxBranches, int maxLevels, Failures failures) {
        if (plan instanceof AbstractSubqueryJoin subqueryJoin) {
            checkNestedSubqueryLimits(subqueryJoin.right(), maxBranches, maxLevels, failures);
            checkInSubqueryLimits(subqueryJoin.left(), maxBranches, maxLevels, failures);
            return;
        }
        for (LogicalPlan child : plan.children()) {
            checkInSubqueryLimits(child, maxBranches, maxLevels, failures);
        }
    }

    /**
     * Rejects a query whose leaf branches exceed {@code maxBranches}, the {@link QueryPragmas#MAX_QUERY_BRANCHES} query pragma.
     * <p>
     * Only producer leaves are counted — {@link Fork} nodes themselves are coordinator merge segments, not branches, and are bounded
     * separately by the maximum nesting-level check. Each leaf becomes a data node query (or a coordinator-local source), so the total
     * is what a single request commits the coordinator to. {@link Fork#MAX_BRANCHES} bounds one merge node, but merge nodes can nest, so
     * without a query-wide limit the leaf total grows as a power of the nesting depth.
     * <p>
     * Unlike the other checks here this one looks at a complete independently executed query rather than a single node, so it is called
     * from {@code LogicalVerifier} instead of through per-node post-optimization verification, which applies each registered check to
     * every node. The main query and each {@code IN} subquery are checked separately because each is executed independently by the
     * compute service. It counts leaves under user forks, {@link ViewUnionAll}s, and dataset/federation unions too: they cost exactly the
     * same at execution time as a user-written subquery union.
     */
    private static void checkTotalBranchCount(BranchingStats stats, int maxBranches, Failures failures) {
        if (stats.first() == null || stats.leaves() <= maxBranches) {
            return;
        }
        failures.add(
            Failure.fail(
                stats.first(),
                "query resolved to {} branches in total, exceeding the limit of {} set by the [{}] query pragma. "
                    + "Reduce the number of sources - forks, subqueries, patterns expanding to several indices, datasets, or views - "
                    + "or split this into multiple queries.",
                stats.leaves(),
                maxBranches,
                QueryPragmas.MAX_QUERY_BRANCHES.getKey()
            )
        );
    }

    /**
     * Rejects a query whose merge nodes nest deeper than {@code maxLevels}, the {@link QueryPragmas#MAX_QUERY_BRANCH_LEVELS} query pragma.
     * <p>
     * Each nested {@link Fork} becomes a coordinator merge segment that is wired before any leaf runs, so the depth is what a single
     * request commits the coordinator to on the merge-segment stack. {@link #checkTotalBranchCount} bounds how many branches there are in
     * total, but a skinny chain of two-way unions can stay under that cap at arbitrary depth.
     * <p>
     * Unlike the other checks here this one looks at a complete independently executed query rather than a single node, so it is called
     * from {@code LogicalVerifier} instead of through per-node post-optimization verification, which applies each registered check to
     * every node. The main query and each {@code IN} subquery are checked separately because each is executed independently by the compute
     * service. It counts user forks, {@link ViewUnionAll}s, and dataset/federation unions because all are planned as the same merge
     * segment.
     */
    private static void checkMaxNestingLevel(BranchingStats stats, int maxLevels, Failures failures) {
        if (stats.depth() <= maxLevels) {
            return;
        }
        failures.add(
            Failure.fail(
                stats.deepest(),
                "query resolved to {} nested union levels, exceeding the limit of {} set by the [{}] query pragma. "
                    + "Reduce the nesting of sources - forks, subqueries, patterns expanding to several indices, datasets, or views - "
                    + "or split this into multiple queries.",
                stats.depth(),
                maxLevels,
                QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey()
            )
        );
    }

    /**
     * Summarizes the branching nodes under {@code plan}. A subtree with no {@link Fork} is one producer leaf. Otherwise its leaf count is
     * the sum of its children's leaves. Its depth is the longest child depth, plus one when {@code plan} is itself a fork or union. The
     * same traversal covers user forks, user-written subqueries, view unions and dataset/federation unions without depending on their
     * origin. Transient view and dataset shadows do not survive to this post-optimization check and therefore consume no branch budget.
     * The right side of an
     * {@link AbstractSubqueryJoin} is excluded because it is an independently executed query. The first and deepest merge nodes provide
     * distinct failure locations when both limits are exceeded.
     */
    private static BranchingStats branchingStats(LogicalPlan plan) {
        Fork first = plan instanceof Fork fork ? fork : null;
        Fork deepest = first;
        int leaves = 0;
        int depth = 0;
        List<LogicalPlan> children = plan instanceof AbstractSubqueryJoin subqueryJoin ? List.of(subqueryJoin.left()) : plan.children();
        for (LogicalPlan child : children) {
            BranchingStats childStats = branchingStats(child);
            if (first == null) {
                first = childStats.first();
            }
            leaves += childStats.leaves();
            if (childStats.depth() > depth) {
                depth = childStats.depth();
                deepest = childStats.deepest();
            }
        }
        if (first == null) {
            return new BranchingStats(null, null, 1, 0);
        }
        return new BranchingStats(first, deepest, leaves, depth + (plan instanceof Fork ? 1 : 0));
    }

    private record BranchingStats(Fork first, Fork deepest, int leaves, int depth) {}
}
