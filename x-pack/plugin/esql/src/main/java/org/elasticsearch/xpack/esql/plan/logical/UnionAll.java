/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class UnionAll extends Fork implements PostOptimizationPlanVerificationAware {

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
        Fork.checkBranchCount(plan, failures);
        // Check that all UnionAll branches have compatible data types for each column
        if (plan instanceof UnionAll unionAll) {
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

    @Override
    public BiConsumer<LogicalPlan, Failures> postOptimizationPlanVerification() {
        return UnionAll::checkNestedUnionAlls;
    }

    /**
     * Nested {@link UnionAll}s (subqueries within subqueries) are supported; this check only rejects the shapes that remain
     * unsupported below a {@link UnionAll}: a {@link ViewUnionAll} (a {@code FROM} pattern expanding to multiple sources) and a
     * bare {@link Fork} ({@code FORK} inside a subquery). It runs after the logical planner because some nested subqueries will be
     * flattened by optimizer rules and only the surviving plan shape matters.
     */
    private static void checkNestedUnionAlls(LogicalPlan logicalPlan, Failures failures) {
        if (logicalPlan instanceof UnionAll unionAll) {
            Fork.forEachForkSkippingSubqueries(unionAll, nested -> {
                if (unionAll == nested || (nested instanceof UnionAll && nested instanceof ViewUnionAll == false)) {
                    return;
                }
                failures.add(nestedUnionAllFailure(nested));
            });
        }
    }

    /**
     * Rejects a query whose {@link UnionAll}s add up to more branches than {@code maxBranches}, the
     * {@code max_query_branches} query pragma.
     * <p>
     * Every branch becomes either a coordinator merge segment - with its own exchange handlers and drivers, all started eagerly - or a
     * data node query, so the total is what a single request commits the coordinator to. {@link Fork#MAX_BRANCHES} bounds one
     * {@code FROM} but subqueries nest, so without a query-wide limit the total grows as a power of the nesting depth.
     * <p>
     * Unlike the other checks here this one looks at the whole plan rather than a single node, so it is called once from
     * {@code LogicalVerifier} instead of through {@link #postOptimizationPlanVerification()}, which applies each registered check to
     * every node. It counts {@link ViewUnionAll}s too: a union produced by expanding a {@code FROM} pattern or view costs exactly the
     * same at execution time as one the user wrote.
     */
    public static void checkTotalBranchCount(LogicalPlan optimizedPlan, int maxBranches, Failures failures) {
        List<UnionAll> unionAlls = new ArrayList<>();
        optimizedPlan.forEachDown(UnionAll.class, unionAlls::add);
        if (unionAlls.isEmpty()) {
            return;
        }
        int branches = unionAlls.stream().mapToInt(unionAll -> unionAll.children().size()).sum();
        if (branches > maxBranches) {
            // forEachDown is pre-order, so the first node found is the outermost FROM - the one worth pointing the user at.
            failures.add(
                Failure.fail(
                    unionAlls.getFirst(),
                    "query resolved to {} branches in total, exceeding the current limit of {}. "
                        + "Reduce the number of subqueries, or split this into multiple queries.",
                    branches,
                    maxBranches
                )
            );
        }
    }

    /**
     * Builds the verification {@link Failure} for a {@link ViewUnionAll} or bare {@link Fork} found nested below another
     * {@link UnionAll} at post-optimization.
     * <p>
     * A {@link ViewUnionAll} is never written by the user: it is added when a {@code FROM} pattern resolves, during view resolution, to
     * more than one source where at least one is a view — for example a wildcard matching a view together with a concrete index, a pattern
     * matching several views, or a view whose body references multiple sources. In every one of those cases the pattern (or view) expands
     * to a union of multiple sources, so a generic nesting error would be misleading - the query the user wrote contains no nested
     * subquery. We describe the real cause instead and quote the offending {@code FROM} clause (from {@link #sourceText()}, truncated to
     * {@link Node#TO_STRING_MAX_WIDTH}) so the user can locate it. A bare {@link Fork} is a {@code FORK} inside a subquery.
     */
    private static Failure nestedUnionAllFailure(LogicalPlan nested) {
        if (nested instanceof ViewUnionAll) {
            String sourceText = nested.sourceText();
            String source = sourceText.length() > Node.TO_STRING_MAX_WIDTH
                ? sourceText.substring(0, Node.TO_STRING_MAX_WIDTH) + "..."
                : sourceText;
            return Failure.fail(
                nested,
                "a pattern that expands to multiple sources, [{}], cannot be combined with subqueries"
                    + "; replace it with a single source in the FROM command",
                source
            );
        }
        return Failure.fail(nested, "FORK inside subquery is not supported");
    }
}
