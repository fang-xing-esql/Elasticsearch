/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Resolves {@link InSubquery} expressions in supported plan node contexts by rewriting them into
 * {@link SemiJoin}, {@link AntiJoin}, or {@link MarkJoin} nodes:
 * <ul>
 *   <li><b>WHERE ({@link Filter})</b>: an {@code InSubquery} at the top of an AND-conjunct
 *       (optionally wrapped in {@link Not}) becomes a row-filtering {@link SemiJoin} /
 *       {@link AntiJoin}. An {@code InSubquery} nested inside {@link Or} is replaced with a
 *       synthetic boolean mark attribute; a {@link MarkJoin} is stacked below the
 *       {@link Filter}. An {@code InSubquery} inside a {@link Case} WHEN condition in a
 *       {@link Filter} is also rewritten via the same mark-join mechanism.</li>
 *   <li><b>EVAL ({@link Eval})</b>: each {@code InSubquery} anywhere in an EVAL field
 *       expression (including inside {@link Case} WHEN conditions or function arguments) is
 *       replaced with a synthetic mark attribute; a {@link MarkJoin} is stacked below the
 *       {@link Eval}.</li>
 *   <li><b>STATS WHERE ({@link Aggregate} + {@link FilteredExpression})</b>: an
 *       {@code InSubquery} inside a {@link FilteredExpression} filter
 *       ({@code STATS agg() WHERE x IN (sub)}) is replaced with a mark attribute; a
 *       {@link MarkJoin} is stacked below the {@link Aggregate}.</li>
 *   <li><b>STATS BY ({@link Aggregate} groupings)</b>: an {@code InSubquery} inside a
 *       grouping expression ({@code STATS BY x IN (sub)}) is replaced with a mark attribute;
 *       a {@link MarkJoin} is stacked below the {@link Aggregate}.</li>
 *   <li><b>SORT ({@link OrderBy})</b>: an {@code InSubquery} in an {@link Order} expression is
 *       replaced with a mark attribute; a {@link MarkJoin} is stacked below the
 *       {@link OrderBy}.</li>
 *   <li><b>LIMIT BY ({@link LimitBy})</b>: an {@code InSubquery} in a grouping expression
 *       ({@code LIMIT N BY x IN (sub)}) is replaced with a mark attribute; a {@link MarkJoin}
 *       is stacked below the {@link LimitBy}.</li>
 *   <li><b>INLINESTATS WHERE ({@link InlineStats} + {@link FilteredExpression})</b>: an
 *       {@code InSubquery} inside a {@link FilteredExpression} filter
 *       ({@code INLINESTATS agg() WHERE x IN (sub)}) is replaced with a mark attribute; a
 *       {@link MarkJoin} is stacked below the inner {@link Aggregate}. The synthetic mark
 *       attribute is excluded from the {@link InlineStats} output.</li>
 *   <li><b>INLINESTATS BY ({@link InlineStats} groupings)</b>: an {@code InSubquery} inside a
 *       grouping expression ({@code INLINESTATS ... BY x IN (sub)}) is replaced with a mark
 *       attribute; a {@link MarkJoin} is stacked below the inner {@link Aggregate}.</li>
 *   <li>An {@code InSubquery} in an unsupported position (e.g. inside a non-boolean scalar
 *       function, inside {@code IS NOT NULL}, or with a complex non-foldable LHS) is left in
 *       place; the post-resolution {@link #verify} step rejects the query with a
 *       {@link VerificationException}.</li>
 * </ul>
 * <p>
 * The two traversal helpers differ in scope:
 * <ul>
 *   <li>{@link #rewriteOrContextInSubqueries} — walks only boolean-connective expressions
 *       ({@link And}/{@link Or}/{@link Not}/{@link Case} WHEN conditions) and stops at anything
 *       else. Used for the {@link Filter} WHERE context, where lifting an {@code InSubquery}
 *       out of a non-boolean position would silently change semantics.</li>
 *   <li>{@link #rewriteAllInSubqueries} — recurses into <em>all</em> expression children.
 *       Used for {@link Eval}, {@link OrderBy}, {@link LimitBy}, and {@link Aggregate}
 *       grouping/filter contexts, where any surviving type error is caught later by the
 *       type-checker.</li>
 * </ul>
 * <p>
 * This runs before {@link PreAnalyzer} so the subquery plans, originally embedded inside
 * {@link InSubquery} expressions, become children of join nodes and visible to standard plan
 * traversals. This eliminates the need for separate InSubquery-aware traversals in
 * {@link PreAnalyzer}, {@link org.elasticsearch.xpack.esql.session.FieldNameUtils FieldNameUtils},
 * and {@link org.elasticsearch.xpack.esql.inference.InferenceService InferenceService}.
 * <p>
 * The join's {@code rightFields} are left empty at this stage because the subquery output is not
 * yet resolved. The Analyzer's {@code ResolveRefs} fills them in during the Resolution batch.
 */
public class InSubqueryResolver {

    /**
     * Resolves all {@link InSubquery} expressions in {@link Filter} conditions and validates the
     * result. Throws a {@link VerificationException} when an {@link InSubquery} survived rewriting
     * (e.g. inside an EVAL, SORT, STATS BY clause, or wrapped in a non-boolean expression).
     * <p>
     * Synchronous — does no I/O. Async callers should invoke this inside an
     * {@link org.elasticsearch.action.ActionListener#delegateFailureAndWrap delegateFailureAndWrap}
     * lambda so the thrown {@link VerificationException} is routed to {@code onFailure}.
     * <p>
     * Telemetry for {@code IN_SUBQUERY} is collected separately by the session — see
     * {@code EsqlSession#gatherInSubqueryMetrics}, which uses {@link #hasInSubqueryInFilter} on
     * the pre-resolution plan because by the time this method returns the originating
     * {@link InSubquery} expressions have been replaced with
     * {@link SemiJoin}/{@link AntiJoin}/{@link MarkJoin} and are no longer visible to plan
     * traversals. The {@code WHERE} counter still picks up SemiJoin/AntiJoin/MarkJoin in the
     * post-resolution plan walk (see {@code FeatureMetric#WHERE}), so the {@code WHERE} bit does
     * not need to be set up-front here.
     */
    public static LogicalPlan resolve(LogicalPlan plan) {
        LogicalPlan resolved = resolveInSubqueries(plan);
        verify(resolved);
        return resolved;
    }

    private static LogicalPlan resolveInSubqueries(LogicalPlan plan) {
        LogicalPlan afterFilterPass = plan.transformUp(Filter.class, InSubqueryResolver::resolveInSubqueryInFilter);
        LogicalPlan afterInlineStatsPass = afterFilterPass.transformUp(
            InlineStats.class,
            InSubqueryResolver::resolveInSubqueryInInlineStats
        );
        // Collect the Aggregate instances that are children of InlineStats (by identity). Those were
        // already handled by the InlineStats pass and must not be touched again by the Aggregate pass.
        Set<Aggregate> inlineStatsAggregates = Collections.newSetFromMap(new IdentityHashMap<>());
        afterInlineStatsPass.forEachDown(InlineStats.class, ils -> inlineStatsAggregates.add(ils.aggregate()));
        LogicalPlan afterAggPass = afterInlineStatsPass.transformUp(
            Aggregate.class,
            agg -> inlineStatsAggregates.contains(agg) ? agg : resolveInSubqueryInAggregate(agg)
        );
        return afterAggPass.transformUp(p -> switch (p) {
            case Eval eval -> resolveInSubqueryInEval(eval);
            case OrderBy orderBy -> resolveInSubqueryInOrderBy(orderBy);
            case LimitBy limitBy -> resolveInSubqueryInLimitBy(limitBy);
            default -> p;
        });
    }

    /**
     * Returns {@code true} if the pre-resolution plan contains any {@link InSubquery} expression
     * inside a {@link Filter} (i.e. as part of a {@code WHERE} condition). Used by the session to
     * decide whether to increment the {@code IN_SUBQUERY} telemetry counter — once per query, in
     * the same spirit as {@code EsqlSession#gatherViewMetrics}.
     * <p>
     * Restricted to {@link Filter} conditions because {@link InSubquery} occurrences elsewhere
     * (EVAL, SORT, etc.) are rejected by {@link #verify} today.
     */
    public static boolean hasInSubqueryInFilter(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof Filter filter && filter.condition().anyMatch(e -> e instanceof InSubquery));
    }

    /**
     * Returns {@code true} if the pre-resolution plan contains any {@link InSubquery} expression
     * in any supported context. Used by {@link org.elasticsearch.xpack.esql.view.ViewResolver} to
     * decide whether to run {@link InSubquery} resolution even when there are no views.
     */
    public static boolean hasInSubquery(LogicalPlan plan) {
        return plan.anyMatch(p -> {
            if (p instanceof Filter filter) {
                return filter.condition().anyMatch(e -> e instanceof InSubquery);
            }
            if (p instanceof Aggregate agg) {
                if (agg.aggregates().stream().anyMatch(a -> a.anyMatch(e -> e instanceof InSubquery))) return true;
                return agg.groupings().stream().anyMatch(g -> g.anyMatch(e -> e instanceof InSubquery));
            }
            if (p instanceof InlineStats ils) {
                Aggregate agg = ils.aggregate();
                if (agg.aggregates().stream().anyMatch(a -> a.anyMatch(e -> e instanceof InSubquery))) return true;
                return agg.groupings().stream().anyMatch(g -> g.anyMatch(e -> e instanceof InSubquery));
            }
            if (p instanceof Eval eval) {
                return eval.fields().stream().anyMatch(f -> f.anyMatch(e -> e instanceof InSubquery));
            }
            if (p instanceof OrderBy orderBy) {
                return orderBy.order().stream().anyMatch(o -> o.anyMatch(e -> e instanceof InSubquery));
            }
            if (p instanceof LimitBy limitBy) {
                return limitBy.groupings().stream().anyMatch(g -> g.anyMatch(e -> e instanceof InSubquery));
            }
            return false;
        });
    }

    /**
     * Spec for a {@link SemiJoin} / {@link AntiJoin} stacked on top of the remaining filter for
     * an {@link InSubquery} that appears as a top-level AND conjunct.
     */
    private record SemiOrAntiJoinSpec(Source source, LogicalPlan subquery, JoinConfig config, boolean anti) {}

    /**
     * Spec for a {@link MarkJoin} stacked below the remaining filter for an {@link InSubquery}
     * that appears under {@code OR}/{@code NOT}/{@code AND} but not as a top-level AND conjunct.
     * The mark attribute is referenced from the rewritten boolean expression.
     */
    private record MarkJoinSpec(Source source, LogicalPlan subquery, JoinConfig config, Attribute markAttribute) {}

    /**
     * Make this public, so that {@link org.elasticsearch.xpack.esql.view.ViewResolver} can drive IN subquery resolution.
     */
    public static LogicalPlan resolveInSubqueryInFilter(Filter filter) {
        Expression condition = filter.condition();

        List<Expression> conjuncts = Predicates.splitAnd(condition);

        List<Expression> remaining = new ArrayList<>();
        // Joins applied AFTER the remaining filter. SemiJoin/AntiJoin filter out rows that don't
        // satisfy the original IN/NOT IN predicate; they are correct only when the predicate is
        // an AND-conjunct.
        List<SemiOrAntiJoinSpec> semiOrAntiJoins = new ArrayList<>();
        // Joins applied BEFORE the remaining filter. MarkJoins emit a boolean mark attribute
        // referenced from the rewritten remaining condition; the mark carries the three-valued
        // IN result through the normal boolean evaluation in the surrounding OR/AND/NOT shape.
        List<MarkJoinSpec> markJoins = new ArrayList<>();
        // Synthetic Eval aliases for constant left-hand side expressions (e.g. WHERE 10001 IN (subquery)).
        // Materialized as an Eval below the joins; the synthetic attributes are projected away above.
        List<Alias> syntheticEvals = new ArrayList<>();

        for (Expression conjunct : conjuncts) {
            if (tryResolveAsSemiOrAntiJoin(conjunct, semiOrAntiJoins, syntheticEvals)) {
                continue;
            }
            // Either no InSubquery in the conjunct (passes through unchanged), or InSubquery is
            // nested inside OR (rewritten with MarkJoin), or InSubquery sits under a
            // non-boolean wrapper (left as-is for {@link #verify} to reject).
            Expression rewritten = rewriteOrContextInSubqueries(conjunct, markJoins, syntheticEvals);
            remaining.add(rewritten);
        }

        if (semiOrAntiJoins.isEmpty() && markJoins.isEmpty()) {
            return filter;
        }

        LogicalPlan current = filter.child();

        // If any constants need materialization, insert an Eval to create the synthetic attributes.
        if (syntheticEvals.isEmpty() == false) {
            current = new Eval(filter.source(), current, syntheticEvals);
        }

        // Stack MarkJoins first — they are applied before the remaining filter so the mark
        // attributes are available to the rewritten boolean expression.
        for (MarkJoinSpec mj : markJoins) {
            current = new MarkJoin(mj.source, current, mj.subquery, mj.config, mj.markAttribute);
        }

        // Apply remaining filter conditions on top of MarkJoins (so mark attributes are in scope).
        if (remaining.isEmpty() == false) {
            current = new Filter(filter.source(), current, Predicates.combineAnd(remaining));
        }

        // Stack SemiJoins / AntiJoins on top — they filter rows but don't modify columns.
        for (SemiOrAntiJoinSpec sj : semiOrAntiJoins) {
            current = sj.anti
                ? new AntiJoin(sj.source, current, sj.subquery, sj.config)
                : new SemiJoin(sj.source, current, sj.subquery, sj.config);
        }

        // The mark attributes from MarkJoins (and any synthetic constant Eval columns introduced
        // for foldable LHS) are flagged synthetic so the analyzer's default output projection
        // (planWithoutSyntheticAttributes) drops them — preserving the filter's apparent schema.
        return current;
    }

    /**
     * Rewrites {@link InSubquery} expressions inside an {@link Aggregate} plan node into
     * {@link MarkJoin} nodes, covering two positions:
     * <ul>
     *   <li>{@code STATS agg() WHERE x IN (sub)}: rewrites {@code InSubquery} inside
     *       {@link FilteredExpression#filter()} so the per-row mark attribute is available to the
     *       aggregate filter before grouping.</li>
     *   <li>{@code STATS BY x IN (sub)}: rewrites {@code InSubquery} inside each grouping
     *       expression (including those wrapped in an
     *       {@link org.elasticsearch.xpack.esql.core.expression.Alias}) so the mark drives the
     *       group key.</li>
     * </ul>
     * Only handles plain {@link Aggregate} nodes, not subclasses (e.g. {@code TimeSeriesAggregate}).
     * <p>
     * Make this public so that {@link org.elasticsearch.xpack.esql.view.ViewResolver} can drive
     * IN subquery resolution for aggregate nodes.
     */
    public static LogicalPlan resolveInSubqueryInAggregate(Aggregate aggregate) {
        if (aggregate.getClass() != Aggregate.class) {
            return aggregate;
        }
        List<MarkJoinSpec> markJoins = new ArrayList<>();
        List<Alias> syntheticEvals = new ArrayList<>();

        // Rewrite InSubquery in FilteredExpression filters (STATS agg() WHERE x IN (sub))
        List<NamedExpression> rewrittenAggregates = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression agg : aggregate.aggregates()) {
            NamedExpression rewritten = (NamedExpression) agg.transformDown(FilteredExpression.class, fe -> {
                Expression rewrittenFilter = rewriteAllInSubqueries(fe.filter(), markJoins, syntheticEvals);
                if (rewrittenFilter == fe.filter()) {
                    return fe;
                }
                return new FilteredExpression(fe.source(), fe.delegate(), rewrittenFilter);
            });
            rewrittenAggregates.add(rewritten);
        }

        // Rewrite InSubquery in grouping expressions (STATS BY x IN (sub))
        List<Expression> rewrittenGroupings = new ArrayList<>(aggregate.groupings().size());
        for (Expression grouping : aggregate.groupings()) {
            rewrittenGroupings.add(rewriteAllInSubqueries(grouping, markJoins, syntheticEvals));
        }

        if (markJoins.isEmpty()) {
            return aggregate;
        }

        // When a grouping alias is rewritten (e.g. InSubquery replaced by a mark attribute), the
        // alias may transition from unresolved to resolved. Its toAttribute() therefore changes: before
        // rewriting it returned a fresh UnresolvedAttribute (cached as lazyAttribute); after rewriting
        // replaceChildren creates a new Alias whose lazyAttribute is null, so the next toAttribute()
        // call returns a ReferenceAttribute with the alias's own NameId. The parser's buildStats added
        // the *original* toAttribute() result as a self-reference into the aggregates list so that the
        // grouping key appears in the output. Aggregate.computeReferences() removes that self-reference
        // via the *current* grouping alias's toAttribute() — the two no longer match, leaving a spurious
        // attribute in references(). Fix: replace the stale self-reference in rewrittenAggregates with
        // the updated toAttribute() so the invariant is restored.
        for (int i = 0; i < aggregate.groupings().size(); i++) {
            Expression origGrouping = aggregate.groupings().get(i);
            Expression rewrittenGrouping = rewrittenGroupings.get(i);
            if (origGrouping != rewrittenGrouping
                && origGrouping instanceof Alias origAlias
                && rewrittenGrouping instanceof Alias newAlias) {
                Attribute origAttr = origAlias.toAttribute();
                Attribute newAttr = newAlias.toAttribute();
                for (int j = 0; j < rewrittenAggregates.size(); j++) {
                    if (rewrittenAggregates.get(j) == origAttr) {
                        rewrittenAggregates.set(j, newAttr);
                        break;
                    }
                }
            }
        }

        LogicalPlan current = aggregate.child();
        if (syntheticEvals.isEmpty() == false) {
            current = new Eval(aggregate.source(), current, syntheticEvals);
        }
        for (MarkJoinSpec mj : markJoins) {
            current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
        }
        return new Aggregate(aggregate.source(), current, rewrittenGroupings, rewrittenAggregates);
    }

    /**
     * Rewrites {@link InSubquery} expressions inside an {@link InlineStats} plan node into
     * {@link MarkJoin} nodes, covering two positions:
     * <ul>
     *   <li>{@code INLINESTATS agg() WHERE x IN (sub)}: rewrites {@code InSubquery} inside
     *       {@link FilteredExpression#filter()} so the per-row mark attribute is available to the
     *       aggregate filter.</li>
     *   <li>{@code INLINESTATS ... BY x IN (sub)}: rewrites {@code InSubquery} inside each grouping
     *       expression so the mark drives the group key.</li>
     * </ul>
     * The {@link MarkJoin} is stacked between the inner {@link Aggregate} and its child so the
     * mark attribute is visible during aggregation. The synthetic mark attribute is excluded from
     * the {@link InlineStats} output by {@link InlineStats#output()}, which filters out synthetic
     * attributes from the aggregate's child output.
     */
    static LogicalPlan resolveInSubqueryInInlineStats(InlineStats inlineStats) {
        Aggregate aggregate = inlineStats.aggregate();
        List<MarkJoinSpec> markJoins = new ArrayList<>();
        List<Alias> syntheticEvals = new ArrayList<>();

        // Rewrite InSubquery in grouping expressions FIRST (INLINE STATS ... BY x IN (sub)).
        // This must come before the aggregates loop because LogicalPlanBuilder.visitInlineStatsCommand
        // does aggregates.addAll(groupings), placing the same Alias objects in both lists. We build
        // an identity map so the aggregates loop can replace those entries without re-running
        // rewriteAllInSubqueries (which would create duplicate MarkJoin nodes).
        List<Expression> rewrittenGroupings = new ArrayList<>(aggregate.groupings().size());
        for (Expression grouping : aggregate.groupings()) {
            rewrittenGroupings.add(rewriteAllInSubqueries(grouping, markJoins, syntheticEvals));
        }

        Map<Expression, Expression> groupingRewrites = new IdentityHashMap<>();
        for (int i = 0; i < aggregate.groupings().size(); i++) {
            Expression orig = aggregate.groupings().get(i);
            Expression rewritten = rewrittenGroupings.get(i);
            if (orig != rewritten) {
                groupingRewrites.put(orig, rewritten);
            }
        }

        // Rewrite InSubquery in FilteredExpression filters (INLINE STATS agg() WHERE x IN (sub)).
        // If an aggregate slot is a grouping alias added directly to the aggregates list by the parser,
        // use the already-rewritten version from groupingRewrites instead of re-processing it.
        List<NamedExpression> rewrittenAggregates = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression agg : aggregate.aggregates()) {
            Expression rewrittenViaGroupings = groupingRewrites.get(agg);
            if (rewrittenViaGroupings != null) {
                rewrittenAggregates.add((NamedExpression) rewrittenViaGroupings);
            } else {
                NamedExpression rewritten = (NamedExpression) agg.transformDown(FilteredExpression.class, fe -> {
                    Expression rewrittenFilter = rewriteAllInSubqueries(fe.filter(), markJoins, syntheticEvals);
                    if (rewrittenFilter == fe.filter()) {
                        return fe;
                    }
                    return new FilteredExpression(fe.source(), fe.delegate(), rewrittenFilter);
                });
                rewrittenAggregates.add(rewritten);
            }
        }

        if (markJoins.isEmpty()) {
            return inlineStats;
        }

        LogicalPlan current = aggregate.child();
        if (syntheticEvals.isEmpty() == false) {
            current = new Eval(aggregate.source(), current, syntheticEvals);
        }
        for (MarkJoinSpec mj : markJoins) {
            current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
        }
        Aggregate rewrittenAggregate = new Aggregate(aggregate.source(), current, rewrittenGroupings, rewrittenAggregates);
        return new InlineStats(inlineStats.source(), rewrittenAggregate);
    }

    /**
     * Rewrites {@link InSubquery} expressions inside an {@link Eval} plan node into
     * {@link MarkJoin} nodes. Every {@code InSubquery} anywhere in an EVAL field expression
     * (including inside {@link Case} WHEN conditions or function arguments) is replaced with a
     * synthetic boolean mark attribute; a {@link MarkJoin} is stacked below the {@link Eval}.
     * <p>
     * Make this public so that {@link org.elasticsearch.xpack.esql.view.ViewResolver} can drive
     * IN subquery resolution for EVAL nodes.
     */
    public static LogicalPlan resolveInSubqueryInEval(Eval eval) {
        List<MarkJoinSpec> markJoins = new ArrayList<>();
        List<Alias> syntheticEvals = new ArrayList<>();

        List<Alias> rewrittenFields = new ArrayList<>(eval.fields().size());
        boolean changed = false;
        for (Alias alias : eval.fields()) {
            Expression r = rewriteAllInSubqueries(alias.child(), markJoins, syntheticEvals);
            Alias rewrittenAlias = r == alias.child() ? alias : alias.replaceChildren(List.of(r));
            rewrittenFields.add(rewrittenAlias);
            changed |= rewrittenAlias != alias;
        }

        if (markJoins.isEmpty()) return eval;

        LogicalPlan current = eval.child();
        if (syntheticEvals.isEmpty() == false) {
            current = new Eval(eval.source(), current, syntheticEvals);
        }
        for (MarkJoinSpec mj : markJoins) {
            current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
        }
        return new Eval(eval.source(), current, rewrittenFields);
    }

    /**
     * Rewrites {@link InSubquery} expressions inside an {@link OrderBy} plan node into
     * {@link MarkJoin} nodes. An {@code InSubquery} in any {@link Order} expression is replaced
     * with a synthetic boolean mark attribute; a {@link MarkJoin} is stacked below the
     * {@link OrderBy}.
     * <p>
     * Make this public so that {@link org.elasticsearch.xpack.esql.view.ViewResolver} can drive
     * IN subquery resolution for SORT nodes.
     */
    public static LogicalPlan resolveInSubqueryInOrderBy(OrderBy orderBy) {
        List<MarkJoinSpec> markJoins = new ArrayList<>();
        List<Alias> syntheticEvals = new ArrayList<>();

        List<Order> rewrittenOrders = new ArrayList<>(orderBy.order().size());
        boolean changed = false;
        for (Order order : orderBy.order()) {
            Expression r = rewriteAllInSubqueries(order.child(), markJoins, syntheticEvals);
            Order rewrittenOrder = r == order.child() ? order : new Order(order.source(), r, order.direction(), order.nullsPosition());
            rewrittenOrders.add(rewrittenOrder);
            changed |= rewrittenOrder != order;
        }

        if (markJoins.isEmpty()) return orderBy;

        LogicalPlan current = orderBy.child();
        if (syntheticEvals.isEmpty() == false) {
            current = new Eval(orderBy.source(), current, syntheticEvals);
        }
        for (MarkJoinSpec mj : markJoins) {
            current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
        }
        return new OrderBy(orderBy.source(), current, rewrittenOrders);
    }

    /**
     * Rewrites {@link InSubquery} expressions inside a {@link LimitBy} plan node into
     * {@link MarkJoin} nodes. An {@code InSubquery} in any grouping expression
     * ({@code LIMIT N BY x IN (sub)}) is replaced with a synthetic boolean mark attribute; a
     * {@link MarkJoin} is stacked below the {@link LimitBy}.
     * <p>
     * Unlike {@code STATS BY}, {@link LimitBy} groupings are raw {@link Expression}s (not wrapped
     * in {@link Alias}), so no alias bookkeeping is needed.
     * <p>
     * Make this public so that {@link org.elasticsearch.xpack.esql.view.ViewResolver} can drive
     * IN subquery resolution for LIMIT BY nodes.
     */
    public static LogicalPlan resolveInSubqueryInLimitBy(LimitBy limitBy) {
        List<MarkJoinSpec> markJoins = new ArrayList<>();
        List<Alias> syntheticEvals = new ArrayList<>();

        List<Expression> rewrittenGroupings = new ArrayList<>(limitBy.groupings().size());
        boolean changed = false;
        for (Expression grouping : limitBy.groupings()) {
            Expression r = rewriteAllInSubqueries(grouping, markJoins, syntheticEvals);
            rewrittenGroupings.add(r);
            changed |= r != grouping;
        }

        if (markJoins.isEmpty()) return limitBy;

        LogicalPlan current = limitBy.child();
        if (syntheticEvals.isEmpty() == false) {
            current = new Eval(limitBy.source(), current, syntheticEvals);
        }
        for (MarkJoinSpec mj : markJoins) {
            current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
        }
        return new LimitBy(limitBy.source(), limitBy.limitPerGroup(), current, rewrittenGroupings);
    }

    /**
     * Attempts to handle {@code conjunct} as a top-level {@link InSubquery} (optionally wrapped in
     * one or more {@link Not}s) with an attribute or foldable LHS. On success appends the
     * corresponding {@link SemiOrAntiJoinSpec} (and any synthetic Eval Alias) and returns
     * {@code true}; otherwise returns {@code false} and leaves the accumulators untouched.
     */
    private static boolean tryResolveAsSemiOrAntiJoin(
        Expression conjunct,
        List<SemiOrAntiJoinSpec> semiOrAntiJoins,
        List<Alias> syntheticEvals
    ) {
        boolean negated = false;
        Expression expr = conjunct;
        while (expr instanceof Not not) {
            expr = not.field();
            negated = !negated;
        }

        if (expr instanceof InSubquery inSubquery) {
            Expression leftValue = inSubquery.value();
            List<Attribute> leftFields;
            if (leftValue instanceof Attribute leftAttr) {
                leftFields = singletonList(leftAttr);
            } else if (leftValue.foldable()) {
                var syntheticAlias = new Alias(
                    leftValue.source(),
                    syntheticConstName(leftValue, inSubquery.subquery()),
                    leftValue,
                    null,
                    true
                );
                syntheticEvals.add(syntheticAlias);
                leftFields = singletonList(syntheticAlias.toAttribute());
            } else {
                // Non-attribute, non-foldable LHS — leave it for the verifier to surface a clear error.
                return false;
            }

            LogicalPlan subquery = resolveNestedInSubqueries(inSubquery.subquery());
            JoinConfig config = new JoinConfig(negated ? JoinTypes.ANTI : JoinTypes.SEMI, leftFields, emptyList(), null);
            semiOrAntiJoins.add(new SemiOrAntiJoinSpec(inSubquery.source(), subquery, config, negated));
            return true;
        }
        return false;
    }

    /**
     * Walks the boolean expression replacing every {@link InSubquery} reachable through
     * {@link And}/{@link Or}/{@link Not} (i.e. boolean position) with a fresh synthetic mark
     * attribute, recording a {@link MarkJoinSpec} per replacement. {@link InSubquery}
     * occurrences that sit under a non-boolean wrapper (function argument, comparison, etc.) are
     * left in place for {@link #verify} to reject. Any expression with no eligible
     * {@link InSubquery} below it is returned unchanged.
     */
    private static Expression rewriteOrContextInSubqueries(Expression expr, List<MarkJoinSpec> joins, List<Alias> syntheticEvals) {
        if (expr instanceof And and) {
            Expression left = rewriteOrContextInSubqueries(and.left(), joins, syntheticEvals);
            Expression right = rewriteOrContextInSubqueries(and.right(), joins, syntheticEvals);
            return left == and.left() && right == and.right() ? and : new And(and.source(), left, right);
        }
        if (expr instanceof Or or) {
            Expression left = rewriteOrContextInSubqueries(or.left(), joins, syntheticEvals);
            Expression right = rewriteOrContextInSubqueries(or.right(), joins, syntheticEvals);
            return left == or.left() && right == or.right() ? or : new Or(or.source(), left, right);
        }
        if (expr instanceof Not not) {
            Expression child = rewriteOrContextInSubqueries(not.field(), joins, syntheticEvals);
            return child == not.field() ? not : new Not(not.source(), child);
        }
        if (expr instanceof InSubquery inSubquery) {
            return rewriteAsMarkJoin(inSubquery, joins, syntheticEvals);
        }
        if (expr instanceof Case caseExpr) {
            // Recurse only into WHEN condition positions — even indices in the children list,
            // excluding the last child when the total count is odd (that last child is the ELSE value).
            // Result/ELSE positions are not boolean expressions and must not be rewritten.
            List<Expression> children = caseExpr.children();
            List<Expression> rewritten = new ArrayList<>(children.size());
            boolean caseChanged = false;
            for (int i = 0; i < children.size(); i++) {
                boolean isCondition = (i % 2 == 0) && (children.size() % 2 == 0 || i < children.size() - 1);
                Expression child = children.get(i);
                Expression r = isCondition ? rewriteOrContextInSubqueries(child, joins, syntheticEvals) : child;
                rewritten.add(r);
                caseChanged |= r != child;
            }
            return caseChanged ? caseExpr.replaceChildren(rewritten) : caseExpr;
        }
        // Non-boolean expression (function call, comparison, IS NOT NULL, etc.). Do NOT recurse:
        // any nested InSubquery should be reported as unsupported by the verifier rather than
        // silently lifted out into a join that would change the expression's semantics.
        return expr;
    }

    /**
     * Recursively walks {@code expr} and replaces every {@link InSubquery} occurrence with a
     * fresh synthetic mark attribute, recording a {@link MarkJoinSpec} per replacement.
     * Unlike {@link #rewriteOrContextInSubqueries}, this method recurses into <em>all</em>
     * expression nodes — it is intended for use in {@link Aggregate} grouping and filter contexts
     * where any expression structure is valid and the enclosing plan node provides the boolean
     * result.
     */
    private static Expression rewriteAllInSubqueries(Expression expr, List<MarkJoinSpec> joins, List<Alias> syntheticEvals) {
        if (expr instanceof InSubquery inSubquery) {
            return rewriteAsMarkJoin(inSubquery, joins, syntheticEvals);
        }
        List<Expression> children = expr.children();
        if (children.isEmpty()) {
            return expr;
        }
        List<Expression> rewritten = new ArrayList<>(children.size());
        boolean changed = false;
        for (Expression child : children) {
            Expression r = rewriteAllInSubqueries(child, joins, syntheticEvals);
            rewritten.add(r);
            changed |= r != child;
        }
        return changed ? expr.replaceChildren(rewritten) : expr;
    }

    /**
     * Allocates a synthetic boolean mark attribute for {@code inSubquery}, records a
     * {@link MarkJoinSpec}, and returns the mark attribute as the replacement expression.
     * Returns the original {@link InSubquery} unchanged when the LHS is neither an attribute
     * nor foldable — those cases are surfaced as errors by {@link #verify}.
     */
    private static Expression rewriteAsMarkJoin(InSubquery inSubquery, List<MarkJoinSpec> joins, List<Alias> syntheticEvals) {
        Expression leftValue = inSubquery.value();
        List<Attribute> leftFields;
        if (leftValue instanceof Attribute leftAttr) {
            leftFields = singletonList(leftAttr);
        } else if (leftValue.foldable()) {
            var syntheticAlias = new Alias(leftValue.source(), syntheticConstName(leftValue, inSubquery.subquery()), leftValue, null, true);
            syntheticEvals.add(syntheticAlias);
            leftFields = singletonList(syntheticAlias.toAttribute());
        } else {
            return inSubquery;
        }

        LogicalPlan subquery = resolveNestedInSubqueries(inSubquery.subquery());
        Attribute markAttribute = new ReferenceAttribute(
            inSubquery.source(),
            null,
            syntheticMarkName(inSubquery),
            DataType.BOOLEAN,
            Nullability.TRUE,
            new NameId(),
            true
        );
        JoinConfig config = new JoinConfig(JoinTypes.MARK, leftFields, emptyList(), null);
        joins.add(new MarkJoinSpec(inSubquery.source(), subquery, config, markAttribute));
        return markAttribute;
    }

    /**
     * Recursively transforms a subquery plan, converting any nested IN/NOT IN subquery expressions
     * into SemiJoin/AntiJoin/MarkJoin nodes. This is needed because nested subquery plans are
     * embedded inside InSubquery expressions and not reachable by the top-level transformUp.
     */
    private static LogicalPlan resolveNestedInSubqueries(LogicalPlan subqueryPlan) {
        LogicalPlan afterFilterPass = subqueryPlan.transformUp(Filter.class, InSubqueryResolver::resolveInSubqueryInFilter);
        LogicalPlan afterInlineStatsPass = afterFilterPass.transformUp(
            InlineStats.class,
            InSubqueryResolver::resolveInSubqueryInInlineStats
        );
        Set<Aggregate> inlineStatsAggregates = Collections.newSetFromMap(new IdentityHashMap<>());
        afterInlineStatsPass.forEachDown(InlineStats.class, ils -> inlineStatsAggregates.add(ils.aggregate()));
        LogicalPlan afterAggPass = afterInlineStatsPass.transformUp(
            Aggregate.class,
            agg -> inlineStatsAggregates.contains(agg) ? agg : resolveInSubqueryInAggregate(agg)
        );
        return afterAggPass.transformUp(p -> switch (p) {
            case Eval eval -> resolveInSubqueryInEval(eval);
            case OrderBy orderBy -> resolveInSubqueryInOrderBy(orderBy);
            case LimitBy limitBy -> resolveInSubqueryInLimitBy(limitBy);
            default -> p;
        });
    }

    /**
     * Generates a unique synthetic name for a constant on the left-hand side of an IN subquery.
     */
    private static String syntheticConstName(Expression value, LogicalPlan subquery) {
        return "$$in_subquery_const$" + value.hashCode() + "$" + subquery.hashCode();
    }

    /**
     * Generates a unique synthetic name for the boolean mark attribute produced by a
     * {@link MarkJoin} in place of an {@link InSubquery}.
     */
    private static String syntheticMarkName(InSubquery inSubquery) {
        return "$$in_subquery_mark$" + inSubquery.value().hashCode() + "$" + inSubquery.subquery().hashCode();
    }

    public static void verify(LogicalPlan plan) {
        Failures failures = new Failures();
        checkInSubqueryUsage(plan, failures);
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
    }

    private static void checkInSubqueryUsage(LogicalPlan plan, Failures failures) {
        // Collect InlineStats's inner Aggregate instances by identity. Those must be skipped by
        // the Aggregate branch below — any surviving InSubquery in INLINESTATS is reported via the
        // InlineStats branch instead, so that the error message uses the INLINESTATS source text.
        Set<Aggregate> inlineStatsAggregates = Collections.newSetFromMap(new IdentityHashMap<>());
        plan.forEachDown(InlineStats.class, ils -> inlineStatsAggregates.add(ils.aggregate()));

        plan.forEachDown(p -> {
            if (p instanceof Filter filter) {
                checkInFilterCondition(filter, filter.condition(), null, failures);
            } else if (p instanceof InlineStats inlineStats) {
                // A surviving InSubquery in INLINESTATS means the resolver could not rewrite it
                // (e.g. complex non-foldable LHS). Report using the INLINESTATS source text.
                Aggregate agg = inlineStats.aggregate();
                for (Expression aggExpr : agg.aggregates()) {
                    aggExpr.forEachDown(
                        InSubquery.class,
                        inSub -> failures.add(fail(inSub, "Complicated IN subquery is not yet supported in [{}]", inlineStats.sourceText()))
                    );
                }
                for (Expression grouping : agg.groupings()) {
                    grouping.forEachDown(
                        InSubquery.class,
                        inSub -> failures.add(fail(inSub, "Complicated IN subquery is not yet supported in [{}]", inlineStats.sourceText()))
                    );
                }
            } else if (p instanceof Aggregate agg && inlineStatsAggregates.contains(agg) == false) {
                // A surviving InSubquery in STATS means the resolver could not rewrite it
                // (e.g. complex non-foldable LHS). Report it as a user-facing error.
                for (Expression aggExpr : agg.aggregates()) {
                    aggExpr.forEachDown(
                        InSubquery.class,
                        inSub -> failures.add(fail(inSub, "Complicated IN subquery is not yet supported in [{}]", agg.sourceText()))
                    );
                }
                for (Expression grouping : agg.groupings()) {
                    grouping.forEachDown(
                        InSubquery.class,
                        inSub -> failures.add(fail(inSub, "Complicated IN subquery is not yet supported in [{}]", agg.sourceText()))
                    );
                }
            } else if (p instanceof LimitBy limitBy) {
                // A surviving InSubquery in LIMIT BY means a complex LHS that the resolver
                // could not rewrite. Report it as a user-facing error.
                for (Expression grouping : limitBy.groupings()) {
                    grouping.forEachDown(
                        InSubquery.class,
                        inSub -> failures.add(fail(inSub, "Complicated IN subquery is not yet supported in [{}]", limitBy.sourceText()))
                    );
                }
            } else if (p instanceof Eval || p instanceof OrderBy) {
                // Supported contexts — a surviving InSubquery here (complex LHS) is surfaced by
                // the serialization guard (InSubquery.writeTo throws) rather than a custom message.
            } else if (inlineStatsAggregates.contains(p) == false) {
                // Skip InlineStats inner Aggregates — already reported via the InlineStats branch above.
                p.forEachExpression(
                    InSubquery.class,
                    inSub -> failures.add(fail(inSub, "IN subquery is not supported in [{}]", p.sourceText()))
                );
            }
        });
    }

    /**
     * Walks the {@code WHERE} condition tree to validate IN subquery usage that the
     * {@link InSubqueryResolver} could not rewrite into a {@link SemiJoin}/{@link AntiJoin}/{@link MarkJoin}.
     * <p>
     * If the IN subquery sits at the top of the boolean condition (i.e. only {@link And} /
     * {@link Or} / {@link Not} above it) the resolver normally rewrites it; if one survives here
     * it means the surrounding boolean shape is not yet supported (e.g. an unsupported LHS shape).
     * In that case we report the whole filter source (the entire {@code WHERE} clause).
     * <p>
     * Otherwise (the IN subquery is nested inside a non-boolean expression such as a scalar
     * function or {@code IS NOT NULL}), we report the immediately enclosing expression.
     */
    private static void checkInFilterCondition(Filter filter, Expression expr, Expression outerExpr, Failures failures) {
        if (expr instanceof InSubquery in) {
            if (outerExpr == null) {
                failures.add(fail(in, "Complicated IN subquery is not yet supported in the WHERE command [{}]", filter.sourceText()));
            } else {
                failures.add(fail(in, "IN subquery is not supported within other expressions [{}]", outerExpr.sourceText()));
            }
        }
        Expression newOuterExpr = outerExpr == null
            && expr instanceof And == false
            && expr instanceof Or == false
            && expr instanceof Not == false ? expr : outerExpr;
        for (Expression child : expr.children()) {
            checkInFilterCondition(filter, child, newOuterExpr, failures);
        }
    }
}
