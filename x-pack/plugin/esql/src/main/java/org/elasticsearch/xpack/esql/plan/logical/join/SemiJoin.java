/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;

/**
 * A semi or anti join used to implement {@code WHERE field IN (subquery)} and
 * {@code WHERE field NOT IN (subquery)}.
 * <p>
 * The right side is an independent subquery that must be executed first. Once the subquery result
 * is available as a {@link LocalRelation}, {@link #inlineData} converts this node into a
 * {@link Filter} with an {@link In} expression (or {@code NOT IN} for ANTI joins).
 */
public class SemiJoin extends Join implements SortPreserving {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "SemiJoin",
        SemiJoin::readFrom
    );

    public SemiJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config);
    }

    public SemiJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields
    ) {
        super(source, left, right, type, leftFields, rightFields, null);
    }

    private static SemiJoin readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        LogicalPlan left = in.readNamedWriteable(LogicalPlan.class);
        LogicalPlan right = in.readNamedWriteable(LogicalPlan.class);
        JoinConfig config = new JoinConfig(in);
        return new SemiJoin(source, left, right, config);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Join> info() {
        JoinConfig config = config();
        return NodeInfo.create(this, SemiJoin::new, left(), right(), config.type(), config.leftFields(), config.rightFields());
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new SemiJoin(source(), left, right, config());
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        return new ArrayList<>(left);
    }

    public boolean isAntiJoin() {
        return JoinTypes.ANTI.equals(config().type());
    }

    /**
     * Converts this SemiJoin into a {@link Filter} once the subquery result is available.
     * Extracts all values from the subquery's single output column and builds an
     * {@code IN (v1, v2, ...)} expression (or {@code NOT IN} for anti joins).
     */
    public static LogicalPlan inlineData(SemiJoin semiJoin, LocalRelation data) {
        List<Attribute> schema = data.output();
        Page page = data.supplier().get();

        List<Expression> values = new ArrayList<>();
        if (page != null && page.getBlockCount() > 0 && schema.isEmpty() == false) {
            Block block = page.getBlock(0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                Object value = toJavaObject(block, i);
                values.add(new Literal(semiJoin.source(), value, schema.get(0).dataType()));
            }
        }

        Attribute leftField = semiJoin.config().leftFields().get(0);
        Expression condition;
        if (values.isEmpty()) {
            condition = semiJoin.isAntiJoin() ? Literal.TRUE : Literal.FALSE;
        } else {
            condition = new In(semiJoin.source(), leftField, values);
            if (semiJoin.isAntiJoin()) {
                condition = new Not(semiJoin.source(), condition);
            }
        }
        return new Filter(semiJoin.source(), semiJoin.left(), condition);
    }

    /**
     * Finds the first SemiJoin in the plan whose right side has not yet been replaced with results.
     * Unlike InlineJoin, the right side is an independent subquery that doesn't use StubRelation.
     */
    public static LogicalPlanTuple firstSubPlan(LogicalPlan optimizedPlan, Set<LocalRelation> subPlansResults) {
        Holder<LogicalPlan> subPlanHolder = new Holder<>();
        Holder<LogicalPlan> originalHolder = new Holder<>();
        optimizedPlan.forEachUp(SemiJoin.class, sj -> {
            if (subPlanHolder.get() == null) {
                if (sj.right() instanceof LocalRelation lr && subPlansResults.contains(lr)) {
                    return;
                }
                subPlanHolder.set(sj.right());
                originalHolder.set(sj.right());
            }
        });
        if (subPlanHolder.get() == null) {
            return null;
        }
        LogicalPlan plan = subPlanHolder.get();
        plan = plan.transformUp(LocalRelation.class, lr -> {
            if (lr.supplier() instanceof CopyingLocalSupplier == false) {
                return new LocalRelation(lr.source(), lr.output(), new CopyingLocalSupplier(lr.supplier().get()));
            }
            return lr;
        });
        plan.setOptimized();
        return new LogicalPlanTuple(plan, originalHolder.get());
    }

    public static LogicalPlan newMainPlan(LogicalPlan optimizedPlan, LogicalPlanTuple subPlans, LocalRelation resultWrapper) {
        LogicalPlan newPlan = optimizedPlan.transformUp(
            SemiJoin.class,
            sj -> sj.right() == subPlans.originalSubPlan() ? inlineData(sj, resultWrapper) : sj
        );
        newPlan.setOptimized();
        return newPlan;
    }

    /**
     * Tuple holding the subplan to execute and the original plan node for identity matching.
     */
    public record LogicalPlanTuple(LogicalPlan subPlan, LogicalPlan originalSubPlan) {}
}
