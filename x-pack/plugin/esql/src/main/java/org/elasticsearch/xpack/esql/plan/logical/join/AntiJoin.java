/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An anti join used to implement {@code WHERE field NOT IN (subquery)}.
 * <p>
 * Behaves identically to {@link SemiJoin} except it uses {@link JoinTypes#ANTI} and the inlined
 * result produces a {@code NOT IN} filter.
 */
public class AntiJoin extends SemiJoin implements SortPreserving {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "AntiJoin",
        AntiJoin::readFrom
    );

    public AntiJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config);
    }

    public AntiJoin(Source source, LogicalPlan left, LogicalPlan right, List<Attribute> leftFields, List<Attribute> rightFields) {
        super(source, left, right, JoinTypes.ANTI, leftFields, rightFields);
    }

    private static AntiJoin readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        LogicalPlan left = in.readNamedWriteable(LogicalPlan.class);
        LogicalPlan right = in.readNamedWriteable(LogicalPlan.class);
        JoinConfig config = new JoinConfig(in);
        return new AntiJoin(source, left, right, config);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Join> info() {
        JoinConfig config = config();
        return NodeInfo.create(this, AntiJoin::new, left(), right(), config.leftFields(), config.rightFields());
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new AntiJoin(source(), left, right, config());
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        return new ArrayList<>(left);
    }

    @Override
    public boolean isAntiJoin() {
        return true;
    }
}
