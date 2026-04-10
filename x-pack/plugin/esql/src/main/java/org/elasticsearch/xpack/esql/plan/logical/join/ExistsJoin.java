/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;

/**
 * An existence join used to implement {@code WHERE field IN (subquery)} and {@code WHERE field NOT IN (subquery)}
 * when the right side has no pipeline breakers and is backed by a lookup index.
 * <p>
 * This extends {@link LookupJoin} so it flows through the same Mapper → LookupJoinExec → LookupFromIndexOperator
 * pipeline. It uses LEFT join type for compatibility with the mapper, but overrides output to be left-only
 * (no right-side columns appended). Deduplication is handled at the operator level via
 * {@link org.elasticsearch.compute.operator.lookup.RightChunkedExistsJoin}.
 */
public class ExistsJoin extends LookupJoin {

    private final boolean anti;

    public ExistsJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        boolean anti
    ) {
        super(source, left, right, LEFT, leftFields, rightFields, false, null);
        this.anti = anti;
    }

    private ExistsJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config, boolean anti) {
        super(source, left, right, config, false);
        this.anti = anti;
    }

    public boolean isAntiJoin() {
        return anti;
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        // Existence check only — output is the left side, no right-side columns are added.
        return new ArrayList<>(left);
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new ExistsJoin(source(), left, right, config(), anti);
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(this, ExistsJoin::new, left(), right(), config().leftFields(), config().rightFields(), anti);
    }

    @Override
    public LogicalPlan surrogate() {
        // ExistsJoin should not be replaced by SubstituteSurrogatePlans.
        return this;
    }

    @Override
    public String telemetryLabel() {
        return anti ? "EXISTS JOIN (ANTI)" : "EXISTS JOIN (SEMI)";
    }
}
