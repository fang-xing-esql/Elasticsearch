/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

public class UnionAll extends Fork {

    private final List<Attribute> output;

    public UnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children, output);
        this.output = output;
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new UnionAll(source(), newChildren, output);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, UnionAll::new, children(), output);
    }

    @Override
    public UnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new UnionAll(source(), subPlans, output);
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

    public boolean canMerge() {
        // if the subquery contains from/eval/where/subquery, it can be merged
        for (LogicalPlan subPlan : children()) {
            if (canMerge(subPlan) == false) {
                return false;
            }
        }
        return true;
    }

    // more plan types could be added if needed, non-pipeline breakers like eval/filter etc.
    private boolean canMerge(LogicalPlan plan) {
        return plan instanceof Subquery || plan instanceof EsRelation || plan instanceof UnresolvedRelation;
    }
}
