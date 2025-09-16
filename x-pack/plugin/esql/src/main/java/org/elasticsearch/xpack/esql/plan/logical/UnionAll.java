/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

public class UnionAll extends Fork {

    public UnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children, output);
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
