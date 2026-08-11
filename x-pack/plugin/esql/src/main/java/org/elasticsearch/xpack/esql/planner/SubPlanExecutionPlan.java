/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Objects;

/**
 * Coordinator-local execution topology for a physical plan containing merge branches. Building this topology before execution
 * separates physical-plan decomposition from the asynchronous exchange lifecycle in {@code ComputeService}.
 */
public sealed interface SubPlanExecutionPlan permits SubPlanExecutionPlan.Leaf, SubPlanExecutionPlan.Merge {

    /** The physical plan executed by this topology node. */
    PhysicalPlan plan();

    /** A producer plan that does not contain a topmost merge point. */
    record Leaf(PhysicalPlan plan) implements SubPlanExecutionPlan {
        public Leaf {
            Objects.requireNonNull(plan);
        }
    }

    /**
     * A coordinator segment whose merge point has been replaced by an exchange source, together with the producer topologies that
     * feed that source.
     */
    record Merge(PhysicalPlan plan, List<SubPlanExecutionPlan> children) implements SubPlanExecutionPlan {
        public Merge {
            Objects.requireNonNull(plan);
            children = List.copyOf(children);
            if (children.isEmpty()) {
                throw new IllegalArgumentException("a merge execution plan requires at least one child");
            }
        }
    }
}
