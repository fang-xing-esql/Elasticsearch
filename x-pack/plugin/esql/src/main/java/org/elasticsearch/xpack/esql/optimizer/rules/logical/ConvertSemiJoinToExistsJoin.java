/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.ExistsJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;

/**
 * Converts a {@link SemiJoin} (or AntiJoin) into an {@link ExistsJoin} when the right side has no pipeline breakers
 * and is backed by a lookup index. This allows the join to be pushed down to data nodes using the lookup join
 * infrastructure instead of materializing the subquery on the coordinator.
 * <p>
 * The right side is restructured to match what the LookupJoin infrastructure expects:
 * the {@link Project} is stripped (ExistsJoin adds no right-side columns) and the
 * {@link EsRelation}'s indexMode is set to {@link IndexMode#LOOKUP}.
 */
public final class ConvertSemiJoinToExistsJoin extends OptimizerRules.OptimizerRule<SemiJoin> {

    @Override
    protected LogicalPlan rule(SemiJoin semiJoin) {
        LogicalPlan right = semiJoin.right();
        // we might need more restrictions here, need to test more query patterns
        if (hasPipelineBreaker(right) || isLookupIndex(right) == false) {
            return semiJoin;
        }

        // Strip Project — ExistsJoin is an existence check, no right-side columns are needed to be projected
        LogicalPlan strippedRight = stripProject(right);
        // Set the EsRelation indexMode to LOOKUP so the LookupJoin mapper/planner infrastructure accepts it.
        LogicalPlan lookupRight = strippedRight.transformUp(EsRelation.class, er -> er.withIndexMode(IndexMode.LOOKUP));

        LogicalPlan existsJoin = new ExistsJoin(
            semiJoin.source(),
            semiJoin.left(),
            lookupRight,
            semiJoin.config().leftFields(),
            semiJoin.config().rightFields(),
            semiJoin.isAntiJoin()
        );

        // For NOT IN (anti-join), null left keys must be excluded to match SQL NOT IN semantics:
        // NULL NOT IN (...) evaluates to UNKNOWN, so the row should not appear in the output.
        if (semiJoin.isAntiJoin()) {
            Attribute leftField = semiJoin.config().leftFields().get(0);
            existsJoin = new Filter(semiJoin.source(), existsJoin, new IsNotNull(semiJoin.source(), leftField));
        }

        return existsJoin;
    }

    private static LogicalPlan stripProject(LogicalPlan plan) {
        if (plan instanceof Project project) {
            return project.child();
        }
        return plan;
    }

    private static boolean hasPipelineBreaker(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof PipelineBreaker);
    }

    /**
     * Checks whether the plan's leaf source is a lookup index based on its actual index settings
     * (index.mode = lookup), as reported by field caps via {@link EsRelation#indexNameWithModes()}.
     * Only checks the leaf-most EsRelation (the FROM source), not nested EsRelations from LOOKUP JOINs.
     */
    private static boolean isLookupIndex(LogicalPlan plan) {
        var leaves = plan.collectLeaves();
        return leaves.size() == 1
            && leaves.get(0) instanceof EsRelation relation
            && relation.indexNameWithModes().isEmpty() == false
            && relation.indexNameWithModes().values().stream().allMatch(mode -> mode == IndexMode.LOOKUP);
    }
}
