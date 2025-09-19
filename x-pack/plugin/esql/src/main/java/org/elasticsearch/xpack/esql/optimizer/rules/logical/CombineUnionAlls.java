/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.ArrayList;
import java.util.List;

/**
 * Combine multiple {@code UnionAll}s into one in order to reduce the nested {@code Fork}s in a plan.
 *
 * This rule mainly target queries that have nested subqueries in the from command, for example:
 *
 * Example 1:
 * FROM index1, (FROM index2, (FROM index3)), its plan after parsing is
 *         UnionAll
 *           EsRelation(index1)
 *           Subquery
 *             UnionAll
 *               EsRelation(index2)
 *               Subquery
 *                  EsRelation(index3)
 * This rule transforms it into
 *        UnionAll
 *          EsRelation(index1)
 *          Subquery
 *            EsRelation(index2)
 *          Subquery
 *            EsRelation(index3)
 * which flatten UnionAll into one level
 *
 * Example 2:
 * FROM index1, (FROM index2, (FROM index3 | WHERE a &lt; 10) | WHERE b &lt; 5), its plan after parsing is
 *        UnionAll
 *          EsRelation(index1)
 *          Subquery
 *            Filter(b &lt; 5)
 *              UnionAll
 *                EsRelation(index2)
 *                Subquery
 *                  Filter(a &lt; 10)
 *                    EsRelation(index3)
 *  PushDownAndCombineFilters transforms it into
 *        UnionAll
 *          EsRelation(index1)
 *          Subquery
 *            UnionAll
 *              Subquery
 *                Filter(b &lt; 5)
 *                  EsRelation(index2)
 *              Subquery
 *                Filter(b &lt; 5) (a &lt; 10)
 *                  EsRelation(index3)
 *  After PushDownAndCombineFilters, the UnionAll is still nested. This rule can flatten it to
 *       UnionAll
 *         EsRelation(index1)
 *         Subquery
 *           Filter(b &lt; 5)
 *             EsRelation(index2)
 *         Subquery
 *           Filter(b &lt; 5) (a &lt; 10)
 *             EsRelation(index3)
 *
 * Example 3
 * FROM index1, (FROM index2, (FROM index3 | WHERE a &lt; 10) | STATS COUNT(*) by b), its plan after parsing is
 *       UnionAll
 *         EsRelation(index1)
 *         Subquery
 *           Aggregate(b, COUNT(*))
 *             UnionAll
 *               EsRelation(index2)
 *               Subquery
 *                 Filter(a &lt; 10)
 *                   EsRelation(index3)
 * This rule cannot combine the two UnionAlls because the second UnionAll is under an Aggregate.
 * The plan remains unchanged.
 */
public final class CombineUnionAlls extends OptimizerRules.OptimizerRule<UnionAll> {

    public CombineUnionAlls() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(UnionAll unionAll) {
        /* The children of a UnionAll/Fork plan has a pattern.
         * Fork adds EsqlProject and Eval and Limit on top of its actual children.
         * UnionAll
         *   EsqlProject
         *     Eval (optional) if the output of UnionAll children need to be aligned
         *       Limit
         *         EsRelation
         *   EsqlProject
         *     Eval (optional) if the output of UnionAll children need to be aligned
         *       Limit
         *         Subquery
         *
         * When there are nested UnionAll, if the outer UnionAll has a child that is a Subquery,
         * and the Subquery has a child that is a UnionAll, the inner UnionAll's children can be merged
         * into the outer UnionAll. This rule detect this pattern and does the transformation.
         */

        boolean changed = false;
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (LogicalPlan child : unionAll.children()) {
            if (child instanceof EsRelation esRelation) {
                newChildren.add(esRelation);
            } else if (child instanceof Subquery subquery && subquery.plan() instanceof UnionAll nestedUnionAll) {
                // if the child is a subquery, and its child is a UnionAll,
                // we will try to merge the children of the nested UnionAll into its parent UnionAll.
                for (LogicalPlan nestedChild : nestedUnionAll.children()) {
                    // if the nested child is a relation, we can merge it
                    if (nestedChild instanceof EsRelation esRelation) {
                        newChildren.add(new Subquery(esRelation.source(), nestedChild));
                        changed = true;
                    } else if (nestedChild instanceof Subquery nestedSubquery) {
                        // if the nested child is a subquery, and it contains only one relation or Eval, we can merge it
                        newChildren.add(nestedSubquery);
                        changed = true;
                    } else {
                        // otherwise, we cannot merge it, keep the original child
                        return unionAll;
                    }
                }
            } else {
                return unionAll;
            }
        }
        return changed ? unionAll.replaceChildren(newChildren) : unionAll;
    }

    private Subquery subqueryWithNestedUnionAll(LogicalPlan logicalPlan) {
        if (logicalPlan instanceof Subquery subquery && subquery.plan() instanceof UnionAll) {
            return subquery;
        } else {
            return null;

        }

    }
}
