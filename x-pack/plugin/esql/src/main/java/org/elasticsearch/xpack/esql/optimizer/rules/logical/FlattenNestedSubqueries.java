/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Removes a plain {@link UnionAll} after other optimizer rules have reduced it to one branch. A one-branch union has no union
 * semantics but still maps to a pipeline-breaking merge. When the union and branch use different output identities, this rule
 * replaces the union with a {@link Project} that correlates the branch output to the attributes expected above the union.
 * Multi-branch unions, {@link ViewUnionAll}s, and bare FORK plans are not flattened.
 */
public final class FlattenNestedSubqueries extends OptimizerRules.OptimizerRule<UnionAll> {

    public FlattenNestedSubqueries() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(UnionAll unionAll) {
        if (unionAll instanceof ViewUnionAll || unionAll.children().size() != 1) {
            return unionAll;
        }

        LogicalPlan child = unionAll.children().getFirst();
        List<Attribute> unionOutput = unionAll.output();
        List<Attribute> childOutput = child.output();
        if (unionOutput.equals(childOutput)) {
            return child;
        }
        if (unionOutput.size() != childOutput.size()) {
            return unionAll;
        }

        Map<String, Attribute> childByName = new HashMap<>(childOutput.size());
        for (Attribute childAttribute : childOutput) {
            if (childByName.put(childAttribute.name(), childAttribute) != null) {
                return unionAll;
            }
        }

        List<NamedExpression> projections = new ArrayList<>(unionOutput.size());
        for (Attribute unionAttribute : unionOutput) {
            Attribute childAttribute = childByName.get(unionAttribute.name());
            if (childAttribute == null || childAttribute.dataType() != unionAttribute.dataType()) {
                return unionAll;
            }
            if (unionAttribute.equals(childAttribute)) {
                projections.add(childAttribute);
            } else {
                // Alias.toAttribute() always creates a ReferenceAttribute. Keep the union when correlation would
                // erase a specialized output subtype such as ExternalMetadataAttribute/VirtualAttribute, since
                // downstream optimizer and execution rules use those marker types for correctness.
                if (unionAttribute instanceof ReferenceAttribute == false) {
                    return unionAll;
                }
                projections.add(
                    new Alias(
                        unionAttribute.source(),
                        unionAttribute.name(),
                        childAttribute,
                        unionAttribute.id(),
                        unionAttribute.synthetic()
                    )
                );
            }
        }
        return new Project(unionAll.source(), child, projections);
    }
}
