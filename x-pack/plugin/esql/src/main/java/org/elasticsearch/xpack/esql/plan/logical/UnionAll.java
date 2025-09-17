/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class UnionAll extends Fork implements PostOptimizationPlanVerificationAware {

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

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return UnionAll::checkUnionAll;
    }

    private static void checkUnionAll(LogicalPlan plan, Failures failures) {
        if (plan instanceof UnionAll == false) {
            return;
        }
        UnionAll unionAll = (UnionAll) plan;

        Map<String, DataType> outputTypes = unionAll.output().stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));

        unionAll.children().forEach(subPlan -> {
            for (Attribute attr : subPlan.output()) {
                var expected = outputTypes.get(attr.name());

                // If the FORK output has an UNSUPPORTED data type, we know there is no conflict.
                // We only assign an UNSUPPORTED attribute in the FORK output when there exists no attribute with the
                // same name and supported data type in any of the FORK branches.
                if (expected == DataType.UNSUPPORTED) {
                    continue;
                }

                var actual = attr.dataType();
                if (actual != expected) {
                    failures.add(
                        Failure.fail(
                            attr,
                            "Column [{}] has conflicting data types in UnionAll branches: [{}] and [{}]",
                            attr.name(),
                            actual,
                            expected
                        )
                    );
                }
            }
        });
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postOptimizationPlanVerification() {
        return UnionAll::checkNestedUnionAlls;
    }

    private static void checkNestedUnionAlls(LogicalPlan logicalPlan, Failures failures) {
        if (logicalPlan instanceof UnionAll unionAll) {
            unionAll.forEachDown(UnionAll.class, otherUnionAll -> {
                if (unionAll == otherUnionAll) {
                    return;
                }
                failures.add(Failure.fail(otherUnionAll, "Only a single FORK command is supported, but found multiple"));
            });
        }
    }
}
