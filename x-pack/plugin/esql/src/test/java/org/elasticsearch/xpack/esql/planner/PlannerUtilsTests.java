/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class PlannerUtilsTests extends ESTestCase {

    public void testBuildSubPlanExecutionPlanRecursivelyBuildsNestedMerges() {
        List<Attribute> output = List.of(field("a"));
        LocalSourceExec branchA = localSource(output);
        LocalSourceExec branchB = localSource(output);
        LocalSourceExec branchC = localSource(output);
        MergeExec inner = new MergeExec(Source.EMPTY, List.of(branchB, branchC), output);
        MergeExec outer = new MergeExec(Source.EMPTY, List.of(branchA, inner), output);

        var executionPlan = PlannerUtils.buildSubPlanExecutionPlan(outer);

        assertThat(executionPlan, instanceOf(SubPlanExecutionPlan.Merge.class));
        var outerMerge = (SubPlanExecutionPlan.Merge) executionPlan;
        assertThat(outerMerge.plan(), instanceOf(ExchangeSourceExec.class));
        assertThat(outerMerge.children(), hasSize(2));

        assertThat(outerMerge.children().get(0), instanceOf(SubPlanExecutionPlan.Leaf.class));
        PhysicalPlan firstBranch = outerMerge.children().get(0).plan();
        assertThat(firstBranch, instanceOf(ExchangeSinkExec.class));
        assertThat(((ExchangeSinkExec) firstBranch).child(), sameInstance(branchA));

        assertThat(outerMerge.children().get(1), instanceOf(SubPlanExecutionPlan.Merge.class));
        var innerMerge = (SubPlanExecutionPlan.Merge) outerMerge.children().get(1);
        assertThat(innerMerge.plan(), instanceOf(ExchangeSinkExec.class));
        assertThat(((ExchangeSinkExec) innerMerge.plan()).child(), instanceOf(ExchangeSourceExec.class));
        assertThat(innerMerge.plan().anyMatch(MergeExec.class::isInstance), is(false));
        assertThat(innerMerge.children(), hasSize(2));
        assertThat(((ExchangeSinkExec) innerMerge.children().get(0).plan()).child(), sameInstance(branchB));
        assertThat(((ExchangeSinkExec) innerMerge.children().get(1).plan()).child(), sameInstance(branchC));
    }

    public void testBuildSubPlanExecutionPlanWithoutMergeReturnsLeaf() {
        LocalSourceExec plan = localSource(List.of(field("a")));
        var executionPlan = PlannerUtils.buildSubPlanExecutionPlan(plan);
        assertThat(executionPlan, instanceOf(SubPlanExecutionPlan.Leaf.class));
        assertThat(executionPlan.plan(), sameInstance(plan));
    }

    public void testBuildSubPlanExecutionPlanRejectsSiblingTopmostMerges() {
        List<Attribute> output = List.of(field("a"));
        MergeExec first = new MergeExec(Source.EMPTY, List.of(localSource(output), localSource(output)), output);
        MergeExec second = new MergeExec(Source.EMPTY, List.of(localSource(output), localSource(output)), output);
        HashJoinExec siblingContainer = new HashJoinExec(Source.EMPTY, first, second, List.of(), List.of(), List.of());

        var exception = expectThrows(EsqlIllegalArgumentException.class, () -> PlannerUtils.buildSubPlanExecutionPlan(siblingContainer));
        assertThat(exception.getMessage(), containsString("expected a single topmost MergeExec"));
    }

    private static FieldAttribute field(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static LocalSourceExec localSource(List<Attribute> output) {
        return new LocalSourceExec(Source.EMPTY, output, EmptyLocalSupplier.EMPTY);
    }
}
