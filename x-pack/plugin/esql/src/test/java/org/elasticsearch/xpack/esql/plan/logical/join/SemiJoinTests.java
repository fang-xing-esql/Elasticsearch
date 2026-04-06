/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link SemiJoin} static methods: inlineData, firstSubPlan, newMainPlan.
 */
public class SemiJoinTests extends ESTestCase {

    private static final int HASH_JOIN_THRESHOLD = PlannerSettings.IN_SUBQUERY_HASH_JOIN_THRESHOLD.getDefault(Settings.EMPTY);

    // -- inlineData tests --

    public void testInlineDataProducesFilterWithIn() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, 3) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD);
        var filter = as(inlined, Filter.class);
        var inExpr = as(filter.condition(), In.class);
        assertThat(inExpr.value(), equalTo(leftField));
        assertThat(inExpr.list().size(), equalTo(3));
    }

    public void testInlineDataAntiJoinProducesNotIn() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);

        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, 2) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD);
        var filter = as(inlined, Filter.class);
        var not = as(filter.condition(), Not.class);
        as(not.field(), In.class);
    }

    public void testInlineDataEmptyResult() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        LocalRelation emptyResult = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(0)));

        // SEMI with empty result -> FALSE
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);
        var inlined = as(SemiJoin.inlineData(semiJoin, emptyResult, HASH_JOIN_THRESHOLD), Filter.class);
        assertThat(inlined.condition(), equalTo(Literal.FALSE));

        // ANTI with empty result -> TRUE
        SemiJoin antiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);
        inlined = as(SemiJoin.inlineData(antiJoin, emptyResult, HASH_JOIN_THRESHOLD), Filter.class);
        assertThat(inlined.condition(), equalTo(Literal.TRUE));
    }

    // -- hash join threshold tests --

    public void testInlineDataSemiJoinAboveThresholdProducesHashJoin() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        int count = HASH_JOIN_THRESHOLD + 1;
        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, count) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD);

        var project = as(inlined, Project.class);
        var filter = as(project.child(), Filter.class);
        as(filter.condition(), IsNotNull.class);
        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
    }

    public void testInlineDataAntiJoinAboveThresholdProducesHashJoin() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin antiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);

        int count = HASH_JOIN_THRESHOLD + 1;
        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, count) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(antiJoin, result, HASH_JOIN_THRESHOLD);

        var project = as(inlined, Project.class);
        var filter = as(project.child(), Filter.class);
        as(filter.condition(), IsNull.class);
        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
    }

    public void testInlineDataAtThresholdStillUsesFilter() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        int count = HASH_JOIN_THRESHOLD;
        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, count) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD);

        var filter = as(inlined, Filter.class);
        as(filter.condition(), In.class);
    }

    // -- subplan discovery tests --

    public void testFirstSubPlanFindsSemiJoin() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        LogicalPlan rightPlan = emptyLocalRelation(List.of(rightField));

        SemiJoin semiJoin = new SemiJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            rightPlan,
            JoinTypes.SEMI,
            List.of(leftField),
            List.of(rightField)
        );

        var subPlan = SemiJoin.firstSubPlan(semiJoin, new HashSet<>());
        assertThat(subPlan, notNullValue());
    }

    public void testFirstSubPlanReturnsNullWithNoSemiJoin() {
        LogicalPlan plan = emptyLocalRelation(List.of(getFieldAttribute("x", DataType.INTEGER)));
        var subPlan = SemiJoin.firstSubPlan(plan, new HashSet<>());
        assertThat(subPlan, nullValue());
    }

    public void testFirstSubPlanSkipsAlreadyProcessedResults() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        LocalRelation alreadyProcessed = new LocalRelation(
            Source.EMPTY,
            List.of(rightField),
            LocalSupplier.of(new Page(BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 1, 1)))
        );

        SemiJoin semiJoin = new SemiJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            alreadyProcessed,
            JoinTypes.SEMI,
            List.of(leftField),
            List.of(rightField)
        );

        Set<LocalRelation> subPlansResults = new HashSet<>();
        subPlansResults.add(alreadyProcessed);

        var subPlan = SemiJoin.firstSubPlan(semiJoin, subPlansResults);
        assertThat(subPlan, nullValue());
    }

    // -- newMainPlan tests --

    public void testNewMainPlanReplacesSemiJoinWithFilter() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);
        LogicalPlan rightPlan = emptyLocalRelation(List.of(rightField));

        SemiJoin semiJoin = new SemiJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            rightPlan,
            JoinTypes.SEMI,
            List.of(leftField),
            List.of(rightField)
        );

        var subPlan = SemiJoin.firstSubPlan(semiJoin, new HashSet<>());
        assertThat(subPlan, notNullValue());

        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 42, 1) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan newPlan = SemiJoin.newMainPlan(semiJoin, subPlan, result, HASH_JOIN_THRESHOLD);
        var filter = as(newPlan, Filter.class);
        assertThat(filter.condition(), instanceOf(In.class));
    }

    // -- helpers --

    private static LocalRelation emptyLocalRelation(List<org.elasticsearch.xpack.esql.core.expression.Attribute> output) {
        return new LocalRelation(Source.EMPTY, output, LocalSupplier.of(new Page(0)));
    }

    private static SemiJoin makeSemiJoin(FieldAttribute leftField, FieldAttribute rightField, JoinType joinType) {
        if (JoinTypes.ANTI.equals(joinType)) {
            return new AntiJoin(
                Source.EMPTY,
                emptyLocalRelation(List.of(leftField)),
                emptyLocalRelation(List.of(rightField)),
                List.of(leftField),
                List.of(rightField)
            );
        }
        return new SemiJoin(
            Source.EMPTY,
            emptyLocalRelation(List.of(leftField)),
            emptyLocalRelation(List.of(rightField)),
            joinType,
            List.of(leftField),
            List.of(rightField)
        );
    }
}
