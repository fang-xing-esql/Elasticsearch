/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SemiJoinTests extends ESTestCase {

    // -- analyzer integration tests --

    /**
     * Verifies that {@code FROM test | WHERE emp_no IN (FROM employees | KEEP emp_no)} produces a SemiJoin
     * with SEMI type after analysis.
     */
    public void testAnalyzerProducesSemiJoinForWhereIn() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_WHERE_IN.isEnabled());
        var plan = analyzeWithBothIndices("FROM test | WHERE emp_no IN (FROM employees | KEEP emp_no)");

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertFalse(semiJoin.isAntiJoin());

        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        as(semiJoin.left(), EsRelation.class);

        var rightProject = as(semiJoin.right(), Project.class);
        as(rightProject.child(), EsRelation.class);

        assertThat(semiJoin.output().size(), equalTo(semiJoin.left().output().size()));
    }

    /**
     * Verifies that {@code WHERE emp_no NOT IN (FROM employees | KEEP emp_no)} produces a SemiJoin
     * with ANTI type.
     */
    public void testAnalyzerProducesAntiJoinForWhereNotIn() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_WHERE_IN.isEnabled());
        var plan = analyzeWithBothIndices("FROM test | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)");

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertTrue(semiJoin.isAntiJoin());
    }

    /**
     * Verifies that an IN subquery combined with another filter condition produces a Filter on top of the SemiJoin.
     */
    public void testSemiJoinWithRemainingFilterCondition() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_WHERE_IN.isEnabled());
        var plan = analyzeWithBothIndices("FROM test | WHERE emp_no IN (FROM employees | KEEP emp_no) AND salary > 50000");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var semiJoin = as(filter.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
    }

    /**
     * Verifies that the SemiJoin output only includes left-side columns.
     */
    public void testSemiJoinOutputIsLeftOnly() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_WHERE_IN.isEnabled());
        var plan = analyzeWithBothIndices("FROM test | WHERE emp_no IN (FROM employees | KEEP emp_no)");

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        var leftOutput = semiJoin.left().output();
        assertThat(semiJoin.output(), equalTo(leftOutput));
    }

    // -- inlineData tests --

    /**
     * Verifies that {@link SemiJoin#inlineData} converts a SEMI join with subquery results
     * into a Filter with an In expression containing the values.
     */
    public void testInlineDataProducesFilterWithIn() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, 3) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result);
        var filter = as(inlined, Filter.class);
        var inExpr = as(filter.condition(), In.class);
        assertThat(inExpr.value(), equalTo(leftField));
        assertThat(inExpr.list().size(), equalTo(3));
    }

    /**
     * Verifies that {@link SemiJoin#inlineData} for an ANTI join wraps the In with Not.
     */
    public void testInlineDataAntiJoinProducesNotIn() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);

        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, 2) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result);
        var filter = as(inlined, Filter.class);
        var not = as(filter.condition(), Not.class);
        as(not.field(), In.class);
    }

    /**
     * Verifies that inlining an empty result for SEMI yields FALSE (no rows match),
     * and for ANTI yields TRUE (all rows pass).
     */
    public void testInlineDataEmptyResult() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        LocalRelation emptyResult = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(0)));

        // SEMI with empty result -> FALSE
        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);
        var inlined = as(SemiJoin.inlineData(semiJoin, emptyResult), Filter.class);
        assertThat(inlined.condition(), equalTo(Literal.FALSE));

        // ANTI with empty result -> TRUE
        SemiJoin antiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);
        inlined = as(SemiJoin.inlineData(antiJoin, emptyResult), Filter.class);
        assertThat(inlined.condition(), equalTo(Literal.TRUE));
    }

    // -- subplan discovery tests --

    /**
     * Verifies that {@link SemiJoin#firstSubPlan} finds the SemiJoin's right side as a subplan.
     */
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

    /**
     * Verifies that {@link SemiJoin#firstSubPlan} returns null when there are no SemiJoins.
     */
    public void testFirstSubPlanReturnsNullWithNoSemiJoin() {
        LogicalPlan plan = emptyLocalRelation(List.of(getFieldAttribute("x", DataType.INTEGER)));
        var subPlan = SemiJoin.firstSubPlan(plan, new HashSet<>());
        assertThat(subPlan, nullValue());
    }

    /**
     * Verifies that {@link SemiJoin#firstSubPlan} skips SemiJoins whose right side
     * is a LocalRelation that has already been processed.
     */
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

    /**
     * Verifies that {@link SemiJoin#newMainPlan} replaces the SemiJoin with a Filter.
     */
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

        LogicalPlan newPlan = SemiJoin.newMainPlan(semiJoin, subPlan, result);
        var filter = as(newPlan, Filter.class);
        assertThat(filter.condition(), instanceOf(In.class));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    // -- helpers --

    private static LogicalPlan analyzeWithBothIndices(String query) {
        Map<IndexPattern, IndexResolution> indexResolutions = Map.of(
            new IndexPattern(Source.EMPTY, "test"),
            loadMapping("mapping-basic.json", "test"),
            new IndexPattern(Source.EMPTY, "employees"),
            loadMapping("mapping-basic.json", "employees")
        );
        return analyze(query, analyzer(indexResolutions));
    }

    private static LocalRelation emptyLocalRelation(List<Attribute> output) {
        return new LocalRelation(Source.EMPTY, output, LocalSupplier.of(new Page(0)));
    }

    private static SemiJoin makeSemiJoin(FieldAttribute leftField, FieldAttribute rightField, JoinType joinType) {
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
