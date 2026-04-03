/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

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
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AnalyzerInSubqueryTests extends ESTestCase {

    private static final int HASH_JOIN_THRESHOLD = PlannerSettings.IN_SUBQUERY_HASH_JOIN_THRESHOLD.getDefault(Settings.EMPTY);

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
    }

    // -- analyzer integration tests --

    /**
     * Verifies that {@code FROM test | WHERE emp_no IN (FROM employees | KEEP emp_no)} produces a SemiJoin
     * with SEMI type after analysis.
     */
    public void testAnalyzerProducesSemiJoinForWhereIn() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            """);

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
     * Verifies that {@code WHERE emp_no NOT IN (FROM employees | KEEP emp_no)} produces an AntiJoin.
     */
    public void testAnalyzerProducesAntiJoinForWhereNotIn() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var antiJoin = as(limit.child(), AntiJoin.class);

        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertTrue(antiJoin.isAntiJoin());

        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        as(antiJoin.left(), EsRelation.class);

        var rightProject = as(antiJoin.right(), Project.class);
        as(rightProject.child(), EsRelation.class);

        assertThat(antiJoin.output().size(), equalTo(antiJoin.left().output().size()));
    }

    /**
     * Verifies that an IN subquery combined with another filter condition produces a SemiJoin on top of the Filter.
     */
    public void testSemiJoinWithRemainingFilterCondition() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
              AND salary > 50000
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);
        var filter = as(semiJoin.left(), Filter.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
    }

    /**
     * Verifies that the SemiJoin output only includes left-side columns.
     */
    public void testSemiJoinOutputIsLeftOnly() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        var leftOutput = semiJoin.left().output();
        assertThat(semiJoin.output(), equalTo(leftOutput));
    }

    /**
     * Verifies that an EVAL alias used as the left side of IN subquery produces a SemiJoin
     * with the alias as the left join field.
     */
    public void testSemiJoinWithEvalAlias() {
        var plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1
            | WHERE x IN (FROM employees | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("x"));

        var eval = as(semiJoin.left(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Verifies that an EVAL alias used as the left side of NOT IN subquery produces an AntiJoin
     * with the alias as the left join field.
     */
    public void testAntiJoinWithEvalAlias() {
        var plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1
            | WHERE x NOT IN (FROM employees | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var antiJoin = as(limit.child(), AntiJoin.class);

        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("x"));

        var eval = as(antiJoin.left(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Verifies that an EVAL alias combined with a remaining filter produces a SemiJoin on top
     * of the Filter, with the Eval below the Filter.
     */
    public void testSemiJoinWithEvalAliasAndRemainingFilter() {
        var plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1
            | WHERE x IN (FROM employees | KEEP emp_no)
              AND salary > 50000
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("x"));

        var filter = as(semiJoin.left(), Filter.class);
        var eval = as(filter.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Verifies that multiple EVAL aliases can be used as left sides of IN and NOT IN subqueries.
     */
    public void testMixedSemiAndAntiJoinWithEvalAliases() {
        var plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1, y = salary * 2
            | WHERE x IN (FROM employees | KEEP emp_no)
              AND y NOT IN (FROM employees | KEEP salary)
            """);

        var limit = as(plan, Limit.class);
        var antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("y"));

        var semiJoin = as(antiJoin.left(), SemiJoin.class);
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("x"));

        var eval = as(semiJoin.left(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Verifies that an IN subquery with STATS produces a SemiJoin whose right side contains an Aggregate.
     */
    public void testSemiJoinWithStatsInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | STATS max_emp = max(emp_no))
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        as(semiJoin.left(), EsRelation.class);

        var aggregate = as(semiJoin.right(), Aggregate.class);
        as(aggregate.child(), EsRelation.class);
    }

    /**
     * Verifies that a NOT IN subquery with STATS produces an AntiJoin whose right side contains an Aggregate.
     */
    public void testAntiJoinWithStatsInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | STATS min_emp = min(emp_no))
            """);

        var limit = as(plan, Limit.class);
        var antiJoin = as(limit.child(), AntiJoin.class);

        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        as(antiJoin.left(), EsRelation.class);

        var aggregate = as(antiJoin.right(), Aggregate.class);
        as(aggregate.child(), EsRelation.class);
    }

    /**
     * Verifies that an IN subquery with STATS ... BY and KEEP produces a SemiJoin
     * whose right side is a Project over an Aggregate with grouping.
     */
    public void testSemiJoinWithStatsByInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees
                              | STATS max_emp = max(emp_no) BY languages
                              | KEEP max_emp)
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        var project = as(semiJoin.right(), Project.class);
        var aggregate = as(project.child(), Aggregate.class);
        as(aggregate.child(), EsRelation.class);
        assertThat(aggregate.groupings().size(), equalTo(1));
    }

    /**
     * Verifies that an IN subquery with STATS, SORT and LIMIT produces a SemiJoin
     * whose right side is Limit -> OrderBy -> Aggregate.
     */
    public void testSemiJoinWithStatsSortLimitInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees
                              | STATS m = max(emp_no) BY y = date_trunc(1 year, hire_date)
                              | SORT y DESC
                              | LIMIT 5
                              | KEEP m)
            """);

        var outerLimit = as(plan, Limit.class);
        var semiJoin = as(outerLimit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        as(semiJoin.left(), EsRelation.class);

        var project = as(semiJoin.right(), Project.class);
        var innerLimit = as(project.child(), Limit.class);
        var orderBy = as(innerLimit.child(), OrderBy.class);
        var aggregate = as(orderBy.child(), Aggregate.class);
        as(aggregate.child(), EsRelation.class);
        assertThat(aggregate.groupings().size(), equalTo(1));
    }

    // -- tests with commands after the WHERE IN subquery --

    /**
     * Verifies that commands after the WHERE IN subquery are placed on top of the SemiJoin.
     */
    public void testCommandsAfterSemiJoin() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | EVAL doubled = salary * 2
            | WHERE doubled > 100000
            | SORT doubled DESC
            | LIMIT 10
            | KEEP emp_no, doubled
            """);

        var outerLimit = as(plan, Limit.class);
        var project = as(outerLimit.child(), Project.class);
        var innerLimit = as(project.child(), Limit.class);
        var orderBy = as(innerLimit.child(), OrderBy.class);
        var filter = as(orderBy.child(), Filter.class);
        var eval = as(filter.child(), Eval.class);
        var semiJoin = as(eval.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(semiJoin.left(), EsRelation.class);
    }

    /**
     * Verifies that commands after the WHERE NOT IN subquery are placed on top of the AntiJoin.
     */
    public void testCommandsAfterAntiJoin() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            | EVAL doubled = salary * 2
            | SORT doubled
            | LIMIT 5
            """);

        var outerLimit = as(plan, Limit.class);
        var innerLimit = as(outerLimit.child(), Limit.class);
        var orderBy = as(innerLimit.child(), OrderBy.class);
        var eval = as(orderBy.child(), Eval.class);
        var antiJoin = as(eval.child(), AntiJoin.class);

        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        as(antiJoin.left(), EsRelation.class);
    }

    /**
     * Verifies that a second WHERE after the IN subquery is separate from the join.
     */
    public void testSecondWhereAfterSemiJoin() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | WHERE salary > 50000
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var semiJoin = as(filter.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(semiJoin.left(), EsRelation.class);
    }

    /**
     * Verifies that STATS after the IN subquery aggregates over the joined result.
     */
    public void testStatsAfterSemiJoin() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | STATS avg_salary = avg(salary) BY languages
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var semiJoin = as(aggregate.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(semiJoin.left(), EsRelation.class);
    }

    // -- tests with FROM subquery inside the IN subquery --

    /**
     * Verifies that a FROM subquery inside the IN subquery produces a SemiJoin
     * whose right side contains a UnionAll.
     */
    public void testSemiJoinWithFromSubqueryInsideInSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees, (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(semiJoin.left(), EsRelation.class);

        var project = as(semiJoin.right(), Project.class);
        var unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
    }

    /**
     * Verifies that a NOT IN subquery containing a FROM subquery produces an AntiJoin
     * whose right side contains a UnionAll.
     */
    public void testAntiJoinWithFromSubqueryInsideInSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees, (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var antiJoin = as(limit.child(), AntiJoin.class);

        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        as(antiJoin.left(), EsRelation.class);

        var project = as(antiJoin.right(), Project.class);
        var unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
    }

    /**
     * Verifies that a FROM subquery containing a WHERE IN subquery produces a UnionAll
     * with a SemiJoin inside the subquery branch.
     */
    public void testFromSubqueryWithInSubqueryInside() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = analyzeInSubquery("""
            FROM test,
                 (FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // main query: Project -> EsRelation (Project added for field alignment in UnionAll)
        var mainProject = as(unionAll.children().get(0), Project.class);
        as(mainProject.child(), EsRelation.class);

        // FROM subquery: Project -> SemiJoin
        // FROM subquery: Project (alignment) -> Eval (null columns) -> Subquery -> Project -> SemiJoin
        var alignProject = as(unionAll.children().get(1), Project.class);
        var subEval = as(alignProject.child(), Eval.class);
        var subquery = as(subEval.child(), Subquery.class);
        var subProject = as(subquery.child(), Project.class);
        var semiJoin = as(subProject.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(semiJoin.left(), EsRelation.class);
    }

    /**
     * Verifies that a FROM subquery containing a WHERE NOT IN subquery produces a UnionAll
     * with an AntiJoin inside the subquery branch.
     */
    public void testFromSubqueryWithNotInSubqueryInside() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = analyzeInSubquery("""
            FROM test,
                 (FROM employees | WHERE emp_no NOT IN (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        var mainProject = as(unionAll.children().get(0), Project.class);
        as(mainProject.child(), EsRelation.class);

        var alignProject = as(unionAll.children().get(1), Project.class);
        var subEval = as(alignProject.child(), Eval.class);
        var subquery = as(subEval.child(), Subquery.class);
        var subProject = as(subquery.child(), Project.class);
        var antiJoin = as(subProject.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        as(antiJoin.left(), EsRelation.class);
    }

    // -- negative analyzer/verifier tests --

    /**
     * Verifies that an IN subquery returning two columns (KEEP emp_no, salary) is rejected.
     */
    public void testRejectsInSubqueryWithMultipleColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no, salary)
            """, containsString("IN subquery must return exactly one column, found [emp_no, salary]"));
    }

    /**
     * Verifies that a NOT IN subquery returning two columns is rejected.
     */
    public void testRejectsNotInSubqueryWithMultipleColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no, salary)
            """, containsString("IN subquery must return exactly one column, found [emp_no, salary]"));
    }

    /**
     * Verifies that an IN subquery returning all columns (no KEEP) is rejected.
     */
    public void testRejectsInSubqueryWithAllColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees)
            """, containsString("IN subquery must return exactly one column"));
    }

    /**
     * Verifies that an IN subquery with integer left side and keyword right side is rejected.
     */
    public void testRejectsTypeMismatchIntegerVsKeyword() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP first_name)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [first_name] of type [KEYWORD]"));
    }

    /**
     * Verifies that a NOT IN subquery with keyword left side and integer right side is rejected.
     */
    public void testRejectsTypeMismatchKeywordVsInteger() {
        errorInSubquery("""
            FROM test
            | WHERE first_name NOT IN (FROM employees | KEEP emp_no)
            """, containsString("left field [first_name] of type [KEYWORD] is incompatible with right field [emp_no] of type [INTEGER]"));
    }

    /**
     * Verifies that an IN subquery with integer left side and date right side is rejected.
     */
    public void testRejectsTypeMismatchIntegerVsDate() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP hire_date)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [hire_date] of type [DATETIME]"));
    }

    /**
     * Verifies that an IN subquery with STATS ... BY returning two columns is rejected.
     */
    public void testRejectsInSubqueryWithStatsByReturningMultipleColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | STATS max(emp_no) BY languages)
            """, containsString("IN subquery must return exactly one column, found [max(emp_no), languages]"));
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

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD);
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

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD);
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
        var inlined = as(SemiJoin.inlineData(semiJoin, emptyResult, HASH_JOIN_THRESHOLD), Filter.class);
        assertThat(inlined.condition(), equalTo(Literal.FALSE));

        // ANTI with empty result -> TRUE
        SemiJoin antiJoin = makeSemiJoin(leftField, rightField, JoinTypes.ANTI);
        inlined = as(SemiJoin.inlineData(antiJoin, emptyResult, HASH_JOIN_THRESHOLD), Filter.class);
        assertThat(inlined.condition(), equalTo(Literal.TRUE));
    }

    // -- hash join threshold tests --

    /**
     * Verifies that a SEMI join with more than HASH_JOIN_THRESHOLD values produces a
     * Project -> Filter(IS NOT NULL) -> Join(LEFT) instead of Filter(IN(...)).
     */
    public void testInlineDataSemiJoinAboveThresholdProducesHashJoin() {
        FieldAttribute leftField = getFieldAttribute("emp_no", DataType.INTEGER);
        FieldAttribute rightField = getFieldAttribute("emp_no", DataType.INTEGER);

        SemiJoin semiJoin = makeSemiJoin(leftField, rightField, JoinTypes.SEMI);

        int count = HASH_JOIN_THRESHOLD + 1;
        Block[] blocks = new Block[] { BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 10, count) };
        LocalRelation result = new LocalRelation(Source.EMPTY, List.of(rightField), LocalSupplier.of(new Page(blocks)));

        LogicalPlan inlined = SemiJoin.inlineData(semiJoin, result, HASH_JOIN_THRESHOLD);

        // Project -> Filter -> Join(LEFT)
        var project = as(inlined, Project.class);
        var filter = as(project.child(), Filter.class);
        as(filter.condition(), IsNotNull.class);
        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
    }

    /**
     * Verifies that an ANTI join with more than HASH_JOIN_THRESHOLD values produces a
     * Project -> Filter(IS NULL) -> Join(LEFT).
     */
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

    /**
     * Verifies that exactly at the threshold, Filter(IN(...)) is still used.
     */
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

        LogicalPlan newPlan = SemiJoin.newMainPlan(semiJoin, subPlan, result, HASH_JOIN_THRESHOLD);
        var filter = as(newPlan, Filter.class);
        assertThat(filter.condition(), instanceOf(In.class));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    // -- helpers --

    private static LogicalPlan analyzeInSubquery(String query) {
        return analyzer().addIndex("test", "mapping-basic.json").addIndex("employees", "mapping-basic.json").query(query);
    }

    private static void errorInSubquery(String query, Matcher<String> messageMatcher) {
        analyzer().addIndex("test", "mapping-basic.json").addIndex("employees", "mapping-basic.json").error(query, messageMatcher);
    }

    private static LocalRelation emptyLocalRelation(List<Attribute> output) {
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
