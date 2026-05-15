/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Tests for subqueries and views inside the FROM command. Companion class to {@link AnalyzerTests};
 * extracted so the subquery/view analysis test surface can grow without dwarfing the main analyzer tests.
 */
public class AnalyzerSubqueryTests extends ESTestCase {

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testSubqueryInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = basic().addLanguages().query("""
            FROM test, (FROM languages | WHERE language_code > 1)
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(2, order.size());
        ReferenceAttribute empNo = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        ReferenceAttribute languageCode = as(order.get(1).child(), ReferenceAttribute.class);
        assertEquals("language_code", languageCode.name());
        Filter filter = as(orderBy.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        List<? extends NamedExpression> projections = subqueryProject.projections();
        assertEquals(13, projections.size()); // all fields from the two indices
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        List<Alias> aliases = subqueryEval.fields(); // nullEvals from languages index
        assertEquals(2, aliases.size());
        assertEquals("language_code", aliases.get(0).name());
        Literal nullLiteral = as(aliases.get(0).child(), Literal.class);
        assertNull(nullLiteral.value());
        assertEquals(INTEGER, nullLiteral.dataType());
        assertEquals("language_name", aliases.get(1).name());
        nullLiteral = as(aliases.get(1).child(), Literal.class);
        assertNull(nullLiteral.value());
        assertEquals(KEYWORD, nullLiteral.dataType());
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        projections = subqueryProject.projections();
        assertEquals(13, projections.size()); // all fields from the two indices
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from test index
        assertEquals(11, aliases.size());
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());
    }

    public void testViewInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        LogicalPlan plan = basic().addLanguages().addView("view", "FROM languages | WHERE language_code > 1").query("""
            FROM test, view
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(2, order.size());
        ReferenceAttribute empNo = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        ReferenceAttribute languageCode = as(order.get(1).child(), ReferenceAttribute.class);
        assertEquals("language_code", languageCode.name());
        Filter filter = as(orderBy.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        ViewUnionAll viewUnionAll = as(filter.child(), ViewUnionAll.class);
        assertEquals(2, viewUnionAll.children().size());

        Project viewProject = as(viewUnionAll.children().get(0), Project.class);
        List<? extends NamedExpression> projections = viewProject.projections();
        assertEquals(13, projections.size()); // all fields from the two indices
        Eval viewEval = as(viewProject.child(), Eval.class);
        List<Alias> aliases = viewEval.fields(); // nullEvals from languages index
        assertEquals(2, aliases.size());
        assertEquals("language_code", aliases.get(0).name());
        Literal nullLiteral = as(aliases.get(0).child(), Literal.class);
        assertNull(nullLiteral.value());
        assertEquals(INTEGER, nullLiteral.dataType());
        assertEquals("language_name", aliases.get(1).name());
        nullLiteral = as(aliases.get(1).child(), Literal.class);
        assertNull(nullLiteral.value());
        assertEquals(KEYWORD, nullLiteral.dataType());
        EsRelation subqueryIndex = as(viewEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        viewProject = as(viewUnionAll.children().get(1), Project.class);
        projections = viewProject.projections();
        assertEquals(13, projections.size()); // all fields from the two indices
        viewEval = as(viewProject.child(), Eval.class);
        aliases = viewEval.fields(); // nullEvals from test index
        assertEquals(11, aliases.size());
        Filter subqueryFilter = as(viewEval.child(), Filter.class);
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());
    }

    /**
     * If there is only one subquery in the main from command, the subquery is merged into the main index pattern
     */
    public void testSubqueryInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = basic().addLanguages().query("""
            FROM (FROM languages | WHERE language_code > 1)
            | WHERE language_name is not null
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        FieldAttribute language_name = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        filter = as(filter.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("languages", relation.indexPattern());
    }

    /**
     * If there is only one view in the main from command, the view is merged into the main index pattern
     */
    public void testViewInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        LogicalPlan plan = basic().addLanguages().addView("view", "FROM languages | WHERE language_code > 1").query("""
            FROM view
            | WHERE language_name is not null
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        FieldAttribute language_name = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        filter = as(filter.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("languages", relation.indexPattern());
    }

    public void testMultipleSubqueriesInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = basic().addLanguages().addSampleData().addLanguagesLookup().query("""
            FROM test
            , (FROM languages | WHERE language_code > 10 | RENAME language_name as languageName)
            , (FROM sample_data | STATS max(@timestamp))
            , (FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE emp_no > 10000
            | STATS count(*) by emp_no, language_code
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """);

        Limit limit = as(plan, Limit.class);
        MvExpand mvExpand = as(limit.child(), MvExpand.class);
        NamedExpression mvExpandTarget = as(mvExpand.target(), NamedExpression.class);
        assertEquals("languageCode", mvExpandTarget.name());
        ReferenceAttribute mvExpandExpanded = as(mvExpand.expanded(), ReferenceAttribute.class);
        assertEquals("languageCode", mvExpandExpanded.name());
        Project rename = as(mvExpand.child(), Project.class);
        List<? extends NamedExpression> projections = rename.projections();
        assertEquals(3, projections.size());
        Alias a = as(projections.get(1), Alias.class);
        assertEquals("empNo", a.name());
        ReferenceAttribute ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        a = as(projections.get(2), Alias.class);
        assertEquals("languageCode", a.name());
        ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Aggregate aggregate = as(rename.child(), Aggregate.class);
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertEquals(3, aggregates.size());
        a = as(aggregates.get(0), Alias.class);
        assertEquals("count(*)", a.name());
        List<Expression> groupings = aggregate.groupings();
        assertEquals(2, groupings.size());
        ra = as(groupings.get(0), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        ra = as(groupings.get(1), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Filter filter = as(aggregate.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(4, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        List<Alias> aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(4, aliases.size());
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(13, aliases.size());
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        rename = as(subquery.child(), Project.class);
        List<? extends NamedExpression> renameProjections = rename.projections();
        assertEquals(2, renameProjections.size());
        FieldAttribute language_code = as(renameProjections.get(0), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        a = as(renameProjections.get(1), Alias.class);
        assertEquals("languageName", a.name());
        FieldAttribute language_name = as(a.child(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        Filter subqueryFilter = as(rename.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(10, literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(14, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(3), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(2, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        LookupJoin lookupJoin = as(subquery.child(), LookupJoin.class);
        subqueryIndex = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", subqueryIndex.indexPattern());
        subqueryEval = as(lookupJoin.left(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());
    }

    public void testMultipleViewsInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        LogicalPlan plan = basic().addLanguages()
            .addSampleData()
            .addLanguagesLookup()
            .addView("view1", "FROM languages | WHERE language_code > 10 | RENAME language_name as languageName")
            .addView("view2", "FROM sample_data | STATS max(@timestamp)")
            .addView("view3", "FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code")
            .query("""
                FROM test, view1, view2, view3
                | WHERE emp_no > 10000
                | STATS count(*) by emp_no, language_code
                | RENAME emp_no AS empNo, language_code AS languageCode
                | MV_EXPAND languageCode
                """);

        Limit limit = as(plan, Limit.class);
        MvExpand mvExpand = as(limit.child(), MvExpand.class);
        NamedExpression mvExpandTarget = as(mvExpand.target(), NamedExpression.class);
        assertEquals("languageCode", mvExpandTarget.name());
        ReferenceAttribute mvExpandExpanded = as(mvExpand.expanded(), ReferenceAttribute.class);
        assertEquals("languageCode", mvExpandExpanded.name());
        Project rename = as(mvExpand.child(), Project.class);
        List<? extends NamedExpression> projections = rename.projections();
        assertEquals(3, projections.size());
        Alias a = as(projections.get(1), Alias.class);
        assertEquals("empNo", a.name());
        ReferenceAttribute ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        a = as(projections.get(2), Alias.class);
        assertEquals("languageCode", a.name());
        ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Aggregate aggregate = as(rename.child(), Aggregate.class);
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertEquals(3, aggregates.size());
        a = as(aggregates.get(0), Alias.class);
        assertEquals("count(*)", a.name());
        List<Expression> groupings = aggregate.groupings();
        assertEquals(2, groupings.size());
        ra = as(groupings.get(0), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        ra = as(groupings.get(1), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Filter filter = as(aggregate.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        ViewUnionAll viewUninAll = as(filter.child(), ViewUnionAll.class);
        assertEquals(4, viewUninAll.children().size());

        Project viewProject = as(viewUninAll.children().get(0), Project.class);
        projections = viewProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        Eval viewEval = as(viewProject.child(), Eval.class);
        List<Alias> aliases = viewEval.fields(); // nullEvals from the other legs
        assertEquals(4, aliases.size());
        EsRelation subqueryIndex = as(viewEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        viewProject = as(viewUninAll.children().get(1), Project.class);
        projections = viewProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        viewEval = as(viewProject.child(), Eval.class);
        aliases = viewEval.fields(); // nullEvals from the other legs
        assertEquals(13, aliases.size());
        rename = as(viewEval.child(), Project.class);
        List<? extends NamedExpression> renameProjections = rename.projections();
        assertEquals(2, renameProjections.size());
        FieldAttribute language_code = as(renameProjections.get(0), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        a = as(renameProjections.get(1), Alias.class);
        assertEquals("languageName", a.name());
        FieldAttribute language_name = as(a.child(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        Filter subqueryFilter = as(rename.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(10, literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        viewProject = as(viewUninAll.children().get(2), Project.class);
        projections = viewProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        viewEval = as(viewProject.child(), Eval.class);
        aliases = viewEval.fields(); // nullEvals from the other legs
        assertEquals(14, aliases.size());
        Aggregate subqueryAggregate = as(viewEval.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

        viewProject = as(viewUninAll.children().get(3), Project.class);
        projections = viewProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        viewEval = as(viewProject.child(), Eval.class);
        aliases = viewEval.fields(); // nullEvals from the other legs
        assertEquals(2, aliases.size());
        LookupJoin lookupJoin = as(viewEval.child(), LookupJoin.class);
        subqueryIndex = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", subqueryIndex.indexPattern());
        viewEval = as(lookupJoin.left(), Eval.class);
        subqueryIndex = as(viewEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());
    }

    public void testMultipleSubqueryInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = basic().addLanguages().addSampleData().addLanguagesLookup().query("""
            FROM (FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            , (FROM languages | WHERE language_code > 10 | RENAME language_name as languageName)
            , (FROM sample_data | STATS max(@timestamp))
            | WHERE emp_no > 10000
            | STATS count(*) by emp_no, language_code
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """);

        Limit limit = as(plan, Limit.class);
        MvExpand mvExpand = as(limit.child(), MvExpand.class);
        NamedExpression mvExpandTarget = as(mvExpand.target(), NamedExpression.class);
        assertEquals("languageCode", mvExpandTarget.name());
        ReferenceAttribute mvExpandExpanded = as(mvExpand.expanded(), ReferenceAttribute.class);
        assertEquals("languageCode", mvExpandExpanded.name());
        Project rename = as(mvExpand.child(), Project.class);
        List<? extends NamedExpression> projections = rename.projections();
        assertEquals(3, projections.size());
        Alias a = as(projections.get(1), Alias.class);
        assertEquals("empNo", a.name());
        ReferenceAttribute ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        a = as(projections.get(2), Alias.class);
        assertEquals("languageCode", a.name());
        ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Aggregate aggregate = as(rename.child(), Aggregate.class);
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertEquals(3, aggregates.size());
        a = as(aggregates.get(0), Alias.class);
        assertEquals("count(*)", a.name());
        List<Expression> groupings = aggregate.groupings();
        assertEquals(2, groupings.size());
        ra = as(groupings.get(0), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        ra = as(groupings.get(1), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Filter filter = as(aggregate.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        List<Alias> aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(2, aliases.size());
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        LookupJoin lookupJoin = as(subquery.child(), LookupJoin.class);
        EsRelation subqueryIndex = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", subqueryIndex.indexPattern());
        subqueryEval = as(lookupJoin.left(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(13, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        rename = as(subquery.child(), Project.class);
        List<? extends NamedExpression> renameProjections = rename.projections();
        assertEquals(2, renameProjections.size());
        FieldAttribute language_code = as(renameProjections.get(0), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        a = as(renameProjections.get(1), Alias.class);
        assertEquals("languageName", a.name());
        FieldAttribute language_name = as(a.child(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        Filter subqueryFilter = as(rename.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(10, literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(14, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

    }

    public void testNestedSubqueryInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = basic().addLanguages().addSampleData().query("""
            FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10)
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        unionAll = as(subqueryFilter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        subqueryProject = as(unionAll.children().get(0), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
    }

    public void testNestedSubqueryInFromWithMetadata() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = basic().addLanguages().addSampleData().query("""
            FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10) metadata _index
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());
        List<Attribute> output = subqueryIndex.output();
        assertEquals(12, output.size());
        MetadataAttribute metadataAttribute = as(output.get(11), MetadataAttribute.class);
        assertEquals("_index", metadataAttribute.name());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        unionAll = as(subqueryFilter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        subqueryProject = as(unionAll.children().get(0), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());
        output = subqueryIndex.output();
        assertEquals(2, output.size());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
        output = subqueryIndex.output();
        assertEquals(4, output.size());
    }

    public void testNestedSubqueriesInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = basic().addSampleData().query("""
            FROM (FROM test, (FROM sample_data | STATS count(*)) | WHERE emp_no > 10)
            | WHERE languages is not null
            | SORT emp_no, languages
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> orderKeys = orderBy.order();
        assertEquals(2, orderKeys.size());
        ReferenceAttribute emp_no = as(orderKeys.get(0).child(), ReferenceAttribute.class);
        assertEquals("emp_no", emp_no.name());
        ReferenceAttribute languages = as(orderKeys.get(1).child(), ReferenceAttribute.class);
        assertEquals("languages", languages.name());
        Filter filter = as(orderBy.child(), Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        languages = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("languages", languages.name());
        filter = as(filter.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        emp_no = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", emp_no.name());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
    }

    /*
     * When there are mixed date types between the main query and the subquery, the fields/references need to be casted to a common type
     * in the UnionAll legs, otherwise FORK's postAnalysisPlanVerification will fail. The common type can be date_nanos,
     * or unsupported if the fields have conflicting types (regardless of whether they are referenced in the main query).
     */
    public void testMixedDataTypesInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = defaultMapping().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types | WHERE languages > 0)
            | EVAL emp_no = emp_no::long
            | WHERE emp_no > 10000
            | SORT emp_no
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(25, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        Eval eval = as(filter.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(1, aliases.size());
        Alias alias = aliases.get(0);
        assertEquals("emp_no", alias.name());
        ReferenceAttribute emp_no = as(alias.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(26, output.size());
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(10, implicitCastingEval.fields().size());
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(1, explicitCastingEval.fields().size());
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(2, missingFieldEval.fields().size());
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(9, implicitCastingEval.fields().size());
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(1, explicitCastingEval.fields().size());
        missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(5, missingFieldEval.fields().size());
        Subquery subquery = as(missingFieldEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        GreaterThan greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute fa = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", fa.name());
        assertEquals(INTEGER, fa.dataType());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(0, literal.value());
        assertEquals(INTEGER, literal.dataType());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", subqueryIndex.indexPattern());
    }

    public void testMixedDataTypesWithExplicitCastingInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = defaultMapping().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types | WHERE languages > 0)
            | EVAL emp_no = emp_no::long
            | WHERE emp_no > 10000
            | EVAL still_hired = still_hired::string, is_rehired = is_rehired::string
            | SORT still_hired, is_rehired
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(25, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Eval eval = as(orderBy.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(2, aliases.size());
        Alias a = aliases.get(0);
        assertEquals("still_hired", a.name());
        ReferenceAttribute still_hired = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$still_hired$converted_to$keyword", still_hired.name());
        a = aliases.get(1);
        assertEquals("is_rehired", a.name());
        ReferenceAttribute is_rehired = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$is_rehired$converted_to$keyword", is_rehired.name());
        Filter filter = as(eval.child(), Filter.class);
        eval = as(filter.child(), Eval.class);
        aliases = eval.fields();
        assertEquals(1, aliases.size());
        a = aliases.get(0);
        assertEquals("emp_no", a.name());
        ReferenceAttribute emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(28, output.size());
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(10, implicitCastingEval.fields().size());
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(3, explicitCastingEval.fields().size());
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(2, missingFieldEval.fields().size());
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(9, implicitCastingEval.fields().size());
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(3, explicitCastingEval.fields().size());
        missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(5, missingFieldEval.fields().size());
        Subquery subquery = as(missingFieldEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        GreaterThan greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute fa = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", fa.name());
        assertEquals(INTEGER, fa.dataType());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(0, literal.value());
        assertEquals(INTEGER, literal.dataType());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", subqueryIndex.indexPattern());
    }

    public void testMixedDataTypesWithMultipleExplicitCastingInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = defaultMapping().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types | WHERE languages > 0)
            | EVAL x = emp_no::long, y = emp_no::string, z = emp_no::double, first_name = first_name::string
            | WHERE z > 10000
            | EVAL still_hired = still_hired::string, is_rehired = is_rehired::string
            | SORT still_hired, is_rehired
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(28, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Eval eval = as(orderBy.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(2, aliases.size());
        Alias a = aliases.get(0);
        assertEquals("still_hired", a.name());
        ReferenceAttribute still_hired = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$still_hired$converted_to$keyword", still_hired.name());
        a = aliases.get(1);
        assertEquals("is_rehired", a.name());
        ReferenceAttribute is_rehired = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$is_rehired$converted_to$keyword", is_rehired.name());
        Filter filter = as(eval.child(), Filter.class);
        eval = as(filter.child(), Eval.class);
        aliases = eval.fields();
        assertEquals(4, aliases.size());
        a = aliases.get(0);
        assertEquals("x", a.name());
        ReferenceAttribute emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        a = aliases.get(1);
        assertEquals("y", a.name());
        emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$keyword", emp_no.name());
        a = aliases.get(2);
        assertEquals("z", a.name());
        emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$double", emp_no.name());
        a = aliases.get(3);
        assertEquals("first_name", a.name());
        ReferenceAttribute first_name = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$first_name$converted_to$keyword", first_name.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(31, output.size());
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(10, implicitCastingEval.fields().size());
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(6, explicitCastingEval.fields().size());
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(2, missingFieldEval.fields().size());
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(9, implicitCastingEval.fields().size());
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(6, explicitCastingEval.fields().size());
        missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(5, missingFieldEval.fields().size());
        Subquery subquery = as(missingFieldEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        GreaterThan greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute fa = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", fa.name());
        assertEquals(INTEGER, fa.dataType());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(0, literal.value());
        assertEquals(INTEGER, literal.dataType());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", subqueryIndex.indexPattern());
    }

    public void testSubqueryWithUnionAllOutputOverwritten() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = basic().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types | WHERE languages > 1)
            | EVAL emp_no = languages::long
            | WHERE emp_no > 1
            | SORT emp_no
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(24, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute emp_no = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", emp_no.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        Eval eval = as(filter.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(1, aliases.size());
        Alias alias = aliases.get(0);
        assertEquals("emp_no", alias.name());
        ReferenceAttribute language_code = as(alias.child(), ReferenceAttribute.class);
        assertEquals("$$languages$converted_to$long", language_code.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(25, output.size());
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        Subquery subquery = as(missingFieldEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute fa = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", fa.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        assertEquals(INTEGER, literal.dataType());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", subqueryIndex.indexPattern());
    }

    public void testUnionAllWithConflictingTypesFromSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        LogicalPlan plan = sampleData().query("""
            FROM (FROM sample_data), (FROM sample_data | EVAL client_ip = 1) | keep client_ip
            """);

        // Limit[1000]
        Limit limit = as(plan, Limit.class);

        // Project[[!client_ip]] — client_ip is UnsupportedAttribute due to type conflict (ip vs integer)
        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        UnsupportedAttribute ua = as(projections.getFirst(), UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        List<String> originalTypes = ua.originalTypes();
        assertThat(originalTypes, hasSize(2));
        assertThat(originalTypes, is(List.of(IP.esType(), INTEGER.esType())));
        assertEquals("client_ip", ua.name());

        // UnionAll[[@timestamp, !client_ip, event_duration, message]]
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Left leg: Project → Eval[null[KEYWORD] AS client_ip] → Subquery → EsRelation[sample_data]
        Project leftProject = as(unionAll.children().get(0), Project.class);
        Eval leftEval = as(leftProject.child(), Eval.class);
        List<Alias> leftAliases = leftEval.fields();
        assertThat(leftAliases, hasSize(1));
        Alias leftAlias = leftAliases.getFirst();
        assertEquals("client_ip", leftAlias.name());
        Literal leftNull = as(leftAlias.child(), Literal.class);
        assertNull(leftNull.value());
        assertEquals(KEYWORD, leftNull.dataType());

        Subquery leftSubquery = as(leftEval.child(), Subquery.class);
        EsRelation leftRelation = as(leftSubquery.child(), EsRelation.class);

        // Right leg: Project → Eval[null[KEYWORD] AS client_ip] → Subquery → Eval[1[INTEGER] AS client_ip] → EsRelation[sample_data]
        Project rightProject = as(unionAll.children().get(1), Project.class);
        Eval rightEval = as(rightProject.child(), Eval.class);
        List<Alias> rightAliases = rightEval.fields();
        assertThat(rightAliases, hasSize(1));
        Alias rightAlias = rightAliases.getFirst();
        assertEquals("client_ip", rightAlias.name());
        Literal rightNull = as(rightAlias.child(), Literal.class);
        assertNull(rightNull.value());
        assertEquals(KEYWORD, rightNull.dataType());

        Subquery rightSubquery = as(rightEval.child(), Subquery.class);
        Eval innerEval = as(rightSubquery.child(), Eval.class);
        List<Alias> innerAliases = innerEval.fields();
        assertThat(innerAliases, hasSize(1));
        Alias innerAlias = innerAliases.getFirst();
        assertEquals("client_ip", innerAlias.name());
        Literal one = as(innerAlias.child(), Literal.class);
        assertEquals(1, one.value());
        assertEquals(INTEGER, one.dataType());
        EsRelation rightRelation = as(innerEval.child(), EsRelation.class);
    }

    public void testUnionAllWithConflictingTypesFromSubqueriesWithoutUsageInMainQuery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        LogicalPlan plan = sampleData().query("""
            FROM (FROM sample_data), (FROM sample_data | EVAL client_ip = 1)
            """);

        // Limit[1000]
        Limit limit = as(plan, Limit.class);

        // Limit directly over UnionAll since there is no keep/project
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        List<Attribute> output = unionAll.output();
        Attribute clientIpAttr = output.stream().filter(a -> "client_ip".equals(a.name())).findFirst().orElseThrow();
        UnsupportedAttribute ua = as(clientIpAttr, UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(List.of(IP.esType(), INTEGER.esType())));
        assertEquals("client_ip", ua.name());
    }

    public void testUnionAllWithConflictingNumericTypesFromSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        LogicalPlan plan = defaultMapping().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types) | keep emp_no
            """);

        // Limit[1000]
        Limit limit = as(plan, Limit.class);

        // Project[[!emp_no]]
        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        UnsupportedAttribute ua = as(projections.getFirst(), UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(List.of(INTEGER.esType(), LONG.esType())));
        assertEquals("emp_no", ua.name());
    }

    public void testSubqueryWithTimeSeriesIndexInMainQuery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = k8s().addSampleData().query("""
            FROM k8s, (FROM sample_data), (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(24, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval eval = as(subqueryProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        relation = as(subquery.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        filter = as(subquery.child(), Filter.class);
        relation = as(filter.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    public void testSubqueryWithTimeSeriesIndexInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = sampleData().addK8sDownsampled().query("""
            FROM sample_data,
                       (FROM k8s | EVAL a = TO_AGGREGATE_METRIC_DOUBLE(1) | INLINE STATS tx_max = MAX(network.eth0.tx) BY pod),
                       (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(26, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval eval = as(subqueryProject.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        InlineStats inlineStats = as(subquery.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        eval = as(aggregate.child(), Eval.class);
        relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        filter = as(subquery.child(), Filter.class);
        relation = as(filter.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    public void testSubqueryWithTimeSeriesIndexInMainQueryAndSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = k8s().addSampleData().query("""
            FROM k8s,
                       (FROM k8s | EVAL a = TO_AGGREGATE_METRIC_DOUBLE(1) | INLINE STATS tx_max = MAX(network.eth0.tx) BY pod),
                       (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(26, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval eval = as(subqueryProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        InlineStats inlineStats = as(subquery.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        eval = as(aggregate.child(), Eval.class);
        relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        filter = as(subquery.child(), Filter.class);
        relation = as(filter.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    public void testSubqueryWithFullTextFunctionInMainQuery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = basic().addSampleData().query("""
            FROM sample_data, (FROM sample_data | WHERE message:"error")
            | WHERE match(client_ip,"127.0.0.1")
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Match matchFunction = as(filter.condition(), Match.class);
        ReferenceAttribute clientIP = as(matchFunction.field(), ReferenceAttribute.class);
        assertEquals("client_ip", clientIP.name());
        Literal literal = as(matchFunction.query(), Literal.class);
        assertEquals(new BytesRef("127.0.0.1"), literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        // all fields from the two indices
        assertEquals(4, output.size());
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        EsRelation subqueryIndex = as(subqueryProject.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        Subquery subquery = as(subqueryProject.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        MatchOperator matchOperator = as(subqueryFilter.condition(), MatchOperator.class);
        FieldAttribute message = as(matchOperator.field(), FieldAttribute.class);
        assertEquals("message", message.name());
        literal = as(matchOperator.query(), Literal.class);
        assertEquals(new BytesRef("error"), literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
    }

    public void testPruneEmptySubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

        LogicalPlan plan = basic().addSampleData().addRemoteMissingIndex().query("""
            FROM test, (FROM remote:missingIndex | WHERE message:"error"), (FROM sample_data)
            | WHERE match(client_ip,"127.0.0.1")
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Match matchFunction = as(filter.condition(), Match.class);
        ReferenceAttribute clientIP = as(matchFunction.field(), ReferenceAttribute.class);
        assertEquals("client_ip", clientIP.name());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(15, output.size());
        // the subquery with remote:missingIndex is pruned, validate PruneEmptyUnionAllBranch
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
    }

    // no_fields_index has empty mapping, however there is entry in indexNameWithModes,originalIndices and concreteIndices
    public void testSubqueryInFromWithNoFieldsIndices() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

        LogicalPlan plan = basic().addNoFieldsIndex().query("""
            FROM
                no_fields_index,
                (FROM no_fields_index),
                (FROM no_fields_index)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(0, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        EsRelation subqueryIndex = as(subqueryProject.child(), EsRelation.class);
        assertEquals("no_fields_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        Subquery subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("no_fields_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("no_fields_index", subqueryIndex.indexPattern());
    }

    // empty_index has empty mapping,indexNameWithModes,originalIndices and concreteIndices
    public void testSubqueryInFromWithEmptyIndex() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

        LogicalPlan plan = basic().addEmptyIndex().query("""
            FROM
                empty_index,
                (FROM empty_index),
                (FROM empty_index)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(0, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        EsRelation subqueryIndex = as(subqueryProject.child(), EsRelation.class);
        assertEquals("empty_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        Subquery subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("empty_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("empty_index", subqueryIndex.indexPattern());
    }

    // no_fields_index has empty mapping, however there is entry in indexNameWithModes,originalIndices and concreteIndices
    // empty_index has empty mapping,indexNameWithModes,originalIndices and concreteIndices
    public void testSubqueryInFromWithNoFieldsAndEmptyIndex() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

        LogicalPlan plan = basic().addNoFieldsIndex().addEmptyIndex().query("""
            FROM
                (FROM no_fields_index),
                (FROM no_fields_index),
                (FROM empty_index)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(0, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        Subquery subquery = as(subqueryProject.child(), Subquery.class);
        EsRelation subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("no_fields_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("no_fields_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("empty_index", subqueryIndex.indexPattern());
    }

    public void testCountWithSubqueryWithNoFields() {
        assumeTrue("Prune no-fields in subquery", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_PRUNE_NO_FIELDS.isEnabled());
        for (String count : List.of("count()", "count(*)", "count(1)")) {
            String query = LoggerMessageFormat.format(null, """
                FROM (FROM no_fields_index), (FROM no_fields_index)
                | STATS {}
                """, count);
            var plan = basic().addNoFieldsIndex().query(query);

            Limit limit = as(plan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            UnionAll unionAll = as(aggregate.child(), UnionAll.class);
            assertEquals(0, unionAll.output().size());
            assertEquals(2, unionAll.children().size());

            for (int i = 0; i < 2; i++) {
                Project project = as(unionAll.children().get(i), Project.class);
                assertEquals(0, project.projections().size());
                Subquery subquery = as(project.child(), Subquery.class);
                EsRelation relation = as(subquery.child(), EsRelation.class);
                assertEquals("no_fields_index", relation.indexPattern());
            }
        }
    }

    public void testCountWithSubqueryWithEmptyIndex() {
        assumeTrue("Prune no-fields in subquery", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_PRUNE_NO_FIELDS.isEnabled());
        for (String count : List.of("count()", "count(*)", "count(1)")) {
            String query = LoggerMessageFormat.format(null, """
                FROM (FROM empty_index), (FROM empty_index)
                | STATS {}
                """, count);
            var plan = basic().addEmptyIndex().query(query);

            Limit limit = as(plan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            UnionAll unionAll = as(aggregate.child(), UnionAll.class);
            assertEquals(0, unionAll.output().size());
            assertEquals(2, unionAll.children().size());

            for (int i = 0; i < 2; i++) {
                Project project = as(unionAll.children().get(i), Project.class);
                assertEquals(0, project.projections().size());
                Subquery subquery = as(project.child(), Subquery.class);
                EsRelation relation = as(subquery.child(), EsRelation.class);
                assertEquals("empty_index", relation.indexPattern());
            }
        }
    }

    public void testCountWithSubqueryWithNoFieldsAndEmptyIndex() {
        assumeTrue("Prune no-fields in subquery", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_PRUNE_NO_FIELDS.isEnabled());
        for (String count : List.of("count()", "count(*)", "count(1)")) {
            String query = LoggerMessageFormat.format(null, """
                FROM (FROM no_fields_index), (FROM empty_index)
                | STATS {}
                """, count);
            var plan = basic().addEmptyIndex().addNoFieldsIndex().query(query);

            Limit limit = as(plan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            UnionAll unionAll = as(aggregate.child(), UnionAll.class);
            assertEquals(0, unionAll.output().size());
            assertEquals(2, unionAll.children().size());

            for (int i = 0; i < 2; i++) {
                Project project = as(unionAll.children().get(i), Project.class);
                assertEquals(0, project.projections().size());
                Subquery subquery = as(project.child(), Subquery.class);
                EsRelation relation = as(subquery.child(), EsRelation.class);
                assertEquals(i == 0 ? "no_fields_index" : "empty_index", relation.indexPattern());
            }
        }
    }

    // ---- Subqueries whose source command is TS ----

    /**
     * A TS subquery alongside a standard FROM index pattern. Mirrors {@link #testSubqueryInFrom} but with
     * a TS source instead of FROM in the subquery; the right-hand branch is wrapped in {@link Subquery}
     * over an {@link EsRelation} in {@link IndexMode#TIME_SERIES}, with counter fields demoted to their
     * base type before union.
     */
    public void testTSSubqueryInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().query("""
            FROM test, (TS k8s | WHERE @timestamp > "2025-10-07")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // 11 test fields + 21 k8s fields
        assertEquals(32, unionAll.output().size());
        assertEquals(2, unionAll.children().size());

        // Left leg (test): Project -> Eval[null evals for k8s fields] -> EsRelation[test]
        Project testProject = as(unionAll.children().get(0), Project.class);
        assertEquals(32, testProject.projections().size());
        Eval testNullEval = as(testProject.child(), Eval.class);
        // null evals for the 21 k8s fields missing in test
        assertEquals(21, testNullEval.fields().size());
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, testRelation.indexMode());

        // Right leg (TS k8s): Project -> Eval[counter demotions] -> Eval[null evals] -> Subquery -> Filter -> EsRelation[TIME_SERIES]
        Project tsProject = as(unionAll.children().get(1), Project.class);
        assertEquals(32, tsProject.projections().size());
        Eval counterDemotionEval = as(tsProject.child(), Eval.class);
        // network.total_bytes_in (counter_long -> long) and network.total_cost (counter_double -> double)
        assertEquals(2, counterDemotionEval.fields().size());
        Eval tsNullEval = as(counterDemotionEval.child(), Eval.class);
        // null evals for the 11 test fields missing in k8s
        assertEquals(11, tsNullEval.fields().size());
        Subquery subquery = as(tsNullEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        GreaterThan gt = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute ts = as(gt.left(), FieldAttribute.class);
        assertEquals("@timestamp", ts.name());
        EsRelation tsRelation = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());
    }

    /**
     * If the only branch of FROM is a TS subquery the {@link UnionAll}/{@link Subquery} wrappers are
     * collapsed and the TS {@link EsRelation} surfaces directly. Because the source is TS, an implicit
     * {@code SORT @timestamp DESC} is inserted above the filter — this is the time-series default and
     * is what distinguishes this case from {@link #testSubqueryInFromWithoutMainIndexPattern}.
     */
    public void testTSSubqueryInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzer().addK8sDownsampled().query("""
            FROM (TS k8s | WHERE @timestamp > "2025-10-07")
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        FieldAttribute orderField = as(order.get(0).child(), FieldAttribute.class);
        assertEquals("@timestamp", orderField.name());
        assertEquals(Order.OrderDirection.DESC, order.get(0).direction());

        Filter filter = as(orderBy.child(), Filter.class);
        GreaterThan gt = as(filter.condition(), GreaterThan.class);
        FieldAttribute ts = as(gt.left(), FieldAttribute.class);
        assertEquals("@timestamp", ts.name());
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, relation.indexMode());
    }

    /**
     * TS subquery with a time-series aggregation ({@code rate} inside an outer {@code MAX}) — verifies
     * the subquery is rewritten to a {@link TimeSeriesAggregate} node that retains the
     * {@link IndexMode#TIME_SERIES} relation underneath.
     */
    public void testTSSubqueryWithTimeSeriesAggregate() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().query("""
            FROM test, (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY cluster, pod)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // 11 test fields + 3 subquery fields (cluster, pod, m)
        assertEquals(14, unionAll.output().size());
        assertEquals(2, unionAll.children().size());

        // Left leg (test): Project -> Eval[3 null evals: cluster, pod, m] -> EsRelation[test]
        Project testProject = as(unionAll.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        assertEquals(3, testNullEval.fields().size());
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, testRelation.indexMode());

        // Right leg (TS k8s | STATS): Project -> Eval[11 null evals for test fields] -> Subquery -> TimeSeriesAggregate ->
        // EsRelation[TIME_SERIES]
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        assertEquals(11, tsNullEval.fields().size());
        Subquery subquery = as(tsNullEval.child(), Subquery.class);
        TimeSeriesAggregate tsAggregate = as(subquery.child(), TimeSeriesAggregate.class);
        // STATS BY cluster, pod
        assertEquals(2, tsAggregate.groupings().size());
        FieldAttribute clusterGrouping = as(tsAggregate.groupings().get(0), FieldAttribute.class);
        assertEquals("cluster", clusterGrouping.name());
        FieldAttribute podGrouping = as(tsAggregate.groupings().get(1), FieldAttribute.class);
        assertEquals("pod", podGrouping.name());
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());
    }

    /**
     * Three subqueries mixing TS and FROM. The {@link UnionAll} has three branches: a TS subquery with
     * a time-series aggregation, a FROM subquery with a regular {@link Aggregate}, and the standard
     * main index pattern as the first branch.
     */
    public void testMultipleSubqueriesInFromWithTS() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().addSampleData().query("""
            FROM test,
              (TS k8s | STATS rate = max(rate(network.total_bytes_in)) BY cluster),
              (FROM sample_data | STATS cnt = count(*))
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        // Branch 0: main FROM test
        Project testProject = as(unionAll.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, testRelation.indexMode());

        // Branch 1: TS k8s subquery with TimeSeriesAggregate
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        Subquery tsSubquery = as(tsNullEval.child(), Subquery.class);
        TimeSeriesAggregate tsAggregate = as(tsSubquery.child(), TimeSeriesAggregate.class);
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 2: FROM sample_data subquery with regular Aggregate
        Project sampleProject = as(unionAll.children().get(2), Project.class);
        Eval sampleNullEval = as(sampleProject.child(), Eval.class);
        Subquery sampleSubquery = as(sampleNullEval.child(), Subquery.class);
        Aggregate sampleAggregate = as(sampleSubquery.child(), Aggregate.class);
        assertFalse(sampleAggregate instanceof TimeSeriesAggregate);
        EsRelation sampleRelation = as(sampleAggregate.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /**
     * TS subquery with chained processing commands ({@code WHERE | EVAL | KEEP}) — verifies the subquery
     * preserves the per-command tree and that the {@link EsRelation} remains in
     * {@link IndexMode#TIME_SERIES}. Mirrors the pattern of FROM subqueries with processing commands.
     */
    public void testTSSubqueryWithProcessingCommands() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().query("""
            FROM test, (TS k8s
                        | WHERE @timestamp > "2025-10-07"
                        | EVAL doubled = network.cost * 2
                        | KEEP cluster, pod, doubled)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // 11 test fields + 3 subquery fields (cluster, pod, doubled)
        assertEquals(14, unionAll.output().size());
        assertEquals(2, unionAll.children().size());

        // Left leg (test)
        Project testProject = as(unionAll.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        assertEquals(3, testNullEval.fields().size());
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());

        // Right leg (TS k8s | WHERE | EVAL | KEEP)
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        assertEquals(11, tsNullEval.fields().size());
        Subquery subquery = as(tsNullEval.child(), Subquery.class);
        Project keepProject = as(subquery.child(), Project.class);
        // KEEP projects 3 fields
        assertEquals(3, keepProject.projections().size());
        Eval evalNode = as(keepProject.child(), Eval.class);
        assertEquals(1, evalNode.fields().size());
        Alias doubledAlias = evalNode.fields().get(0);
        assertEquals("doubled", doubledAlias.name());
        Filter subqueryFilter = as(evalNode.child(), Filter.class);
        EsRelation tsRelation = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());
    }

    /**
     * TS subquery nested inside an outer FROM subquery — verifies that a {@code Subquery} can host its
     * own {@code UnionAll}, and that a TS subquery sitting inside still resolves to a
     * {@link IndexMode#TIME_SERIES} {@link EsRelation} at the leaf.
     */
    public void testNestedTSSubqueryInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().addSampleData().query("""
            FROM test, (FROM sample_data, (TS k8s | WHERE @timestamp > "2025-10-07"))
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll outerUnion = as(limit.child(), UnionAll.class);
        assertEquals(2, outerUnion.children().size());

        // Left leg: test
        Project testProject = as(outerUnion.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());

        // Right leg: outer FROM subquery wrapping (sample_data union TS k8s)
        // Because the inner UnionAll surfaces counter-typed fields, the outer right leg adds a
        // counter-rename Eval on top of the null-eval before the Subquery wrapper.
        Project rightProject = as(outerUnion.children().get(1), Project.class);
        Eval rightCounterRename = as(rightProject.child(), Eval.class);
        assertEquals(2, rightCounterRename.fields().size());
        Eval rightNullEval = as(rightCounterRename.child(), Eval.class);
        // 11 null evals for the missing test fields
        assertEquals(11, rightNullEval.fields().size());
        Subquery outerSubquery = as(rightNullEval.child(), Subquery.class);
        UnionAll innerUnion = as(outerSubquery.child(), UnionAll.class);
        assertEquals(2, innerUnion.children().size());

        // Inner branch 0: sample_data — needs counter demotions for fields surfaced by the TS branch
        Project sampleProject = as(innerUnion.children().get(0), Project.class);
        Eval sampleCounterEval = as(sampleProject.child(), Eval.class);
        // TOLONG/TODOUBLE for network.total_bytes_in, network.total_cost
        assertEquals(2, sampleCounterEval.fields().size());
        Eval sampleNullEval = as(sampleCounterEval.child(), Eval.class);
        EsRelation sampleRelation = as(sampleNullEval.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());

        // Inner branch 1: TS k8s subquery — two stacked counter demotions (outer rename + inner cast),
        // followed by null evals for sample_data fields, then the Subquery wrapping the TS relation.
        Project tsProject = as(innerUnion.children().get(1), Project.class);
        Eval tsOuterRename = as(tsProject.child(), Eval.class);
        assertEquals(2, tsOuterRename.fields().size());
        Eval tsInnerCast = as(tsOuterRename.child(), Eval.class);
        assertEquals(2, tsInnerCast.fields().size());
        Eval tsNullEval = as(tsInnerCast.child(), Eval.class);
        Subquery innerTSSubquery = as(tsNullEval.child(), Subquery.class);
        Filter tsFilter = as(innerTSSubquery.child(), Filter.class);
        EsRelation tsRelation = as(tsFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());
    }

    /**
     * TS subquery alongside another FROM subquery in a {@code FROM} that has no main index pattern.
     * The {@link UnionAll} has two {@link Subquery}-wrapped branches and no standalone index pattern
     * leg. Counter-typed fields surfaced by the TS branch are demoted to their base type before union.
     */
    public void testTSAndFromSubqueriesInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | WHERE @timestamp > "2025-10-07"), (FROM sample_data)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 0: TS k8s subquery
        Project tsProject = as(unionAll.children().get(0), Project.class);
        Eval tsCounterDemotionEval = as(tsProject.child(), Eval.class);
        // counter_long and counter_double fields demoted to long/double
        assertEquals(2, tsCounterDemotionEval.fields().size());
        Eval tsNullEval = as(tsCounterDemotionEval.child(), Eval.class);
        Subquery tsSubquery = as(tsNullEval.child(), Subquery.class);
        Filter tsFilter = as(tsSubquery.child(), Filter.class);
        EsRelation tsRelation = as(tsFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 1: FROM sample_data subquery
        Project sampleProject = as(unionAll.children().get(1), Project.class);
        Eval sampleNullEval = as(sampleProject.child(), Eval.class);
        Subquery sampleSubquery = as(sampleNullEval.child(), Subquery.class);
        EsRelation sampleRelation = as(sampleSubquery.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /**
     * Subquery using TS with a BY WITHOUT(...) grouping, combined with a FROM subquery.
     * Validates that the WITHOUT(...) grouping surfaces as an attribute in the UnionAll output
     * and is propagated as a null reference into the non-TS branch.
     */
    public void testTSSubqueryWithByWithoutAndFromSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires ESQL_WITHOUT_GROUPING", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());

        LogicalPlan plan = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY WITHOUT(pod)),
              (FROM sample_data)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // The UnionAll output combines the TS aggregate columns (m, WITHOUT(pod)) with the
        // sample_data fields (@timestamp, client_ip, event_duration, message).
        List<? extends NamedExpression> unionOutput = unionAll.output();
        assertEquals(6, unionOutput.size());
        Attribute mAttr = (Attribute) unionOutput.get(0);
        assertEquals("m", mAttr.name());
        assertThat(mAttr, instanceOf(ReferenceAttribute.class));
        Attribute withoutAttr = (Attribute) unionOutput.get(1);
        assertEquals("WITHOUT(pod)", withoutAttr.name());
        assertThat(withoutAttr, instanceOf(ReferenceAttribute.class));

        // Branch 0: TS subquery with BY WITHOUT(pod)
        Project tsProject = as(unionAll.children().get(0), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        assertEquals(4, tsNullEval.fields().size());
        // Each null eval corresponds to a missing sample_data field.
        for (Alias nullAlias : tsNullEval.fields()) {
            assertThat(nullAlias.child(), instanceOf(Literal.class));
        }
        Subquery tsSubquery = as(tsNullEval.child(), Subquery.class);
        TimeSeriesAggregate tsAggregate = as(tsSubquery.child(), TimeSeriesAggregate.class);

        // The WITHOUT(pod) grouping survives the analyzer as an Alias wrapping a TimeSeriesWithout.
        assertEquals(1, tsAggregate.groupings().size());
        Alias withoutGrouping = as(tsAggregate.groupings().get(0), Alias.class);
        assertEquals("WITHOUT(pod)", withoutGrouping.name());
        TimeSeriesWithout tsWithout = as(withoutGrouping.child(), TimeSeriesWithout.class);
        // The grouping references the excluded dimension.
        FieldAttribute excluded = as(tsWithout.children().get(0), FieldAttribute.class);
        assertEquals("pod", excluded.name());

        // The aggregate output exposes [m, WITHOUT(pod)].
        assertEquals(2, tsAggregate.aggregates().size());
        Alias mAlias = as(tsAggregate.aggregates().get(0), Alias.class);
        assertEquals("m", mAlias.name());
        // The second aggregate is the grouping reference WITHOUT(pod).
        ReferenceAttribute withoutRef = as(tsAggregate.aggregates().get(1), ReferenceAttribute.class);
        assertEquals("WITHOUT(pod)", withoutRef.name());
        // It points back at the Alias of the grouping.
        assertEquals(withoutGrouping.toAttribute().id(), withoutRef.id());

        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 1: FROM sample_data — produces null Evals for m (DOUBLE) and WITHOUT(pod) (KEYWORD).
        Project sampleProject = as(unionAll.children().get(1), Project.class);
        Eval sampleNullEval = as(sampleProject.child(), Eval.class);
        assertEquals(2, sampleNullEval.fields().size());
        Alias sampleMNull = sampleNullEval.fields().get(0);
        assertEquals("m", sampleMNull.name());
        Alias sampleWithoutNull = sampleNullEval.fields().get(1);
        assertEquals("WITHOUT(pod)", sampleWithoutNull.name());
        Subquery sampleSubquery = as(sampleNullEval.child(), Subquery.class);
        EsRelation sampleRelation = as(sampleSubquery.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /**
     * Combines a main FROM index, a TS subquery using BY WITHOUT(...), and another FROM subquery.
     * Validates the UnionAll surfaces the WITHOUT(...) attribute across all three branches and that
     * the non-TS branches receive a null Eval for the WITHOUT(...) reference.
     */
    public void testTSSubqueryWithByWithoutInFromCommand() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires ESQL_WITHOUT_GROUPING", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());

        LogicalPlan plan = basic().addK8sDownsampled().addSampleData().query("""
            FROM test,
              (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY WITHOUT(pod)),
              (FROM sample_data)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        // The UnionAll output spans all branches: 11 test fields + m + WITHOUT(pod) + 4 sample_data fields.
        List<? extends NamedExpression> unionOutput = unionAll.output();
        assertEquals(17, unionOutput.size());
        // Locate the WITHOUT(pod) attribute in the union output.
        Attribute unionWithout = (Attribute) unionOutput.stream()
            .filter(a -> a.name().equals("WITHOUT(pod)"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("UnionAll output must contain WITHOUT(pod) attribute"));
        assertThat(unionWithout, instanceOf(ReferenceAttribute.class));
        Attribute unionM = (Attribute) unionOutput.stream()
            .filter(a -> a.name().equals("m"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("UnionAll output must contain m attribute"));
        assertThat(unionM, instanceOf(ReferenceAttribute.class));

        // Branch 0: main FROM test — null Evals for the missing aggregate + sample_data columns.
        Project testProject = as(unionAll.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        // 6 nulls: m, WITHOUT(pod), @timestamp, client_ip, event_duration, message
        assertEquals(6, testNullEval.fields().size());
        assertTrue(
            "test branch must materialize WITHOUT(pod) as a null reference",
            testNullEval.fields().stream().anyMatch(a -> a.name().equals("WITHOUT(pod)"))
        );
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());

        // Branch 1: TS subquery with BY WITHOUT(pod).
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        // null evals for the 11 test fields + 4 sample_data fields = 15.
        assertEquals(15, tsNullEval.fields().size());
        // Verify the TS branch itself does not emit a null for WITHOUT(pod) — it owns that column.
        assertTrue(
            "TS branch must not null-eval WITHOUT(pod)",
            tsNullEval.fields().stream().noneMatch(a -> a.name().equals("WITHOUT(pod)"))
        );
        Subquery tsSubquery = as(tsNullEval.child(), Subquery.class);
        TimeSeriesAggregate tsAggregate = as(tsSubquery.child(), TimeSeriesAggregate.class);
        assertEquals(1, tsAggregate.groupings().size());
        Alias withoutGrouping = as(tsAggregate.groupings().get(0), Alias.class);
        assertEquals("WITHOUT(pod)", withoutGrouping.name());
        as(withoutGrouping.child(), TimeSeriesWithout.class);
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 2: FROM sample_data — null Evals for the 11 test fields + m + WITHOUT(pod) = 13.
        Project sampleProject = as(unionAll.children().get(2), Project.class);
        Eval sampleNullEval = as(sampleProject.child(), Eval.class);
        assertEquals(13, sampleNullEval.fields().size());
        assertTrue(
            "sample_data branch must materialize WITHOUT(pod) as a null reference",
            sampleNullEval.fields().stream().anyMatch(a -> a.name().equals("WITHOUT(pod)"))
        );
        Subquery sampleSubquery = as(sampleNullEval.child(), Subquery.class);
        EsRelation sampleRelation = as(sampleSubquery.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /**
     * TS subquery aggregates produce {@code m} as LONG while a sibling FROM subquery emits {@code m} as KEYWORD.
     * Without an explicit cast, the analyzer marks the column as {@link UnsupportedAttribute} in the
     * UnionAll output and rewrites both branches' {@code m} reference to a null KEYWORD eval.
     */
    public void testTSSubqueryWithConflictingTypesInUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());

        LogicalPlan plan = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY cluster),
              (FROM sample_data | EVAL m = "abc")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        List<Attribute> output = unionAll.output();
        assertEquals(6, output.size());
        UnsupportedAttribute mUnsupported = as(output.get(0), UnsupportedAttribute.class);
        assertEquals("m", mUnsupported.name());
        assertEquals(UNSUPPORTED, mUnsupported.dataType());
        assertThat(mUnsupported.originalTypes(), hasSize(2));
        assertThat(mUnsupported.originalTypes(), is(List.of(DOUBLE.esType(), KEYWORD.esType())));

        // Branch 0: TS leg
        Project tsProject = as(unionAll.children().get(0), Project.class);
        Eval tsNullM = as(tsProject.child(), Eval.class);
        assertEquals(1, tsNullM.fields().size());
        Alias tsNullMAlias = tsNullM.fields().get(0);
        assertEquals("m", tsNullMAlias.name());
        Literal tsNullMLit = as(tsNullMAlias.child(), Literal.class);
        assertNull(tsNullMLit.value());
        assertEquals(KEYWORD, tsNullMLit.dataType());

        Eval tsNullSampleFields = as(tsNullM.child(), Eval.class);
        assertEquals(4, tsNullSampleFields.fields().size());
        Subquery tsSubquery = as(tsNullSampleFields.child(), Subquery.class);
        TimeSeriesAggregate tsAggregate = as(tsSubquery.child(), TimeSeriesAggregate.class);
        assertEquals(1, tsAggregate.groupings().size());
        FieldAttribute clusterGrouping = as(tsAggregate.groupings().get(0), FieldAttribute.class);
        assertEquals("cluster", clusterGrouping.name());
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 1: FROM sample_data
        Project sampleProject = as(unionAll.children().get(1), Project.class);
        Eval sampleNullM = as(sampleProject.child(), Eval.class);
        assertEquals(1, sampleNullM.fields().size());
        Alias sampleNullMAlias = sampleNullM.fields().get(0);
        assertEquals("m", sampleNullMAlias.name());
        Literal sampleNullMLit = as(sampleNullMAlias.child(), Literal.class);
        assertNull(sampleNullMLit.value());
        assertEquals(KEYWORD, sampleNullMLit.dataType());

        Eval sampleNullCluster = as(sampleNullM.child(), Eval.class);
        assertEquals(1, sampleNullCluster.fields().size());
        assertEquals("cluster", sampleNullCluster.fields().get(0).name());
        Subquery sampleSubquery = as(sampleNullCluster.child(), Subquery.class);
        Eval innerEval = as(sampleSubquery.child(), Eval.class);
        assertEquals(1, innerEval.fields().size());
        Alias innerMAlias = innerEval.fields().get(0);
        assertEquals("m", innerMAlias.name());
        Literal innerMLit = as(innerMAlias.child(), Literal.class);
        assertEquals(KEYWORD, innerMLit.dataType());
        EsRelation sampleRelation = as(innerEval.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /**
     * Same shape as {@link #testTSSubqueryWithConflictingTypesInUnionAll} but the conflicting {@code m}
     * is reconciled by an explicit cast {@code EVAL m = m::string}. The cast pushes a {@code TOSTRING}
     * eval into each UnionAll branch and exposes a {@code $$m$converted_to$keyword} reference at the top.
     */
    public void testTSSubqueryWithConflictingTypesAndExplicitCast() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());

        LogicalPlan plan = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY cluster),
              (FROM sample_data | EVAL m = "abc")
            | EVAL m = m::string
            | KEEP m
            """);

        Limit limit = as(plan, Limit.class);
        Project topProject = as(limit.child(), Project.class);
        assertEquals(1, topProject.projections().size());
        assertEquals("m", topProject.projections().get(0).name());

        Eval topEval = as(topProject.child(), Eval.class);
        assertEquals(1, topEval.fields().size());
        Alias mFromCast = topEval.fields().get(0);
        assertEquals("m", mFromCast.name());
        ReferenceAttribute castRef = as(mFromCast.child(), ReferenceAttribute.class);
        assertEquals("$$m$converted_to$keyword", castRef.name());
        assertEquals(KEYWORD, castRef.dataType());

        UnionAll unionAll = as(topEval.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        List<Attribute> output = unionAll.output();
        assertEquals(7, output.size());
        UnsupportedAttribute mUnsupported = as(output.get(0), UnsupportedAttribute.class);
        assertEquals("m", mUnsupported.name());
        assertEquals(UNSUPPORTED, mUnsupported.dataType());
        ReferenceAttribute castedAttr = as(output.get(1), ReferenceAttribute.class);
        assertEquals("$$m$converted_to$keyword", castedAttr.name());
        assertEquals(KEYWORD, castedAttr.dataType());

        // Branch 0: TS leg — TOSTRING(m) eval is inserted above the null sample-data evals.
        Project tsProject = as(unionAll.children().get(0), Project.class);
        Eval tsNullM = as(tsProject.child(), Eval.class);
        assertEquals(1, tsNullM.fields().size());
        assertEquals("m", tsNullM.fields().get(0).name());
        Eval tsCastEval = as(tsNullM.child(), Eval.class);
        assertEquals(1, tsCastEval.fields().size());
        Alias tsCastAlias = tsCastEval.fields().get(0);
        assertEquals("$$m$converted_to$keyword", tsCastAlias.name());
        Eval tsNullSampleFields = as(tsCastEval.child(), Eval.class);
        assertEquals(4, tsNullSampleFields.fields().size());
        Subquery tsSubquery = as(tsNullSampleFields.child(), Subquery.class);
        TimeSeriesAggregate tsAggregate = as(tsSubquery.child(), TimeSeriesAggregate.class);
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 1: FROM sample_data — TOSTRING(m) is inserted above the cluster null and inner literal eval.
        Project sampleProject = as(unionAll.children().get(1), Project.class);
        Eval sampleNullM = as(sampleProject.child(), Eval.class);
        assertEquals("m", sampleNullM.fields().get(0).name());
        Eval sampleCastEval = as(sampleNullM.child(), Eval.class);
        assertEquals(1, sampleCastEval.fields().size());
        assertEquals("$$m$converted_to$keyword", sampleCastEval.fields().get(0).name());
        Eval sampleNullCluster = as(sampleCastEval.child(), Eval.class);
        assertEquals("cluster", sampleNullCluster.fields().get(0).name());
        Subquery sampleSubquery = as(sampleNullCluster.child(), Subquery.class);
        Eval innerEval = as(sampleSubquery.child(), Eval.class);
        Alias innerMAlias = innerEval.fields().get(0);
        assertEquals("m", innerMAlias.name());
        assertEquals(KEYWORD, as(innerMAlias.child(), Literal.class).dataType());
        EsRelation sampleRelation = as(innerEval.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /**
     * TS aggregate produces {@code m} as LONG while the sibling FROM subquery emits {@code m} as DOUBLE.
     * Even though both types are numeric, the analyzer does not implicitly promote across UnionAll
     * branches, so {@code m} surfaces as {@link UnsupportedAttribute} (LONG vs DOUBLE). An explicit
     * cast resolves the conflict.
     */
    public void testTSSubqueryWithNumericConflictAndExplicitCast() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());

        // (1) Without explicit cast: LONG vs DOUBLE → UNSUPPORTED.
        LogicalPlan planNoCast = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = sum(last_over_time(network.bytes_in))),
              (FROM sample_data | EVAL m = 1.5)
            """);

        Limit noCastLimit = as(planNoCast, Limit.class);
        UnionAll noCastUnion = as(noCastLimit.child(), UnionAll.class);
        assertEquals(2, noCastUnion.children().size());
        List<Attribute> noCastOutput = noCastUnion.output();
        assertEquals(5, noCastOutput.size());
        UnsupportedAttribute mNoCast = as(noCastOutput.get(0), UnsupportedAttribute.class);
        assertEquals("m", mNoCast.name());
        assertEquals(UNSUPPORTED, mNoCast.dataType());
        assertThat(mNoCast.originalTypes(), is(List.of(LONG.esType(), DOUBLE.esType())));

        Project tsProject = as(noCastUnion.children().get(0), Project.class);
        Eval tsNullM = as(tsProject.child(), Eval.class);
        assertEquals("m", tsNullM.fields().get(0).name());
        assertEquals(KEYWORD, as(tsNullM.fields().get(0).child(), Literal.class).dataType());
        Eval tsNullSampleFields = as(tsNullM.child(), Eval.class);
        assertEquals(4, tsNullSampleFields.fields().size());
        Subquery tsSubquery = as(tsNullSampleFields.child(), Subquery.class);
        TimeSeriesAggregate tsAggregate = as(tsSubquery.child(), TimeSeriesAggregate.class);
        assertTrue(tsAggregate.groupings().isEmpty());
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        Project sampleProject = as(noCastUnion.children().get(1), Project.class);
        Eval sampleNullM = as(sampleProject.child(), Eval.class);
        assertEquals("m", sampleNullM.fields().get(0).name());
        Subquery sampleSubquery = as(sampleNullM.child(), Subquery.class);
        Eval innerEval = as(sampleSubquery.child(), Eval.class);
        Literal oneAndAHalf = as(innerEval.fields().get(0).child(), Literal.class);
        assertEquals(DOUBLE, oneAndAHalf.dataType());
        assertEquals(1.5, oneAndAHalf.value());
        EsRelation sampleRelation = as(innerEval.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());

        // (2) With explicit cast m::double: TODOUBLE is pushed into each branch.
        LogicalPlan planCast = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = sum(last_over_time(network.bytes_in))),
              (FROM sample_data | EVAL m = 1.5)
            | EVAL m = m::double
            | KEEP m
            """);

        Limit castLimit = as(planCast, Limit.class);
        Project castTopProject = as(castLimit.child(), Project.class);
        assertEquals(1, castTopProject.projections().size());
        Eval castTopEval = as(castTopProject.child(), Eval.class);
        Alias mFromCast = castTopEval.fields().get(0);
        assertEquals("m", mFromCast.name());
        ReferenceAttribute castRef = as(mFromCast.child(), ReferenceAttribute.class);
        assertEquals("$$m$converted_to$double", castRef.name());
        assertEquals(DOUBLE, castRef.dataType());

        UnionAll castUnion = as(castTopEval.child(), UnionAll.class);
        assertEquals(2, castUnion.children().size());
        ReferenceAttribute castedOutput = as(castUnion.output().get(1), ReferenceAttribute.class);
        assertEquals("$$m$converted_to$double", castedOutput.name());
        assertEquals(DOUBLE, castedOutput.dataType());

        Project castTsProject = as(castUnion.children().get(0), Project.class);
        Eval castTsNullM = as(castTsProject.child(), Eval.class);
        assertEquals("m", castTsNullM.fields().get(0).name());
        Eval castTsConvertEval = as(castTsNullM.child(), Eval.class);
        assertEquals("$$m$converted_to$double", castTsConvertEval.fields().get(0).name());
        Eval castTsNullSamples = as(castTsConvertEval.child(), Eval.class);
        assertEquals(4, castTsNullSamples.fields().size());
        Subquery castTsSubquery = as(castTsNullSamples.child(), Subquery.class);
        TimeSeriesAggregate castTsAggregate = as(castTsSubquery.child(), TimeSeriesAggregate.class);
        EsRelation castTsRelation = as(castTsAggregate.child(), EsRelation.class);
        assertEquals(IndexMode.TIME_SERIES, castTsRelation.indexMode());

        Project castSampleProject = as(castUnion.children().get(1), Project.class);
        Eval castSampleNullM = as(castSampleProject.child(), Eval.class);
        assertEquals("m", castSampleNullM.fields().get(0).name());
        Eval castSampleConvertEval = as(castSampleNullM.child(), Eval.class);
        assertEquals("$$m$converted_to$double", castSampleConvertEval.fields().get(0).name());
        Subquery castSampleSubquery = as(castSampleConvertEval.child(), Subquery.class);
        Eval castSampleInnerEval = as(castSampleSubquery.child(), Eval.class);
        assertEquals(DOUBLE, as(castSampleInnerEval.fields().get(0).child(), Literal.class).dataType());
        EsRelation castSampleRelation = as(castSampleInnerEval.child(), EsRelation.class);
        assertEquals("sample_data", castSampleRelation.indexPattern());
    }

    private static TestAnalyzer basic() {
        return analyzer().addEmployees("test").stripErrorPrefix(true);
    }

    private static TestAnalyzer k8s() {
        return analyzer().addK8sDownsampled();
    }

    private static TestAnalyzer sampleData() {
        return analyzer().addSampleData();
    }

    private static TestAnalyzer defaultMapping() {
        return analyzer().addDefaultIndex();
    }
}
