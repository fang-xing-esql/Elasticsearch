/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.GenerativeFeature;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.EvalGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.InlineStatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.StatsGenerator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;

/** Tests generation of IN subqueries in every supported boolean-expression position. */
public class BooleanExpressionGeneratorTests extends ESTestCase {

    private static final List<Column> COLUMNS = List.of(
        new Column("number", "integer", List.of("integer")),
        new Column("word", "keyword", List.of("keyword"))
    );
    private static final List<Column> INNER_COLUMNS = List.of(new Column("number", "integer", List.of("integer")));
    private static final CommandGenerator.QuerySchema QUERY_SCHEMA = new CommandGenerator.QuerySchema(
        List.of("test-index"),
        List.of(),
        List.of(),
        Set.of()
    );
    private static final QueryExecutor EXECUTOR = (query, depth) -> new QueryExecuted(query, depth, INNER_COLUMNS, List.of(), null);

    /** Every allowlisted wrapper should retain the generated predicate in the intended argument position. */
    public void testWrapInSubqueryExpression() {
        String expression = "number IN (FROM test-index | KEEP number)";
        Map<BooleanExpressionGenerator.InSubqueryVariant, String> expected = Map.of(
            BooleanExpressionGenerator.InSubqueryVariant.BARE,
            expression,
            BooleanExpressionGenerator.InSubqueryVariant.CASE_CONDITION,
            "case(" + expression + ", true, false)",
            BooleanExpressionGenerator.InSubqueryVariant.CASE_TRUE_VALUE,
            "case(true, " + expression + ", false)",
            BooleanExpressionGenerator.InSubqueryVariant.CASE_ELSE_VALUE,
            "case(false, true, " + expression + ")",
            BooleanExpressionGenerator.InSubqueryVariant.COALESCE,
            "coalesce(" + expression + ", false)",
            BooleanExpressionGenerator.InSubqueryVariant.IS_NULL,
            "(" + expression + ") IS NULL",
            BooleanExpressionGenerator.InSubqueryVariant.IS_NOT_NULL,
            "(" + expression + ") IS NOT NULL"
        );

        for (var entry : expected.entrySet()) {
            assertEquals(entry.getValue(), BooleanExpressionGenerator.wrapInSubqueryExpression(expression, entry.getKey()));
        }
    }

    /** The shared helper should build a compatible subquery and record successful generation in the context. */
    public void testMaybeInSubqueryBooleanExpression() {
        GenerationContext context = inSubqueryContext();
        String expression = EsqlQueryGenerator.maybeInSubqueryBooleanExpression(COLUMNS, QUERY_SCHEMA, EXECUTOR, context);

        assertNotNull(expression);
        assertInSubquery(expression);
        assertTrue(context.hasGeneratedInSubquery());
    }

    /** Reaching the nesting limit must suppress further subquery generation without changing the shared flag. */
    public void testMaybeInSubqueryBooleanExpressionRespectsNestingLimit() {
        GenerationContext context = inSubqueryContext().withSubqueryDepth(GenerationContext.MAX_IN_SUBQUERY_NESTING_DEPTH);

        assertNull(EsqlQueryGenerator.maybeInSubqueryBooleanExpression(COLUMNS, QUERY_SCHEMA, EXECUTOR, context));
        assertFalse(context.hasGeneratedInSubquery());
    }

    /** EVAL should be able to assign a generated IN-subquery boolean expression. */
    public void testEvalGeneratesInSubquery() {
        CommandGenerator.CommandDescription description = EvalGenerator.INSTANCE.generate(
            List.of(),
            COLUMNS,
            QUERY_SCHEMA,
            EXECUTOR,
            inSubqueryContext()
        );

        assertInSubquery(description.commandString());
    }

    /** STATS should attach a generated IN subquery to an aggregate's WHERE filter. */
    public void testStatsGeneratesInSubqueryInAggregateWhere() {
        assertAggregateWhereInSubquery(StatsGenerator.INSTANCE, " | stats ");
    }

    /** INLINE STATS shares the filtered-aggregate generator while retaining its command name. */
    public void testInlineStatsGeneratesInSubqueryInAggregateWhere() {
        assertAggregateWhereInSubquery(InlineStatsGenerator.INSTANCE, " | inline stats ");
    }

    private static GenerationContext inSubqueryContext() {
        return GenerationContext.root(Set.of(GenerativeFeature.IN_SUBQUERY));
    }

    private static void assertAggregateWhereInSubquery(CommandGenerator generator, String commandPrefix) {
        CommandGenerator.CommandDescription description = generator.generate(
            List.of(),
            COLUMNS,
            QUERY_SCHEMA,
            EXECUTOR,
            inSubqueryContext()
        );

        assertTrue(description.commandString(), description.commandString().startsWith(commandPrefix));
        assertThat(description.commandString(), containsString(" WHERE "));
        assertInSubquery(description.commandString());
    }

    private static void assertInSubquery(String expression) {
        assertThat(expression, anyOf(containsString(" IN ("), containsString(" NOT IN (")));
    }
}
