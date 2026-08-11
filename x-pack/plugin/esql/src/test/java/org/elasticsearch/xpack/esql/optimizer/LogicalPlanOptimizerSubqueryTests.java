/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;

/**
 * Negative tests for subqueries in the {@code FROM} command; the positive tests live in
 * {@code LogicalPlanOptimizerSubqueryGoldenTests}.
 * <p>
 * These cover the query-wide branch limit ({@code max_query_branches}). Nesting is otherwise unbounded: each {@code FROM} is
 * limited to {@link org.elasticsearch.xpack.esql.plan.logical.Fork#MAX_BRANCHES} branches, but subqueries nest, so the total number
 * of branches - each one a coordinator merge segment or a data node query - grows as a power of the nesting depth.
 */
public class LogicalPlanOptimizerSubqueryTests extends AbstractLogicalPlanOptimizerTests {

    @Before
    public void checkNestedSubquerySupport() {
        assumeTrue("Requires nested subquery in FROM support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    public void testRejectsQueryExceedingMaxBranches() {
        int limit = QueryPragmas.EMPTY.maxQueryBranches();
        var e = expectThrows(VerificationException.class, () -> planSubquery(nestedSubqueries(limit / 2 + 1)));
        assertThat(e.getMessage(), containsString("query resolved to " + (limit + 2) + " branches in total, exceeding the current limit"));
        assertThat(e.getMessage(), containsString("Reduce the number of subqueries, or split this into multiple queries."));
    }

    public void testAcceptsQueryAtMaxBranches() {
        assertNotNull(planSubquery(nestedSubqueries(QueryPragmas.EMPTY.maxQueryBranches() / 2)));
    }

    /**
     * The limit is a query pragma, so lowering it must reject a query that the default accepts.
     */
    public void testMaxBranchesHonorsPragma() {
        String query = nestedSubqueries(3); // 6 branches: well under the default
        assertNotNull(planSubquery(query));

        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 4).build());
        var optimizer = new LogicalPlanOptimizer(
            new LogicalOptimizerContext(
                EsqlTestUtils.configuration(pragmas),
                logicalOptimizerCtx.foldCtx(),
                logicalOptimizerCtx.minimumVersion()
            )
        );
        var e = expectThrows(VerificationException.class, () -> optimizer.optimize(subqueryAnalyzer().query(query)));
        assertThat(e.getMessage(), containsString("query resolved to 6 branches in total, exceeding the current limit of 4"));
    }

    /**
     * Builds {@code levels} nested unions, each with two branches - the {@code test} index plus the next level down - for a total of
     * {@code 2 * levels} branches. Two branches per level keeps every union above the single-branch shape that
     * {@code FlattenNestedSubqueries} would collapse, so the count survives optimization.
     */
    private static String nestedSubqueries(int levels) {
        StringBuilder query = new StringBuilder("FROM test");
        for (int i = 0; i < levels; i++) {
            query.insert(0, "FROM test, (").append(")");
        }
        return query.toString();
    }
}
