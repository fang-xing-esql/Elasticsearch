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
 * These cover the query-wide branch limit ({@code max_query_branches}) and unbounded sorts that stay rejected inside a union branch.
 * On the branch limit: nesting is otherwise unbounded: each {@code FROM} is limited to
 * {@link org.elasticsearch.xpack.esql.plan.logical.Fork#MAX_BRANCHES} branches, but subqueries nest, so the total number of branches -
 * each one a coordinator merge segment or a data node query - grows as a power of the nesting depth.
 */
public class LogicalPlanOptimizerSubqueryTests extends AbstractLogicalPlanOptimizerTests {

    @Before
    public void checkNestedSubquerySupport() {
        assumeTrue("Requires nested subquery in FROM support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    /**
     * An unbounded {@code SORT} inside a {@code WHERE ... IN (subquery)} is rejected, even when that {@code IN} sits in one branch of a
     * {@code UnionAll}. Two failures are reported: the generic unbounded-sort check, and the IN-subquery-specific one.
     * <p>
     * Worth pinning because {@code PushDownFilterAndLimitIntoUnionAll} relies on it. That rule searches each union branch for an
     * unbounded sort to bound, and its traversal descends into <em>both</em> sides of the {@code SemiJoin}/{@code AntiJoin}/
     * {@code MarkJoin} that an {@code IN} subquery becomes - {@code children()} is {@code [left, right]}. The right side is an
     * independently executed subquery, so a {@code Limit} placed at the branch root would not bound it while still truncating the left
     * stream. That never happens only because this verification makes an unbounded sort there unreachable; a bounded one has its
     * {@code Limit} as parent, which the search stops at first. If this rejection is ever relaxed - the "not supported yet" wording
     * suggests it might be - that rule needs revisiting.
     */
    public void testUnboundedSortInsideInSubqueryInUnionAllBranchIsRejected() {
        var e = expectThrows(VerificationException.class, () -> planSubquery("""
            FROM (FROM test | WHERE emp_no IN (FROM test | SORT emp_no | KEEP emp_no)), (FROM languages)
            | STATS c = COUNT(*)
            """));
        assertThat(e.getMessage(), containsString("Unbounded SORT not supported yet [SORT emp_no] please add a LIMIT"));
        assertThat(
            e.getMessage(),
            containsString(
                "cannot yet have an unbounded SORT [SORT emp_no] before it: either move the SORT after it, or add a LIMIT after the SORT"
            )
        );
    }

    /**
     * As {@link #testUnboundedSortInsideInSubqueryInUnionAllBranchIsRejected}, but the offending {@code IN} subquery sits two union
     * levels down - in a branch of the inner {@code UnionAll}, which is itself a branch of the outer one.
     * <p>
     * Both failures still surface, with the same wording and pointing at the same offsets, so the diagnostic does not degrade with
     * nesting depth. Worth its own case because several other subquery checks are depth-sensitive, and because the rule this
     * verification protects ({@code PushDownFilterAndLimitIntoUnionAll}) treats a nested {@code UnionAll} as a traversal boundary -
     * a reader could reasonably expect the boundary to stop the check from firing too. It does not.
     */
    public void testUnboundedSortInsideInSubqueryInNestedUnionAllBranchIsRejected() {
        var e = expectThrows(VerificationException.class, () -> planSubquery("""
            FROM (FROM (FROM test | WHERE emp_no IN (FROM test | SORT emp_no | KEEP emp_no)), (FROM test)), (FROM languages)
            | STATS c = COUNT(*)
            """));
        assertThat(e.getMessage(), containsString("Unbounded SORT not supported yet [SORT emp_no] please add a LIMIT"));
        assertThat(
            e.getMessage(),
            containsString(
                "cannot yet have an unbounded SORT [SORT emp_no] before it: either move the SORT after it, or add a LIMIT after the SORT"
            )
        );
    }

    public void testRejectsQueryExceedingMaxBranches() {
        int limit = QueryPragmas.EMPTY.maxQueryBranches();
        var e = expectThrows(VerificationException.class, () -> planSubquery(nestedSubqueries(limit / 2 + 1)));
        assertThat(
            e.getMessage(),
            containsString("query resolved to " + (limit + 2) + " branches in total, exceeding the limit of " + limit)
        );
        // The message names the pragma so the limit is discoverable, and avoids saying "subqueries" alone - branches also come from
        // patterns expanding to several sources, from views, and from PromQL.
        assertThat(e.getMessage(), containsString("set by the [" + QueryPragmas.MAX_QUERY_BRANCHES.getKey() + "] query pragma"));
        assertThat(
            e.getMessage(),
            containsString(
                "Reduce the number of sources - subqueries, patterns expanding to several indices, or views - "
                    + "or split this into multiple queries."
            )
        );
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
        assertThat(e.getMessage(), containsString("query resolved to 6 branches in total, exceeding the limit of 4"));
    }

    /**
     * Builds {@code levels} nested unions, each with two branches - the {@code test} index plus the next level down - for a total of
     * {@code 2 * levels} branches. Two branches per level keeps every union above the single-branch shape that
     * {@code FlattenNestedSubqueries} would collapse, so the count survives optimization.
     * <p>
     * {@code nestedSubqueries(2)} returns the query text:
     * <pre>
     * FROM test, (FROM test, (FROM test))
     * </pre>
     * Each parenthesized source is a subquery, and a {@code FROM} with two sources is one union, so that text nests two unions with
     * four branches between them:
     * <pre>
     * UnionAll                     &lt;- level 1, branches: test + the level-2 subquery
     * |_ test
     * \_ Subquery
     *    \_ UnionAll               &lt;- level 2, branches: test + the innermost subquery
     *       |_ test
     *       \_ Subquery
     *          \_ test             &lt;- innermost source, a plain relation rather than a union
     * </pre>
     * The recursion adds one union of two branches per level, so {@code nestedSubqueries(3)} is
     * {@code FROM test, (FROM test, (FROM test, (FROM test)))} - three unions, six branches, which is the count
     * {@link #testMaxBranchesHonorsPragma} relies on.
     */
    private static String nestedSubqueries(int levels) {
        StringBuilder query = new StringBuilder("FROM test");
        for (int i = 0; i < levels; i++) {
            query.insert(0, "FROM test, (").append(")");
        }
        return query.toString();
    }
}
