/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPatterns;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.unquoteIndexPattern;
import static org.hamcrest.Matchers.containsString;

/**
 * Parser tests for subqueries whose source command is {@code EXTERNAL}, e.g.
 * {@code FROM idx, (EXTERNAL "s3://bucket/data.parquet" | WHERE x > 0)}.
 * Counterpart to {@link SubqueryWithRowCommandTests}: same shape of assertions,
 * with {@link UnresolvedExternalRelation} replacing {@code Row} at the subquery leaf.
 */
public class SubqueryWithExternalCommandTests extends AbstractStatementParserTests {

    /**
     * A spread of external data source URIs covering the schemes the {@code EXTERNAL} command is
     * expected to handle: AWS S3, Google Cloud Storage, Azure Blob/Data Lake, a plain HTTPS endpoint
     * and a local filesystem CSV. The {@code subquerySourceCommand} grammar treats the URI as an
     * opaque string literal, so any of these is equally valid at parse time — picking one at random
     * keeps the tests from accidentally pinning behaviour to a single (S3) scheme.
     */
    private static final List<String> EXTERNAL_DATA_SOURCE_URIS = List.of(
        "s3://bucket/data.parquet",
        "gs://my-bucket/data/sales.parquet",
        "wasbs://account.blob.core.windows.net/container/path/data.parquet",
        "abfss://container@account.dfs.core.windows.net/path/data.parquet",
        "https://datasets.example.com/headerless.csv.gz",
        "file:///var/data/local/employees.csv"
    );

    /** Picks one of the {@link #EXTERNAL_DATA_SOURCE_URIS} at random. */
    private static String randomExternalUri() {
        return randomFrom(EXTERNAL_DATA_SOURCE_URIS);
    }

    /**
     * Bare data source names that are legal as a <em>quoted index string</em> directly in the FROM
     * target list — e.g. {@code FROM "employees.csv"}. Unlike {@link #EXTERNAL_DATA_SOURCE_URIS},
     * these carry no scheme ({@code ://}) and no path separators, so they survive the index-name
     * validation the parser applies to a quoted {@code indexPattern}. A bare quoted name parses to a
     * plain {@link UnresolvedRelation}; only the {@code EXTERNAL} command produces an
     * {@link UnresolvedExternalRelation}.
     */
    private static final List<String> BARE_QUOTED_LOCAL_SOURCES = List.of("employees.csv", "sales.tsv", "logs.ndjson", "data.parquet");

    /** Picks one of the {@link #BARE_QUOTED_LOCAL_SOURCES} at random. */
    private static String randomBareQuotedLocalSource() {
        return randomFrom(BARE_QUOTED_LOCAL_SOURCES);
    }

    @Before
    public void requireSubqueryWithExternalCommand() {
        assumeTrue(
            "Requires subquery with external command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITH_EXTERNAL_DATA_SOURCE.isEnabled()
        );
    }

    private static void requireSubqueryInFromCommand() {
        assumeTrue("Requires subquery in from command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireWhereInSubquery() {
        assumeTrue("Requires where in subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
    }

    /**
     * Single EXTERNAL subquery (over a randomly chosen data source) alongside an index pattern in the
     * main FROM:
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testIndexPatternWithExternalSubquery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (EXTERNAL "{}")
            """, mainQueryIndexPattern, uri);

        LogicalPlan plan = query(query);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        UnresolvedExternalRelation external = as(subquery.plan(), UnresolvedExternalRelation.class);
        assertExternalUri(external, uri);
        assertTrue("config should be empty for bare EXTERNAL", external.config().isEmpty());
    }

    /**
     * If the only child of FROM is an EXTERNAL subquery without an index pattern, the {@code UnionAll}
     * is collapsed and the {@link UnresolvedExternalRelation} is returned directly — mirrors the
     * behaviour for a single ROW subquery in {@link SubqueryWithRowCommandTests#testRowSubqueryOnly()}.
     *
     * UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testExternalSubqueryOnly() {
        requireSubqueryInFromCommand();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM (EXTERNAL "{}")
            """, uri);

        LogicalPlan plan = query(query);
        UnresolvedExternalRelation external = as(plan, UnresolvedExternalRelation.class);
        assertExternalUri(external, uri);
    }

    /**
     * Multiple EXTERNAL subqueries with no main index pattern produce a {@code UnionAll} of
     * {@code Subquery} over {@link UnresolvedExternalRelation}s.
     *
     * UnionAll[[]]
     * |_Subquery[]
     * | \_UnresolvedExternalRelation[&lt;random-uri-1&gt;]
     * \_Subquery[]
     *   \_UnresolvedExternalRelation[&lt;random-uri-2&gt;]
     */
    public void testMultipleExternalSubqueriesOnly() {
        requireSubqueryInFromCommand();
        String firstUri = randomExternalUri();
        String secondUri = randomValueOtherThan(firstUri, SubqueryWithExternalCommandTests::randomExternalUri);
        String query = LoggerMessageFormat.format(null, """
            FROM (EXTERNAL "{}"), (EXTERNAL "{}")
            """, firstUri, secondUri);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        Subquery first = as(children.get(0), Subquery.class);
        assertExternalUri(as(first.plan(), UnresolvedExternalRelation.class), firstUri);

        Subquery second = as(children.get(1), Subquery.class);
        assertExternalUri(as(second.plan(), UnresolvedExternalRelation.class), secondUri);
    }

    /**
     * Mix of an index pattern, an EXTERNAL subquery and a FROM subquery in the same FROM.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * |_Subquery[]
     * | \_Filter[?x &gt; 1[INTEGER]]
     * |   \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     * \_Subquery[]
     *   \_Filter[?x &gt; 1[INTEGER]]
     *     \_UnresolvedRelation[]
     */
    public void testIndexPatternWithExternalAndFromSubqueries() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        var subqueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (EXTERNAL "{}" | WHERE x > 1), (FROM {} | WHERE x > 1)
            """, mainQueryIndexPattern, uri, subqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        // main statement
        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        // EXTERNAL subquery
        Subquery externalSubquery = as(children.get(1), Subquery.class);
        Filter externalFilter = as(externalSubquery.plan(), Filter.class);
        GreaterThan externalCondition = as(externalFilter.condition(), GreaterThan.class);
        assertEquals("x", as(externalCondition.left(), Attribute.class).name());
        assertEquals(1, as(externalCondition.right(), Literal.class).value());
        assertExternalUri(as(externalFilter.child(), UnresolvedExternalRelation.class), uri);

        // FROM subquery
        Subquery fromSubquery = as(children.get(2), Subquery.class);
        Filter fromFilter = as(fromSubquery.plan(), Filter.class);
        UnresolvedRelation fromRelation = as(fromFilter.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern), fromRelation.indexPattern().indexPattern());
    }

    /**
     * An EXTERNAL subquery with several processing commands inside.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Limit[5[INTEGER],false]
     *     \_Eval[[?x + 1[INTEGER] AS y]]
     *       \_Filter[?x &gt; 0[INTEGER]]
     *         \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testExternalSubqueryWithProcessingCommandsInSubquery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (EXTERNAL "{}"
                      | WHERE x > 0
                      | EVAL y = x + 1
                      | LIMIT 5)
            """, mainQueryIndexPattern, uri);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Limit limit = as(subquery.plan(), Limit.class);
        assertEquals(5, as(limit.limit(), Literal.class).value());
        Eval eval = as(limit.child(), Eval.class);
        assertEquals("y", eval.fields().get(0).name());
        Filter filter = as(eval.child(), Filter.class);
        GreaterThan condition = as(filter.condition(), GreaterThan.class);
        assertEquals("x", as(condition.left(), Attribute.class).name());
        assertEquals(0, as(condition.right(), Literal.class).value());
        assertExternalUri(as(filter.child(), UnresolvedExternalRelation.class), uri);
    }

    /**
     * EXTERNAL subquery with processing commands in the main query (outside the UnionAll).
     *
     * Limit[10[INTEGER],false]
     * \_Filter[?x &gt; 5[INTEGER]]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testExternalSubqueryWithProcessingCommandsInMainQuery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (EXTERNAL "{}")
            | WHERE x > 5
            | LIMIT 10
            """, mainQueryIndexPattern, uri);

        LogicalPlan plan = query(query);
        Limit limit = as(plan, Limit.class);
        assertEquals(10, as(limit.limit(), Literal.class).value());
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        assertExternalUri(as(subquery.plan(), UnresolvedExternalRelation.class), uri);
    }

    /**
     * Processing commands in both the EXTERNAL subquery and the main query — confirms the
     * sub-pipeline's commands attach below the UnionAll while the outer commands attach above it.
     *
     * Limit[10[INTEGER],false]
     * \_Filter[?y &gt; 0[INTEGER]]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_Eval[[?x + 1[INTEGER] AS y]]
     *         \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testExternalSubqueryWithProcessingCommandsInSubqueryAndMainQuery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (EXTERNAL "{}" | EVAL y = x + 1)
            | WHERE y > 0
            | LIMIT 10
            """, mainQueryIndexPattern, uri);

        LogicalPlan plan = query(query);
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Eval eval = as(subquery.plan(), Eval.class);
        assertExternalUri(as(eval.child(), UnresolvedExternalRelation.class), uri);
    }

    /**
     * EXTERNAL subquery alongside another EXTERNAL subquery and a FROM subquery, with no main index.
     *
     * UnionAll[[]]
     * |_Subquery[]
     * | \_UnresolvedExternalRelation[&lt;random-uri-1&gt;]
     * |_Subquery[]
     * | \_UnresolvedExternalRelation[&lt;random-uri-2&gt;]
     * \_Subquery[]
     *   \_UnresolvedRelation[]
     */
    public void testExternalAndFromSubqueriesOnly() {
        requireSubqueryInFromCommand();
        var subqueryIndexPattern = randomIndexPatterns();
        String firstUri = randomExternalUri();
        String secondUri = randomValueOtherThan(firstUri, SubqueryWithExternalCommandTests::randomExternalUri);
        String query = LoggerMessageFormat.format(null, """
            FROM (EXTERNAL "{}"), (EXTERNAL "{}"), (FROM {})
            """, firstUri, secondUri, subqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        Subquery first = as(children.get(0), Subquery.class);
        assertExternalUri(as(first.plan(), UnresolvedExternalRelation.class), firstUri);

        Subquery second = as(children.get(1), Subquery.class);
        assertExternalUri(as(second.plan(), UnresolvedExternalRelation.class), secondUri);

        Subquery third = as(children.get(2), Subquery.class);
        UnresolvedRelation rel = as(third.plan(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern), rel.indexPattern().indexPattern());
    }

    /**
     * An EXTERNAL subquery nested inside a FROM subquery — the {@code UnionAll}s stack two levels deep.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testExternalSubqueryNestedInsideFromSubquery() {
        requireSubqueryInFromCommand();
        var outerIndexPattern = randomIndexPatterns();
        var innerIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (FROM {}, (EXTERNAL "{}"))
            """, outerIndexPattern, innerIndexPattern, uri);

        LogicalPlan plan = query(query);
        UnionAll outerUnion = as(plan, UnionAll.class);
        List<LogicalPlan> outerChildren = outerUnion.children();
        assertEquals(2, outerChildren.size());

        UnresolvedRelation outerRelation = as(outerChildren.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(outerIndexPattern), outerRelation.indexPattern().indexPattern());

        Subquery outerSubquery = as(outerChildren.get(1), Subquery.class);
        UnionAll innerUnion = as(outerSubquery.plan(), UnionAll.class);
        List<LogicalPlan> innerChildren = innerUnion.children();
        assertEquals(2, innerChildren.size());

        UnresolvedRelation innerRelation = as(innerChildren.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(innerIndexPattern), innerRelation.indexPattern().indexPattern());

        Subquery innerSubquery = as(innerChildren.get(1), Subquery.class);
        assertExternalUri(as(innerSubquery.plan(), UnresolvedExternalRelation.class), uri);
    }

    /**
     * EXTERNAL subquery carries a {@code WITH { ... }} options bag. The parser folds the option
     * literals into the {@link UnresolvedExternalRelation#config()} map; entries are preserved
     * verbatim ({@link BytesRef} for keyword string values).
     */
    public void testExternalSubqueryWithOptions() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (EXTERNAL "{}" WITH {"format": "parquet", "compression": "zstd"})
            """, mainQueryIndexPattern, uri);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        Subquery subquery = as(unionAll.children().get(1), Subquery.class);
        UnresolvedExternalRelation external = as(subquery.plan(), UnresolvedExternalRelation.class);
        assertExternalUri(external, uri);
        assertEquals(2, external.config().size());
        // foldOptionLiterals normalizes BytesRef string literals to plain String.
        assertEquals("parquet", external.config().get("format"));
        assertEquals("zstd", external.config().get("compression"));
    }

    /**
     * Verifies the parser accepts an EXTERNAL subquery whose trailing processing command sits in each
     * of the different ANTLR lexer modes the {@code processingCommand} rule can transition into.
     * Counterpart of {@link SubqueryWithRowCommandTests#testRowSubqueryEndsWithProcessingCommandsInDifferentMode()}.
     */
    public void testExternalSubqueryEndsWithProcessingCommandsInDifferentMode() {
        requireSubqueryInFromCommand();
        List<String> processingCommandInDifferentMode = List.of(
            "INLINE STATS max_x = MAX(x) BY x",
            "DISSECT y \"%{a} %{b}\"",
            "ENRICH clientip_policy ON x WITH env",
            "CHANGE_POINT x ON x AS type, pvalue",
            "FORK (WHERE x < 100) (WHERE x > 200)",
            "MV_EXPAND x",
            "RENAME x AS z",
            "DROP x"
        );
        var mainQueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        for (String processingCommand : processingCommandInDifferentMode) {
            String query = LoggerMessageFormat.format(null, """
                FROM {}, (EXTERNAL "{}" | {})
                | WHERE x > 0
                """, mainQueryIndexPattern, uri, processingCommand);

            LogicalPlan plan = query(query);
            Filter filter = as(plan, Filter.class);
            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(query, 2, children.size());
            UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals(query, unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());
            as(children.get(1), Subquery.class);
        }
    }

    /**
     * Mix of an index pattern, a ROW subquery, a FROM subquery and an EXTERNAL subquery (over a
     * randomly chosen data source) in the same FROM. Each non-leading source becomes its own
     * {@link Subquery} child of the {@link UnionAll}, with the leaf type tracking the source command.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * |_Subquery[]
     * | \_Row[[1[INTEGER] AS x]]
     * |_Subquery[]
     * | \_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testIndexPatternWithRowFromAndExternalSubqueries() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW x = 1), (FROM {}), (EXTERNAL "{}")
            """, mainQueryIndexPattern, fromSubqueryIndexPattern, uri);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(4, children.size());

        // main statement
        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        // ROW subquery
        Subquery rowSubquery = as(children.get(1), Subquery.class);
        assertRowField(as(rowSubquery.plan(), Row.class), "x", 1);

        // FROM subquery
        Subquery fromSubquery = as(children.get(2), Subquery.class);
        UnresolvedRelation fromRelation = as(fromSubquery.plan(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(fromSubqueryIndexPattern), fromRelation.indexPattern().indexPattern());

        // EXTERNAL subquery
        Subquery externalSubquery = as(children.get(3), Subquery.class);
        assertExternalUri(as(externalSubquery.plan(), UnresolvedExternalRelation.class), uri);
    }

    /**
     * A ROW subquery, a FROM subquery and an EXTERNAL subquery (over a randomly chosen data source)
     * with no main index pattern. All three are wrapped in their own {@link Subquery} under the
     * {@link UnionAll}.
     *
     * UnionAll[[]]
     * |_Subquery[]
     * | \_Row[[1[INTEGER] AS x]]
     * |_Subquery[]
     * | \_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testRowFromAndExternalSubqueriesOnly() {
        requireSubqueryInFromCommand();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM (ROW x = 1), (FROM {}), (EXTERNAL "{}")
            """, fromSubqueryIndexPattern, uri);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        Subquery rowSubquery = as(children.get(0), Subquery.class);
        assertRowField(as(rowSubquery.plan(), Row.class), "x", 1);

        Subquery fromSubquery = as(children.get(1), Subquery.class);
        UnresolvedRelation fromRelation = as(fromSubquery.plan(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(fromSubqueryIndexPattern), fromRelation.indexPattern().indexPattern());

        Subquery externalSubquery = as(children.get(2), Subquery.class);
        assertExternalUri(as(externalSubquery.plan(), UnresolvedExternalRelation.class), uri);
    }

    /**
     * Two EXTERNAL subqueries over two distinct data sources (e.g. an S3 bucket and an Azure blob)
     * fan out into sibling {@link Subquery} children. Confirms each URI is preserved independently
     * on its own {@link UnresolvedExternalRelation} leaf.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * |_Subquery[]
     * | \_UnresolvedExternalRelation[&lt;random-uri-1&gt;]
     * \_Subquery[]
     *   \_UnresolvedExternalRelation[&lt;random-uri-2&gt;]
     */
    public void testMultipleExternalSubqueriesWithDifferentDataSources() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String firstUri = randomExternalUri();
        String secondUri = randomValueOtherThan(firstUri, SubqueryWithExternalCommandTests::randomExternalUri);
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (EXTERNAL "{}"), (EXTERNAL "{}")
            """, mainQueryIndexPattern, firstUri, secondUri);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        Subquery first = as(children.get(1), Subquery.class);
        assertExternalUri(as(first.plan(), UnresolvedExternalRelation.class), firstUri);

        Subquery second = as(children.get(2), Subquery.class);
        assertExternalUri(as(second.plan(), UnresolvedExternalRelation.class), secondUri);
    }

    /**
     * Mix of a ROW subquery, a FROM subquery and an EXTERNAL subquery (over a randomly chosen data
     * source), each carrying its own inner processing pipeline. Verifies the per-branch commands
     * stack independently above each leaf inside the {@link UnionAll}.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * |_Subquery[]
     * | \_Filter[?x &gt; 0[INTEGER]]
     * |   \_Row[[1[INTEGER] AS x]]
     * |_Subquery[]
     * | \_Filter[?x &gt; 0[INTEGER]]
     * |   \_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Eval[[?x + 1[INTEGER] AS y]]
     *     \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testRowFromAndExternalSubqueriesWithProcessingCommands() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW x = 1 | WHERE x > 0),
                     (FROM {} | WHERE x > 0),
                     (EXTERNAL "{}" | EVAL y = x + 1)
            """, mainQueryIndexPattern, fromSubqueryIndexPattern, uri);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(4, children.size());

        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        // ROW subquery with inner WHERE
        Subquery rowSubquery = as(children.get(1), Subquery.class);
        Filter rowFilter = as(rowSubquery.plan(), Filter.class);
        GreaterThan rowCondition = as(rowFilter.condition(), GreaterThan.class);
        assertEquals("x", as(rowCondition.left(), Attribute.class).name());
        assertEquals(0, as(rowCondition.right(), Literal.class).value());
        assertRowField(as(rowFilter.child(), Row.class), "x", 1);

        // FROM subquery with inner WHERE
        Subquery fromSubquery = as(children.get(2), Subquery.class);
        Filter fromFilter = as(fromSubquery.plan(), Filter.class);
        UnresolvedRelation fromRelation = as(fromFilter.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(fromSubqueryIndexPattern), fromRelation.indexPattern().indexPattern());

        // EXTERNAL subquery with inner EVAL
        Subquery externalSubquery = as(children.get(3), Subquery.class);
        Eval eval = as(externalSubquery.plan(), Eval.class);
        assertEquals("y", eval.fields().get(0).name());
        assertExternalUri(as(eval.child(), UnresolvedExternalRelation.class), uri);
    }

    /**
     * An EXTERNAL subquery (over a randomly chosen data source) nested inside a FROM subquery that
     * also fans out a ROW subquery — the {@code UnionAll}s stack two levels deep with all three
     * source-command flavours represented.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     |_Subquery[]
     *     | \_Row[[1[INTEGER] AS x]]
     *     \_Subquery[]
     *       \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testExternalAndRowSubqueriesNestedInsideFromSubquery() {
        requireSubqueryInFromCommand();
        var outerIndexPattern = randomIndexPatterns();
        var innerIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (FROM {}, (ROW x = 1), (EXTERNAL "{}"))
            """, outerIndexPattern, innerIndexPattern, uri);

        LogicalPlan plan = query(query);
        UnionAll outerUnion = as(plan, UnionAll.class);
        List<LogicalPlan> outerChildren = outerUnion.children();
        assertEquals(2, outerChildren.size());

        UnresolvedRelation outerRelation = as(outerChildren.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(outerIndexPattern), outerRelation.indexPattern().indexPattern());

        Subquery outerSubquery = as(outerChildren.get(1), Subquery.class);
        UnionAll innerUnion = as(outerSubquery.plan(), UnionAll.class);
        List<LogicalPlan> innerChildren = innerUnion.children();
        assertEquals(3, innerChildren.size());

        UnresolvedRelation innerRelation = as(innerChildren.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(innerIndexPattern), innerRelation.indexPattern().indexPattern());

        Subquery rowSubquery = as(innerChildren.get(1), Subquery.class);
        assertRowField(as(rowSubquery.plan(), Row.class), "x", 1);

        Subquery externalSubquery = as(innerChildren.get(2), Subquery.class);
        assertExternalUri(as(externalSubquery.plan(), UnresolvedExternalRelation.class), uri);
    }

    // ---- external data source specified directly in the FROM target list (bare quoted name) ----

    /**
     * The external data source is specified directly inside the FROM command as a bare quoted name
     * (no {@code EXTERNAL} keyword, no subquery), e.g. {@code FROM "employees.csv"}. Because the name
     * carries no scheme or path separators it is a legal quoted {@code indexPattern} and parses to a
     * plain {@link UnresolvedRelation} — not an {@link UnresolvedExternalRelation}, which only the
     * {@code EXTERNAL} command produces.
     *
     * UnresolvedRelation[&lt;source&gt;]
     */
    public void testBareQuotedLocalSourceOnly() {
        requireSubqueryInFromCommand();
        String source = randomBareQuotedLocalSource();
        String query = LoggerMessageFormat.format(null, """
            FROM "{}"
            """, source);

        LogicalPlan plan = query(query);
        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals(source, relation.indexPattern().indexPattern());
    }

    /**
     * A bare quoted local source in the FROM target list alongside an {@code EXTERNAL} subquery. The
     * bare name is the leading {@link UnresolvedRelation} of the {@link UnionAll}; the {@code EXTERNAL}
     * source is wrapped in a {@link Subquery} over an {@link UnresolvedExternalRelation}. Confirms the
     * two ways of naming a data source coexist in one FROM.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[&lt;source&gt;]
     * \_Subquery[]
     *   \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testBareQuotedLocalSourceWithExternalSubquery() {
        requireSubqueryInFromCommand();
        String source = randomBareQuotedLocalSource();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM "{}", (EXTERNAL "{}")
            """, source, uri);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation relation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(source, relation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        assertExternalUri(as(subquery.plan(), UnresolvedExternalRelation.class), uri);
    }

    /**
     * A bare quoted local source in the FROM target list mixed with a ROW subquery, a FROM subquery
     * and an EXTERNAL subquery. The bare name leads as an {@link UnresolvedRelation}; every other
     * source is wrapped in its own {@link Subquery}, with the leaf type tracking the source command.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[&lt;source&gt;]
     * |_Subquery[]
     * | \_Row[[1[INTEGER] AS x]]
     * |_Subquery[]
     * | \_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedExternalRelation[&lt;random-uri&gt;]
     */
    public void testBareQuotedLocalSourceWithRowFromAndExternalSubqueries() {
        requireSubqueryInFromCommand();
        String source = randomBareQuotedLocalSource();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        String uri = randomExternalUri();
        String query = LoggerMessageFormat.format(null, """
            FROM "{}", (ROW x = 1), (FROM {}), (EXTERNAL "{}")
            """, source, fromSubqueryIndexPattern, uri);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(4, children.size());

        // bare quoted source
        UnresolvedRelation relation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(source, relation.indexPattern().indexPattern());

        // ROW subquery
        Subquery rowSubquery = as(children.get(1), Subquery.class);
        assertRowField(as(rowSubquery.plan(), Row.class), "x", 1);

        // FROM subquery
        Subquery fromSubquery = as(children.get(2), Subquery.class);
        UnresolvedRelation fromRelation = as(fromSubquery.plan(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(fromSubqueryIndexPattern), fromRelation.indexPattern().indexPattern());

        // EXTERNAL subquery
        Subquery externalSubquery = as(children.get(3), Subquery.class);
        assertExternalUri(as(externalSubquery.plan(), UnresolvedExternalRelation.class), uri);
    }

    // ---- negative tests ----

    /**
     * EXTERNAL is NOT accepted inside a {@code WHERE x IN (...)} subquery. The
     * {@code IN_SUBQUERY_LP} lexer rule (in {@code InExpression.g4}) only allowlists
     * {@code from | row | show | ts | promql} after the open paren — extending the
     * IN-subquery grammar to accept EXTERNAL is a separate piece of work from extending
     * {@code subquerySourceCommand}. This test pins that boundary so the rejection doesn't
     * silently disappear if either grammar is touched.
     */
    public void testWhereInSubqueryWithExternalSourceRejected() {
        requireWhereInSubquery();
        String query = LoggerMessageFormat.format(null, """
            FROM main_index | WHERE x IN (EXTERNAL "{}")
            """, randomExternalUri());
        expectThrows(ParsingException.class, () -> query(query));
    }

    /**
     * A scheme-bearing external data source URI cannot be named directly in the FROM target list as a
     * bare quoted index string — {@code FROM "s3://bucket/data.parquet"} is rejected because the
     * parser splits the quoted string on the {@code :} (cluster separator) and then validates the
     * index part, which contains the illegal {@code /} character. The same URI is, however, accepted
     * via the {@code EXTERNAL} subquery form. This pins the boundary that motivates the {@code EXTERNAL}
     * command: scheme URIs are external sources, not index names.
     */
    public void testBareQuotedSchemeUriRejectedButAcceptedViaExternal() {
        requireSubqueryInFromCommand();
        for (String uri : EXTERNAL_DATA_SOURCE_URIS) {
            // Bare quoted URI in the FROM target list: rejected as an invalid index name.
            String bareQuotedQuery = LoggerMessageFormat.format(null, """
                FROM "{}"
                """, uri);
            expectThrows(ParsingException.class, containsString("must not contain the following characters"), () -> query(bareQuotedQuery));

            // Same URI via the EXTERNAL subquery form: accepted, producing an UnresolvedExternalRelation.
            String externalQuery = LoggerMessageFormat.format(null, """
                FROM (EXTERNAL "{}")
                """, uri);
            assertExternalUri(as(query(externalQuery), UnresolvedExternalRelation.class), uri);
        }
    }

    /**
     * TS does not allow subqueries, including EXTERNAL subqueries.
     */
    public void testTimeSeriesWithExternalSubquery() {
        requireSubqueryInFromCommand();
        String query = LoggerMessageFormat.format(null, """
            TS index1, (EXTERNAL "{}")
            """, randomExternalUri());
        expectThrows(ParsingException.class, containsString("Subqueries are not supported in TS command"), () -> query(query));
    }

    /**
     * If the external datasources feature flag is off, the {@code subquerySourceCommand} alternative
     * for {@code externalCommand} is gated off in the grammar and the parser must reject it.
     * Mirrors {@link SubqueryWithRowCommandTests#testRowSubqueryNotAllowedInReleaseBuild()}.
     */
    public void testExternalSubqueryNotAllowedWhenFeatureFlagOff() {
        requireSubqueryInFromCommand();
        assumeFalse("only relevant when the external datasources feature flag is off", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (EXTERNAL "{}")
            """, mainQueryIndexPattern, randomExternalUri());

        expectThrows(ParsingException.class, () -> query(query));
    }

    /**
     * Asserts the given {@link UnresolvedExternalRelation}'s {@code tablePath} is a {@link Literal}
     * whose {@link BytesRef} value decodes to the expected URI string.
     */
    private static void assertExternalUri(UnresolvedExternalRelation external, String expectedUri) {
        Expression tablePath = external.tablePath();
        Literal literal = as(tablePath, Literal.class);
        Object value = literal.value();
        String actual = value instanceof BytesRef ? BytesRefs.toString((BytesRef) value) : value.toString();
        assertEquals(expectedUri, actual);
    }

    /**
     * Asserts the given {@link Row} has a single field with the given name whose child is an
     * integer {@link Literal} with the given value. Mirrors the helper used in
     * {@link SubqueryWithRowCommandTests}, kept local so the mixed ROW/FROM/EXTERNAL tests can
     * verify their {@code ROW} branches.
     */
    private static void assertRowField(Row row, String aliasName, int aliasValue) {
        assertEquals(1, row.fields().size());
        assertEquals(aliasName, row.fields().get(0).name());
        Literal literal = as(row.fields().get(0).child(), Literal.class);
        assertEquals(aliasValue, literal.value());
    }
}
