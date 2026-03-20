/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Tests for subquery batch execution in ComputeService.
 * Verifies that limiting concurrent subqueries via the {@code subquery_batch_size} pragma
 * produces correct results across different batch sizes and query shapes.
 */
public class SubqueryIT extends AbstractEsqlIntegTestCase {

    @Before
    public void checkSubqueryInFromCommandSupport() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testSubqueryBatchSizeOne() {
        var query = """
            FROM
               ( FROM test | WHERE content:"fox" ),
               ( FROM test | WHERE content:"dog" ),
               ( FROM test | WHERE content:"cat" )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.SUBQUERY_BATCH_SIZE.getKey(), 1).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "This is a brown fox"),
                List.of(2, "This is a brown dog"),
                List.of(3, "This dog is really brown"),
                List.of(4, "The dog is brown but this document is very very long"),
                List.of(5, "There is also a white cat"),
                List.of(6, "The quick brown fox jumps over the lazy dog"),
                List.of(6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testSubqueryBatchSizeTwo() {
        var query = """
            FROM
               ( FROM test | WHERE id == 6 ),
               ( FROM test | WHERE id == 2 ),
               ( FROM test | WHERE id == 5 ),
               ( FROM test | WHERE id == 1 ),
               ( FROM test | WHERE id == 3 )
            | SORT id
            | KEEP id, content
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.SUBQUERY_BATCH_SIZE.getKey(), 2).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "This is a brown fox"),
                List.of(2, "This is a brown dog"),
                List.of(3, "This dog is really brown"),
                List.of(5, "There is also a white cat"),
                List.of(6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testSubqueryBatchSizeWithStatsAndBatchSizeOne() {
        var query = """
            FROM
               (FROM test | STATS x=COUNT(*), y=MV_SORT(VALUES(id)) ),
               (FROM test | WHERE id == 2 )
            | KEEP x, y, id
            | SORT x NULLS LAST
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.SUBQUERY_BATCH_SIZE.getKey(), 1).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("x", "y", "id"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                Arrays.stream(new Object[] { 6L, List.of(1, 2, 3, 4, 5, 6), null }).toList(),
                Arrays.stream(new Object[] { null, null, 2 }).toList()
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testSubqueryBatchSizeWithEmptyBranches() {
        var query = """
            FROM
               ( FROM test | WHERE content:"rabbit" ),
               ( FROM test | WHERE content:"dog" ),
               ( FROM test | WHERE content:"lion" ),
               ( FROM test | WHERE content:"cat" )
            | KEEP id, content
            | SORT id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.SUBQUERY_BATCH_SIZE.getKey(), 2).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(2, "This is a brown dog"),
                List.of(3, "This dog is really brown"),
                List.of(4, "The dog is brown but this document is very very long"),
                List.of(5, "There is also a white cat"),
                List.of(6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    private void createAndPopulateIndex() {
        var indexName = "test";
        var client = client().admin().indices();
        var createRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 6)))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(createRequest);
        client().prepareBulk()
            .add(new IndexRequest(indexName).id("1").source("id", 1, "content", "This is a brown fox"))
            .add(new IndexRequest(indexName).id("2").source("id", 2, "content", "This is a brown dog"))
            .add(new IndexRequest(indexName).id("3").source("id", 3, "content", "This dog is really brown"))
            .add(new IndexRequest(indexName).id("4").source("id", 4, "content", "The dog is brown but this document is very very long"))
            .add(new IndexRequest(indexName).id("5").source("id", 5, "content", "There is also a white cat"))
            .add(new IndexRequest(indexName).id("6").source("id", 6, "content", "The quick brown fox jumps over the lazy dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);
    }

    static Iterator<Iterator<Object>> valuesFilter(Iterator<Iterator<Object>> values, Predicate<Iterator<Object>> filter) {
        return getValuesList(values).stream().filter(row -> filter.test(row.iterator())).map(List::iterator).toList().iterator();
    }
}
