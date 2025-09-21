/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;

public class SubqueryTests extends AbstractStatementParserTests {

    public void testIndexPatternWithSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        for (String mainQueryIndexPattern : List.of("index1", "index1,index3")) {
            for (String subqueryIndexPattern : List.of("index2", "index2,index4", "index1,index2,index4")) {
                String query = LoggerMessageFormat.format(null, """
                    FROM {}, (FROM {})
                    """, mainQueryIndexPattern, subqueryIndexPattern);

                LogicalPlan plan = statement(query);
                validateSimpleSubqueryPlan(plan, mainQueryIndexPattern, subqueryIndexPattern);
            }
        }
    }

    public void testSubqueryWithProcessingCommandsInMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        for (String mainQueryIndexPattern : List.of("index1", "index1,index3")) {
            for (String subqueryIndexPattern : List.of("index2", "index2,index4", "index1,index2,index4")) {
                String query = LoggerMessageFormat.format(null, """
                    FROM {}, (FROM {})
                    | WHERE a > 10
                    | EVAL b = a * 2
                    | FORK (WHERE c < 100) (WHERE d > 200)
                    | STATS cnt = COUNT(*) BY e
                    | SORT cnt desc
                    | LIMIT 10
                    | DROP f
                    | KEEP g
                    """, mainQueryIndexPattern, subqueryIndexPattern);

                LogicalPlan plan = statement(query);
                Keep keep = as(plan, Keep.class);
                Drop drop = as(keep.child(), Drop.class);
                Limit limit = as(drop.child(), Limit.class);
                OrderBy orderBy = as(limit.child(), OrderBy.class);
                Aggregate aggregate = as(orderBy.child(), Aggregate.class);
                Fork fork = as(aggregate.child(), Fork.class);
                List<LogicalPlan> forkChildren = fork.children();
                assertEquals(2, forkChildren.size());
                for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
                    Filter forkFilter = as(forkEval.child(), Filter.class);
                    Eval eval = as(forkFilter.child(), Eval.class);
                    Filter filter = as(eval.child(), Filter.class);
                    validateSimpleSubqueryPlan(filter.child(), mainQueryIndexPattern, subqueryIndexPattern);
                }
            }
        }
    }

    private void validateSimpleSubqueryPlan(LogicalPlan plan, String mainQueryIndexPattern, String subqueryIndexPattern) {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(mainQueryIndexPattern, unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        UnresolvedRelation subqueryRelation = as(subquery.plan(), UnresolvedRelation.class);
        assertEquals(subqueryIndexPattern, subqueryRelation.indexPattern().indexPattern());
    }

    public void testWithSubqueryWithProcessingCommandsInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM index1, (FROM index2
                                      | WHERE a > 10
                                      | EVAL b = a * 2
                                      | FORK (WHERE c < 100) (WHERE d > 200)
                                      | STATS cnt = COUNT(*) BY e
                                      | SORT cnt desc
                                      | LIMIT 10
                                      | DROP f
                                      | KEEP g)
            """;

        LogicalPlan plan = statement(query);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals("index1", unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Keep keep = as(subquery.plan(), Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            UnresolvedRelation subqueryRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals("index2", subqueryRelation.indexPattern().indexPattern());
        }
    }

    public void testSubqueryWithProcessingCommandsInSubqueryAndMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM index1, (FROM index2
                                      | WHERE a > 10
                                      | EVAL b = a * 2
                                      | FORK (WHERE c < 100) (WHERE d > 200)
                                      | STATS cnt = COUNT(*) BY e
                                      | SORT cnt desc
                                      | LIMIT 10
                                      | DROP f
                                      | KEEP g)
             | WHERE a > 10
             | EVAL b = a * 2
             | FORK (WHERE c < 100) (WHERE d > 200)
             | STATS cnt = COUNT(*) BY e
             | SORT cnt desc
             | LIMIT 10
             | DROP f
             | KEEP g
            """;

        LogicalPlan plan = statement(query);
        Keep keep = as(plan, Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);

            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(2, children.size());

            UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals("index1", unresolvedRelation.indexPattern().indexPattern());

            Subquery subquery = as(children.get(1), Subquery.class);
            Keep subqueryKeep = as(subquery.plan(), Keep.class);
            Drop subqueryDrop = as(subqueryKeep.child(), Drop.class);
            Limit subqueryLimit = as(subqueryDrop.child(), Limit.class);
            OrderBy subqueryOrderby = as(subqueryLimit.child(), OrderBy.class);
            Aggregate subqueryAggregate = as(subqueryOrderby.child(), Aggregate.class);
            Fork subqueryFork = as(subqueryAggregate.child(), Fork.class);
            List<LogicalPlan> subqueryForkChildren = subqueryFork.children();
            assertEquals(2, forkChildren.size());
            for (Eval subqueryForkEval : List.of(
                as(subqueryForkChildren.get(0), Eval.class),
                as(subqueryForkChildren.get(1), Eval.class)
            )) {
                Filter subqueryForkFilter = as(subqueryForkEval.child(), Filter.class);
                Eval subqueryEval = as(subqueryForkFilter.child(), Eval.class);
                Filter subqueryFilter = as(subqueryEval.child(), Filter.class);
                UnresolvedRelation subqueryRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
                assertEquals("index2", subqueryRelation.indexPattern().indexPattern());
            }
        }
    }

    public void testSubqueryOnly() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM (FROM index2)
            """;

        LogicalPlan plan = statement(query);
        UnresolvedRelation unresolvedRelation = as(plan, UnresolvedRelation.class);
        assertEquals("index2", unresolvedRelation.indexPattern().indexPattern());
    }

    public void testSubqueryOnlyWithProcessingCommandInMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM (FROM index2)
             | WHERE a > 10
             | EVAL b = a * 2
             | FORK (WHERE c < 100) (WHERE d > 200)
             | STATS cnt = COUNT(*) BY e
             | SORT cnt desc
             | LIMIT 10
             | DROP f
             | KEEP g
            """;

        LogicalPlan plan = statement(query);
        Keep keep = as(plan, Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            UnresolvedRelation unresolvedRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals("index2", unresolvedRelation.indexPattern().indexPattern());
        }
    }

    public void testSubqueryOnlyWithProcessingCommandsInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM (FROM index2
                         | WHERE a > 10
                         | EVAL b = a * 2
                         | FORK (WHERE c < 100) (WHERE d > 200)
                         | STATS cnt = COUNT(*) BY e
                         | SORT cnt desc
                         | LIMIT 10
                         | DROP f
                         | KEEP g)
            """;

        LogicalPlan plan = statement(query);
        Keep keep = as(plan, Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            UnresolvedRelation subqueryRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals("index2", subqueryRelation.indexPattern().indexPattern());
        }
    }

    public void testSubqueryOnlyWithProcessingCommandsInSubqueryAndMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM (FROM index2
                         | WHERE a > 10
                         | EVAL b = a * 2
                         | FORK (WHERE c < 100) (WHERE d > 200)
                         | STATS cnt = COUNT(*) BY e
                         | SORT cnt desc
                         | LIMIT 10
                         | DROP f
                         | KEEP g)
              | WHERE a > 10
              | EVAL b = a * 2
              | FORK (WHERE c < 100) (WHERE d > 200)
              | STATS cnt = COUNT(*) BY e
              | SORT cnt desc
              | LIMIT 10
              | DROP f
              | KEEP g
            """;

        LogicalPlan plan = statement(query);
        Keep keep = as(plan, Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            Keep subqueryKeep = as(filter.child(), Keep.class);
            Drop subqueryDrop = as(subqueryKeep.child(), Drop.class);
            Limit subqueryLimit = as(subqueryDrop.child(), Limit.class);
            OrderBy subqueryOrderby = as(subqueryLimit.child(), OrderBy.class);
            Aggregate subqueryAggregate = as(subqueryOrderby.child(), Aggregate.class);
            Fork subqueryFork = as(subqueryAggregate.child(), Fork.class);
            List<LogicalPlan> subqueryForkChildren = subqueryFork.children();
            assertEquals(2, forkChildren.size());
            for (Eval subqueryForkEval : List.of(
                as(subqueryForkChildren.get(0), Eval.class),
                as(subqueryForkChildren.get(1), Eval.class)
            )) {
                Filter subqueryForkFilter = as(subqueryForkEval.child(), Filter.class);
                Eval subqueryEval = as(subqueryForkFilter.child(), Eval.class);
                Filter subqueryFilter = as(subqueryEval.child(), Filter.class);
                UnresolvedRelation subqueryRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
                assertEquals("index2", subqueryRelation.indexPattern().indexPattern());
            }
        }
    }

    public void testMultipleSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM index1, (FROM index2), index3, (FROM index4)
            """;

        LogicalPlan plan = statement(query);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals("index1,index3", unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery1 = as(children.get(1), Subquery.class);
        UnresolvedRelation subqueryRelation1 = as(subquery1.plan(), UnresolvedRelation.class);
        assertEquals("index2", subqueryRelation1.indexPattern().indexPattern());
        Subquery subquery2 = as(children.get(2), Subquery.class);
        UnresolvedRelation subqueryRelation2 = as(subquery2.plan(), UnresolvedRelation.class);
        assertEquals("index4", subqueryRelation2.indexPattern().indexPattern());
    }

    public void testMultipleSubqueriesWithProcessingCommands() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM index1, (FROM index2
                                      | WHERE a > 10
                                      | EVAL b = a * 2
                                      | LOOKUP JOIN index3 ON c
                                      | FORK (WHERE c < 100) (WHERE d > 200)
                                      | STATS cnt = COUNT(*) BY e
                                      | SORT cnt desc
                                      | LIMIT 10
                                      | DROP f
                                      | KEEP g)
             , index3, (FROM index4)
              | WHERE a > 10
              | EVAL b = a * 2
              | LOOKUP JOIN index3 ON c
              | FORK (WHERE c < 100) (WHERE d > 200)
              | STATS cnt = COUNT(*) BY e
              | SORT cnt desc
              | LIMIT 10
              | DROP f
              | KEEP g
            """;

        LogicalPlan plan = statement(query);
        Keep keep = as(plan, Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            LookupJoin lookupJoin = as(forkFilter.child(), LookupJoin.class);
            Eval eval = as(lookupJoin.left(), Eval.class);
            UnresolvedRelation joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
            assertEquals("index3", joinRelation.indexPattern().indexPattern());
            Filter filter = as(eval.child(), Filter.class);

            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(3, children.size());

            UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals("index1,index3", unresolvedRelation.indexPattern().indexPattern());

            Subquery subquery1 = as(children.get(1), Subquery.class);
            Keep subqueryKeep = as(subquery1.plan(), Keep.class);
            Drop subqueryDrop = as(subqueryKeep.child(), Drop.class);
            Limit subqueryLimit = as(subqueryDrop.child(), Limit.class);
            OrderBy subqueryOrderby = as(subqueryLimit.child(), OrderBy.class);
            Aggregate subqueryAggregate = as(subqueryOrderby.child(), Aggregate.class);
            Fork subqueryFork = as(subqueryAggregate.child(), Fork.class);
            List<LogicalPlan> subqueryForkChildren = subqueryFork.children();
            assertEquals(2, forkChildren.size());
            for (Eval subqueryForkEval : List.of(
                as(subqueryForkChildren.get(0), Eval.class),
                as(subqueryForkChildren.get(1), Eval.class)
            )) {
                Filter subqueryForkFilter = as(subqueryForkEval.child(), Filter.class);
                LookupJoin subqueryLookupJoin = as(subqueryForkFilter.child(), LookupJoin.class);
                Eval subqueryEval = as(subqueryLookupJoin.left(), Eval.class);
                UnresolvedRelation subqueryJoinRelation = as(subqueryLookupJoin.right(), UnresolvedRelation.class);
                assertEquals("index3", subqueryJoinRelation.indexPattern().indexPattern());
                Filter subqueryFilter = as(subqueryEval.child(), Filter.class);
                UnresolvedRelation subqueryRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
                assertEquals("index2", subqueryRelation.indexPattern().indexPattern());
            }

            Subquery subquery2 = as(children.get(2), Subquery.class);
            UnresolvedRelation subqueryRelation2 = as(subquery2.plan(), UnresolvedRelation.class);
            assertEquals("index4", subqueryRelation2.indexPattern().indexPattern());
        }
    }

    // unionAlls can be flattened
    public void testNestedSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM index1, (FROM index2, (FROM index3, (FROM index4)))
            """;

        LogicalPlan plan = statement(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals("index1", unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery1 = as(children.get(1), Subquery.class);
        unionAll = as(subquery1.plan(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals("index2", unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery2 = as(children.get(1), Subquery.class);
        unionAll = as(subquery2.plan(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals("index3", unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery3 = as(children.get(1), Subquery.class);
        unresolvedRelation = as(subquery3.plan(), UnresolvedRelation.class);
        assertEquals("index4", unresolvedRelation.indexPattern().indexPattern());
    }

    public void testNestedSubqueryWithProcessingCommands() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM index1, (FROM index2, (FROM index3, (FROM index4
                                                                                          | WHERE a > 10)
                                                                | EVAL b = a * 2)
                                      |STATS cnt = COUNT(*) BY e)
            | LIMIT 10
            """;

        LogicalPlan plan = statement(query);
        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals("index1", unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery1 = as(children.get(1), Subquery.class);
        Aggregate aggregate = as(subquery1.plan(), Aggregate.class);
        unionAll = as(aggregate.child(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals("index2", unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery2 = as(children.get(1), Subquery.class);
        Eval eval = as(subquery2.plan(), Eval.class);
        unionAll = as(eval.child(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals("index3", unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery3 = as(children.get(1), Subquery.class);
        Filter filter = as(subquery3.plan(), Filter.class);
        unresolvedRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("index4", unresolvedRelation.indexPattern().indexPattern());
    }

    public void testSubqueriesWithMetadada() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             FROM index1, (FROM index2, (FROM index3) metadata _score | WHERE a > 10) metadata _index
             | STATS cnt = COUNT(*) BY a
            """;

        LogicalPlan plan = statement(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        UnionAll unionAll = as(aggregate.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals("index1", mainRelation.indexPattern().indexPattern());
        List<Attribute> metadata = mainRelation.metadataFields();
        assertEquals(1, metadata.size());
        MetadataAttribute metadataAttribute = as(metadata.get(0), MetadataAttribute.class);
        assertEquals("_index", metadataAttribute.name());

        Subquery subquery = as(children.get(1), Subquery.class);
        Filter filter = as(subquery.plan(), Filter.class);
        unionAll = as(filter.child(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        UnresolvedRelation subqueryRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals("index2", subqueryRelation.indexPattern().indexPattern());
        metadata = subqueryRelation.metadataFields();
        assertEquals(2, metadata.size());
        metadataAttribute = as(metadata.get(0), MetadataAttribute.class);
        assertEquals("_score", metadataAttribute.name());
        metadataAttribute = as(metadata.get(1), MetadataAttribute.class);
        assertEquals("_index", metadataAttribute.name());

        subquery = as(children.get(1), Subquery.class);
        subqueryRelation = as(subquery.plan(), UnresolvedRelation.class);
        assertEquals("index3", subqueryRelation.indexPattern().indexPattern());
        metadata = subqueryRelation.metadataFields();
        assertEquals(2, metadata.size());
        metadataAttribute = as(metadata.get(0), MetadataAttribute.class);
        assertEquals("_score", metadataAttribute.name());
        metadataAttribute = as(metadata.get(1), MetadataAttribute.class);
        assertEquals("_index", metadataAttribute.name());
    }

    public void testTimeSeriesWithSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
             TS index1, (FROM index2)
            """;

        expectThrows(
            ParsingException.class,
            containsString("line 1:2: Subqueries are not supported in TS command"),
            () -> statement(query)
        );
    }
}
