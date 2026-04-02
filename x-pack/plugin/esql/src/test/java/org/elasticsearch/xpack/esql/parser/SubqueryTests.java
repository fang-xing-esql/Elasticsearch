/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.CROSS_CLUSTER;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPatterns;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.unquoteIndexPattern;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.without;
import static org.hamcrest.Matchers.containsString;

public class SubqueryTests extends AbstractStatementParserTests {

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedRelation[]
     */
    public void testIndexPatternWithSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainQueryIndexPattern = randomIndexPatterns();
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (FROM {})
            """, mainQueryIndexPattern, subqueryIndexPattern);

        LogicalPlan plan = query(query);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        unresolvedRelation = as(subquery.plan(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * Subqueries in the FROM command with all the processing commands in the main query.
     * All processing commands are supported in the main query when subqueries exist in the
     * FROM command. With an exception on FORK, the grammar or parser doesn't block FORK,
     * however nested FORK will error out in the analysis or logical planning phase. We are hoping
     * to lift this restriction in the future, so it is not blocked in the grammar.
     *
     * Rerank[test_reranker[KEYWORD],war and peace[KEYWORD],[?title AS title#45],?_score]
     * \_Sample[0.5[DOUBLE]]
     *   \_Completion[test_completion[KEYWORD],?prompt,?completion_output]
     *     \_ChangePoint[?count,?@timestamp,type{r}#39,pvalue{r}#40]
     *       \_Enrich[ANY,clientip_policy[KEYWORD],?client_ip,null,{},[?env]]
     *         \_LookupJoin[LEFT,[?n],[?n],false,null]
     *           |_MvExpand[?m,?m]
     *           | \_Rename[[?k AS l#29]]
     *           |   \_Keep[[?j]]
     *           |     \_Drop[[?i]]
     *           |       \_Limit[10[INTEGER],false]
     *           |         \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *           |           \_Grok[?h,Parser[pattern=%{WORD:word} %{NUMBER:number},
     *           grok=org.elasticsearch.grok.Grok@710201ab],[number{r}#22, word{r}#23]]
     *           |             \_Dissect[?g,Parser[pattern=%{b} %{c}, appendSeparator=,
     *           parser=org.elasticsearch.dissect.DissectParser@6bd8533a],[b{r}#16, c{r}#17]]
     *           |               \_InlineStats[]
     *           |                 \_Aggregate[[?f],[?MAX[?e] AS max_e#14, ?f]]
     *           |                   \_Aggregate[[?e],[?COUNT[*] AS cnt#11, ?e]]
     *           |                     \_Fork[[]]
     *           |                       |_Eval[[fork1[KEYWORD] AS _fork#7]]
     *           |                       | \_Filter[?c &gt; 100[INTEGER]]
     *           |                       |   \_Eval[[?a * 2[INTEGER] AS b#5]]
     *           |                       |     \_Filter[?a &gt; 10[INTEGER]]
     *           |                       |       \_UnionAll[[]]
     *           |                       |         |_UnresolvedRelation[]
     *           |                       |         \_Subquery[]
     *           |                       |           \_Filter[?a &lt; 100[INTEGER]]
     *           |                       |             \_UnresolvedRelation[]
     *           |                       \_Eval[[fork2[KEYWORD] AS _fork#7]]
     *           |                         \_Filter[?d &gt; 200[INTEGER]]
     *           |                           \_Eval[[?a * 2[INTEGER] AS b#5]]
     *           |                             \_Filter[?a &lt; 10[INTEGER]]
     *           |                               \_UnionAll[[]]
     *           |                                 |_UnresolvedRelation[]
     *           |                                 \_Subquery[]
     *           |                                   \_Filter[?a &lt; 100[INTEGER]]
     *           |                                     \_UnresolvedRelation[]
     *           \_UnresolvedRelation[lookup_index]
     */
    public void testSubqueryWithAllProcessingCommandsInMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        // remote cluster does not support COMPLETION or RERANK
        var mainQueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var subqueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var joinIndexPattern = "lookup_index"; // randomIndexPatterns may generate on as index pattern, it collides with the ON token
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (FROM {} | WHERE a < 100)
            | WHERE a > 10
            | EVAL b = a * 2
            | FORK (WHERE c < 100) (WHERE d > 200)
            | STATS cnt = COUNT(*) BY e
            | INLINE STATS max_e = MAX(e) BY f
            | DISSECT g "%{b} %{c}"
            | GROK h "%{WORD:word} %{NUMBER:number}"
            | SORT cnt desc
            | LIMIT 10
            | DROP i
            | KEEP j
            | RENAME k AS l
            | MV_EXPAND m
            | LOOKUP JOIN {} ON n
            | ENRICH clientip_policy ON client_ip WITH env
            | CHANGE_POINT count ON @timestamp AS type, pvalue
            | COMPLETION completion_output = prompt WITH { "inference_id" : "test_completion" }
            | SAMPLE 0.5
            | RERANK "war and peace" ON title WITH { "inference_id" : "test_reranker" }
            """, mainQueryIndexPattern, subqueryIndexPattern, joinIndexPattern);

        LogicalPlan plan = query(query);
        Rerank rerank = as(plan, Rerank.class);
        Sample sample = as(rerank.child(), Sample.class);
        Completion completion = as(sample.child(), Completion.class);
        ChangePoint changePoint = as(completion.child(), ChangePoint.class);
        Enrich enrich = as(changePoint.child(), Enrich.class);
        LookupJoin lookupJoin = as(enrich.child(), LookupJoin.class);
        UnresolvedRelation joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
        MvExpand mvExpand = as(lookupJoin.left(), MvExpand.class);
        Rename rename = as(mvExpand.child(), Rename.class);
        Keep keep = as(rename.child(), Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Grok grok = as(orderBy.child(), Grok.class);
        Dissect dissect = as(grok.child(), Dissect.class);
        InlineStats inlineStats = as(dissect.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        aggregate = as(aggregate.child(), Aggregate.class);
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
            // main statement
            UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
            // subquery
            Subquery subquery = as(children.get(1), Subquery.class);
            Filter subqueryFilter = as(subquery.plan(), Filter.class);
            LessThan lessThan = as(subqueryFilter.condition(), LessThan.class);
            Attribute left = as(lessThan.left(), Attribute.class);
            assertEquals("a", left.name());
            Literal right = as(lessThan.right(), Literal.class);
            assertEquals(100, right.value());
            UnresolvedRelation subqueryRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(subqueryIndexPattern), subqueryRelation.indexPattern().indexPattern());
        }
    }

    /**
     * Subqueries in the FROM command with all the processing commands in the subquery query.
     * The grammar allows all processing commands inside the subquery. With an exception on FORK,
     * the grammar or parser doesn't block FORK, however nested FORK will error out in the analysis
     * or logical planning phase. We are hoping to lift this restriction in the future, so it is not blocked
     * in the grammar.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Rerank[test_reranker[KEYWORD],war and peace[KEYWORD],[?title AS title#30],?_score]
     *     \_Sample[0.5[DOUBLE]]
     *       \_Completion[test_completion[KEYWORD],?prompt,?completion_output]
     *         \_ChangePoint[?count,?@timestamp,type{r}#24,pvalue{r}#25]
     *           \_Enrich[ANY,clientip_policy[KEYWORD],?client_ip,null,{},[?env]]
     *             \_LookupJoin[LEFT,[?n],[?n],false,null]
     *               |_MvExpand[?m,?m]
     *               | \_Rename[[?k AS l#17]]
     *               |   \_Keep[[?j]]
     *               |     \_Drop[[?i]]
     *               |       \_Limit[10[INTEGER],false]
     *               |         \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *               |           \_Grok[?h,Parser[pattern=%{WORD:word} %{NUMBER:number},
     *               grok=org.elasticsearch.grok.Grok@2d54cab4],[number{r}#41, word{r}#42]]
     *               |             \_Dissect[?g,Parser[pattern=%{b} %{c}, appendSeparator=,
     *               parser=org.elasticsearch.dissect.DissectParser@5ca49d89],[b{r}#35, c{r}#36]]
     *               |               \_InlineStats[]
     *               |                 \_Aggregate[[?f],[?MAX[?e] AS max_e#10, ?f]]
     *               |                   \_Aggregate[[?e],[?COUNT[*] AS cnt#7, ?e]]
     *               |                     \_Fork[[]]
     *               |                       |_Eval[[fork1[KEYWORD] AS _fork#3]]
     *               |                       | \_Filter[?c &lt; 100[INTEGER]]
     *               |                       |   \_Eval[[?a * 2[INTEGER] AS b#34]]
     *               |                       |     \_Filter[?a &gt; 10[INTEGER]]
     *               |                       |       \_UnresolvedRelation[]
     *               |                       \_Eval[[fork2[KEYWORD] AS _fork#3]]
     *               |                         \_Filter[?d &gt; 200[INTEGER]]
     *               |                           \_Eval[[?a * 2[INTEGER] AS b#34]]
     *               |                             \_Filter[?a &gt; 10[INTEGER]]
     *               |                               \_UnresolvedRelation[]
     *               \_UnresolvedRelation[lookup_index]
     */
    public void testWithSubqueryWithProcessingCommandsInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainQueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var subqueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var joinIndexPattern = "lookup_index";
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}
                              | WHERE a > 10
                              | EVAL b = a * 2
                              | FORK (WHERE c < 100) (WHERE d > 200)
                              | STATS cnt = COUNT(*) BY e
                              | INLINE STATS max_e = MAX(e) BY f
                              | DISSECT g "%{b} %{c}"
                              | GROK h "%{WORD:word} %{NUMBER:number}"
                              | SORT cnt desc
                              | LIMIT 10
                              | DROP i
                              | KEEP j
                              | RENAME k AS l
                              | MV_EXPAND m
                              | LOOKUP JOIN {} ON n
                              | ENRICH clientip_policy ON client_ip WITH env
                              | CHANGE_POINT count ON @timestamp AS type, pvalue
                              | COMPLETION completion_output = prompt WITH { "inference_id" : "test_completion" }
                              | SAMPLE 0.5
                              | RERANK "war and peace" ON title WITH { "inference_id" : "test_reranker" })
            """, mainQueryIndexPattern, subqueryIndexPattern, joinIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // main query
        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
        // subquery
        Subquery subquery = as(children.get(1), Subquery.class);
        Rerank rerank = as(subquery.plan(), Rerank.class);
        Sample sample = as(rerank.child(), Sample.class);
        Completion completion = as(sample.child(), Completion.class);
        ChangePoint changePoint = as(completion.child(), ChangePoint.class);
        Enrich enrich = as(changePoint.child(), Enrich.class);
        LookupJoin lookupJoin = as(enrich.child(), LookupJoin.class);
        UnresolvedRelation joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
        MvExpand mvExpand = as(lookupJoin.left(), MvExpand.class);
        Rename rename = as(mvExpand.child(), Rename.class);
        Keep keep = as(rename.child(), Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Grok grok = as(orderBy.child(), Grok.class);
        Dissect dissect = as(grok.child(), Dissect.class);
        InlineStats inlineStats = as(dissect.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        aggregate = as(aggregate.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        List<LogicalPlan> forkChildren = fork.children();
        assertEquals(2, forkChildren.size());
        for (Eval forkEval : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter filter = as(eval.child(), Filter.class);
            UnresolvedRelation subqueryRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(subqueryIndexPattern), subqueryRelation.indexPattern().indexPattern());
        }
    }

    /**
     * A combination of the two previous tests with processing commands in both the subquery and main statement.
     * Plan string is skipped as it is too long, and it should be the combination of the above two tests..
     */
    public void testSubqueryWithProcessingCommandsInSubqueryAndMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainQueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var subqueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var joinIndexPattern = "lookup_index";
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}
                              | WHERE a > 10
                              | EVAL b = a * 2
                              | FORK (WHERE c < 100) (WHERE d > 200)
                              | STATS cnt = COUNT(*) BY e
                              | INLINE STATS max_e = MAX(e) BY f
                              | DISSECT g "%{b} %{c}"
                              | GROK h "%{WORD:word} %{NUMBER:number}"
                              | SORT cnt desc
                              | LIMIT 10
                              | DROP i
                              | KEEP j
                              | RENAME k AS l
                              | MV_EXPAND m
                              | LOOKUP JOIN {} ON n
                              | ENRICH clientip_policy ON client_ip WITH env
                              | CHANGE_POINT count ON @timestamp AS type, pvalue
                              | COMPLETION completion_output = prompt WITH { "inference_id" : "test_completion" }
                              | SAMPLE 0.5
                              | RERANK "war and peace" ON title WITH { "inference_id" : "test_reranker" })
             | WHERE a > 10
             | EVAL b = a * 2
             | FORK (WHERE c < 100) (WHERE d > 200)
             | STATS cnt = COUNT(*) BY e
             | INLINE STATS max_e = MAX(e) BY f
             | DISSECT g "%{b} %{c}"
             | GROK h "%{WORD:word} %{NUMBER:number}"
             | SORT cnt desc
             | LIMIT 10
             | DROP i
             | KEEP j
             | RENAME k AS l
             | MV_EXPAND m
             | LOOKUP JOIN {} ON n
             | ENRICH clientip_policy ON client_ip WITH env
             | CHANGE_POINT count ON @timestamp AS type, pvalue
             | COMPLETION completion_output = prompt WITH { "inference_id" : "test_completion" }
             | SAMPLE 0.5
             | RERANK "war and peace" ON title WITH { "inference_id" : "test_reranker" }
            """, mainQueryIndexPattern, subqueryIndexPattern, joinIndexPattern, joinIndexPattern);

        LogicalPlan plan = query(query);
        Rerank rerank = as(plan, Rerank.class);
        Sample sample = as(rerank.child(), Sample.class);
        Completion completion = as(sample.child(), Completion.class);
        ChangePoint changePoint = as(completion.child(), ChangePoint.class);
        Enrich enrich = as(changePoint.child(), Enrich.class);
        LookupJoin lookupJoin = as(enrich.child(), LookupJoin.class);
        UnresolvedRelation joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
        MvExpand mvExpand = as(lookupJoin.left(), MvExpand.class);
        Rename rename = as(mvExpand.child(), Rename.class);
        Keep keep = as(rename.child(), Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Grok grok = as(orderBy.child(), Grok.class);
        Dissect dissect = as(grok.child(), Dissect.class);
        InlineStats inlineStats = as(dissect.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        aggregate = as(aggregate.child(), Aggregate.class);
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
            // main statement
            UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
            // subquery
            Subquery subquery = as(children.get(1), Subquery.class);
            rerank = as(subquery.plan(), Rerank.class);
            sample = as(rerank.child(), Sample.class);
            completion = as(sample.child(), Completion.class);
            changePoint = as(completion.child(), ChangePoint.class);
            enrich = as(changePoint.child(), Enrich.class);
            lookupJoin = as(enrich.child(), LookupJoin.class);
            joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
            mvExpand = as(lookupJoin.left(), MvExpand.class);
            rename = as(mvExpand.child(), Rename.class);
            keep = as(rename.child(), Keep.class);
            drop = as(keep.child(), Drop.class);
            limit = as(drop.child(), Limit.class);
            orderBy = as(limit.child(), OrderBy.class);
            grok = as(orderBy.child(), Grok.class);
            dissect = as(grok.child(), Dissect.class);
            inlineStats = as(dissect.child(), InlineStats.class);
            aggregate = as(inlineStats.child(), Aggregate.class);
            aggregate = as(aggregate.child(), Aggregate.class);
            fork = as(aggregate.child(), Fork.class);
            forkChildren = fork.children();
            assertEquals(2, forkChildren.size());
            for (Eval forkEvalSubquery : List.of(as(forkChildren.get(0), Eval.class), as(forkChildren.get(1), Eval.class))) {
                forkFilter = as(forkEvalSubquery.child(), Filter.class);
                eval = as(forkFilter.child(), Eval.class);
                filter = as(eval.child(), Filter.class);
                UnresolvedRelation subqueryRelation = as(filter.child(), UnresolvedRelation.class);
                assertEquals(unquoteIndexPattern(subqueryIndexPattern), subqueryRelation.indexPattern().indexPattern());
            }
        }
    }

    /**
     * Verify there is no parsing error if the subquery ends with different modes.
     */
    public void testSubqueryEndsWithProcessingCommandsInDifferentMode() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        List<String> processingCommandInDifferentMode = List.of(
            "INLINE STATS max_e = MAX(e) BY f",  // inline mode, expression mode
            "DISSECT g \"%{b} %{c}\"",  // expression mode
            "LOOKUP JOIN index1 ON n", // join mode
            "ENRICH clientip_policy ON client_ip WITH env", // enrich mode
            "CHANGE_POINT count ON @timestamp AS type, pvalue", // change_point mode
            "FORK (WHERE c < 100) (WHERE d > 200)", // fork mode
            "MV_EXPAND m", // mv_expand mode
            "RENAME k AS l", // rename mode
            "DROP i" // project mode
        );
        var mainQueryIndexPattern = randomIndexPatterns();
        var subqueryIndexPattern = randomIndexPatterns();
        for (String processingCommand : processingCommandInDifferentMode) {
            String query = LoggerMessageFormat.format(null, """
                 FROM {}, (FROM {}
                                  | {})
                  | WHERE a > 10
                """, mainQueryIndexPattern, subqueryIndexPattern, processingCommand);

            LogicalPlan plan = query(query);
            Filter filter = as(plan, Filter.class);
            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(2, children.size());
            // main statement
            UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
            // subquery
            Subquery subquery = as(children.get(1), Subquery.class);
        }
    }

    /**
     * UnionAll[[]]
     * |_Subquery[]
     * | \_UnresolvedRelation[]
     * |_Subquery[]
     * | \_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedRelation[]
     */
    public void testSubqueryOnly() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var subqueryIndexPattern1 = randomIndexPatterns();
        var subqueryIndexPattern2 = randomIndexPatterns();
        var subqueryIndexPattern3 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM (FROM {}), (FROM {}), (FROM {})
            """, subqueryIndexPattern1, subqueryIndexPattern2, subqueryIndexPattern3);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        Subquery subquery = as(children.get(0), Subquery.class);
        UnresolvedRelation unresolvedRelation = as(subquery.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern1), unresolvedRelation.indexPattern().indexPattern());

        subquery = as(children.get(1), Subquery.class);
        unresolvedRelation = as(subquery.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern2), unresolvedRelation.indexPattern().indexPattern());

        subquery = as(children.get(2), Subquery.class);
        unresolvedRelation = as(subquery.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern3), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * If the FROM command contains only one subquery, the subquery is merged into an index pattern.
     *
     * Keep[[?g]]
     * \_Drop[[?f]]
     *   \_Limit[10[INTEGER],false]
     *     \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *       \_Aggregate[[?e],[?COUNT[*] AS cnt#10, ?e]]
     *         \_Fork[[]]
     *           |_Eval[[fork1[KEYWORD] AS _fork#6]]
     *           | \_Filter[?c &lt; 100[INTEGER]]
     *           |   \_Eval[[?a * 2[INTEGER] AS b#4]]
     *           |     \_Filter[?a &gt; 10[INTEGER]]
     *           |       \_UnresolvedRelation[]
     *           \_Eval[[fork2[KEYWORD] AS _fork#6]]
     *             \_Filter[?d &gt; 200[INTEGER]]
     *               \_Eval[[?a * 2[INTEGER] AS b#4]]
     *                 \_Filter[?a &gt; 10[INTEGER]]
     *                   \_UnresolvedRelation[]
     */
    public void testSubqueryOnlyWithProcessingCommandInMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM (FROM {})
             | WHERE a > 10
             | EVAL b = a * 2
             | FORK (WHERE c < 100) (WHERE d > 200)
             | STATS cnt = COUNT(*) BY e
             | SORT cnt desc
             | LIMIT 10
             | DROP f
             | KEEP g
            """, subqueryIndexPattern);

        LogicalPlan plan = query(query);
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
            assertEquals(unquoteIndexPattern(subqueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
        }
    }

    /**
     * Keep[[?g]]
     * \_Drop[[?f]]
     *   \_Limit[10[INTEGER],false]
     *     \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *       \_Aggregate[[?e],[?COUNT[*] AS cnt#7, ?e]]
     *         \_Fork[[]]
     *           |_Eval[[fork1[KEYWORD] AS _fork#3]]
     *           | \_Filter[?c &lt; 100[INTEGER]]
     *           |   \_Eval[[?a * 2[INTEGER] AS b#13]]
     *           |     \_Filter[?a &gt; 10[INTEGER]]
     *           |       \_UnresolvedRelation[]
     *           \_Eval[[fork2[KEYWORD] AS _fork#3]]
     *             \_Filter[?d &gt; 200[INTEGER]]
     *               \_Eval[[?a * 2[INTEGER] AS b#13]]
     *                 \_Filter[?a &gt; 10[INTEGER]]
     *                   \_UnresolvedRelation[]
     */
    public void testSubqueryOnlyWithProcessingCommandsInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM (FROM {}
                         | WHERE a > 10
                         | EVAL b = a * 2
                         | FORK (WHERE c < 100) (WHERE d > 200)
                         | STATS cnt = COUNT(*) BY e
                         | SORT cnt desc
                         | LIMIT 10
                         | DROP f
                         | KEEP g)
            """, subqueryIndexPattern);

        LogicalPlan plan = query(query);
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
            assertEquals(unquoteIndexPattern(subqueryIndexPattern), subqueryRelation.indexPattern().indexPattern());
        }
    }

    /**
     * If the FROM command contains only a subquery, the subquery is merged into an index pattern.
     *
     * Keep[[?g]]
     * \_Drop[[?f]]
     *   \_Limit[10[INTEGER],false]
     *     \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *       \_Aggregate[[?e],[?COUNT[*] AS cnt#23, ?e]]
     *         \_Fork[[]]
     *           |_Eval[[fork1[KEYWORD] AS _fork#19]]
     *           | \_Filter[?c &lt; 100[INTEGER]]
     *           |   \_Eval[[?a * 2[INTEGER] AS b#17]]
     *           |     \_Filter[?a &gt; 10[INTEGER]]
     *           |       \_Keep[[?g]]
     *           |         \_Drop[[?f]]
     *           |           \_Limit[10[INTEGER],false]
     *           |             \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *           |               \_Aggregate[[?e],[?COUNT[*] AS cnt#7, ?e]]
     *           |                 \_Fork[[]]
     *           |                   |_Eval[[fork1[KEYWORD] AS _fork#3]]
     *           |                   | \_Filter[?c &lt; 100[INTEGER]]
     *           |                   |   \_Eval[[?a * 2[INTEGER] AS b#13]]
     *           |                   |     \_Filter[?a &gt; 10[INTEGER]]
     *           |                   |       \_UnresolvedRelation[]
     *           |                   \_Eval[[fork2[KEYWORD] AS _fork#3]]
     *           |                     \_Filter[?d &gt; 200[INTEGER]]
     *           |                       \_Eval[[?a * 2[INTEGER] AS b#13]]
     *           |                         \_Filter[?a &gt; 10[INTEGER]]
     *           |                           \_UnresolvedRelation[]
     *           \_Eval[[fork2[KEYWORD] AS _fork#19]]
     *             \_Filter[?d &gt; 200[INTEGER]]
     *               \_Eval[[?a * 2[INTEGER] AS b#17]]
     *                 \_Filter[?a &gt; 10[INTEGER]]
     *                   \_Keep[[?g]]
     *                     \_Drop[[?f]]
     *                       \_Limit[10[INTEGER],false]
     *                         \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *                           \_Aggregate[[?e],[?COUNT[*] AS cnt#7, ?e]]
     *                             \_Fork[[]]
     *                               |_Eval[[fork1[KEYWORD] AS _fork#3]]
     *                               | \_Filter[?c &lt; 100[INTEGER]]
     *                               |   \_Eval[[?a * 2[INTEGER] AS b#13]]
     *                               |     \_Filter[?a &gt; 10[INTEGER]]
     *                               |       \_UnresolvedRelation[]
     *                               \_Eval[[fork2[KEYWORD] AS _fork#3]]
     *                                 \_Filter[?d &gt; 200[INTEGER]]
     *                                   \_Eval[[?a * 2[INTEGER] AS b#13]]
     *                                     \_Filter[?a &gt; 10[INTEGER]]
     *                                       \_UnresolvedRelation[]
     */
    public void testSubqueryOnlyWithProcessingCommandsInSubqueryAndMainquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM (FROM {}
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
            """, subqueryIndexPattern);

        LogicalPlan plan = query(query);
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
                assertEquals(unquoteIndexPattern(subqueryIndexPattern), subqueryRelation.indexPattern().indexPattern());
            }
        }
    }

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * |_Subquery[]
     * | \_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedRelation[]
     */
    public void testMultipleMixedIndexPatternsAndSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var indexPattern1 = randomIndexPatterns();
        var indexPattern2 = randomIndexPatterns();
        var indexPattern3 = randomIndexPatterns();
        var indexPattern4 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}), {}, (FROM {})
            """, indexPattern1, indexPattern2, indexPattern3, indexPattern4);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(
            unquoteIndexPattern(indexPattern1) + "," + unquoteIndexPattern(indexPattern3),
            unresolvedRelation.indexPattern().indexPattern()
        );

        Subquery subquery1 = as(children.get(1), Subquery.class);
        unresolvedRelation = as(subquery1.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern2), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery2 = as(children.get(2), Subquery.class);
        unresolvedRelation = as(subquery2.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern4), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * Keep[[?g]]
     * \_Drop[[?f]]
     *   \_Limit[10[INTEGER],false]
     *     \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *       \_Aggregate[[?e],[?COUNT[*] AS cnt#25, ?e]]
     *         \_Fork[[]]
     *           |_Eval[[fork1[KEYWORD] AS _fork#21]]
     *           | \_Filter[?c &lt; 100[INTEGER]]
     *           |   \_LookupJoin[LEFT,[?c],[?c],true,null]
     *           |     |_Eval[[?a * 2[INTEGER] AS b#18]]
     *           |     | \_Filter[?a &gt; 10[INTEGER]]
     *           |     |   \_UnionAll[[]]
     *           |     |     |_UnresolvedRelation[]
     *           |     |     |_Subquery[]
     *           |     |     | \_Keep[[?g]]
     *           |     |     |   \_Drop[[?f]]
     *           |     |     |     \_Limit[10[INTEGER],false]
     *           |     |     |       \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *           |     |     |         \_Aggregate[[?e],[?COUNT[*] AS cnt#8, ?e]]
     *           |     |     |           \_Fork[[]]
     *           |     |     |             |_Eval[[fork1[KEYWORD] AS _fork#4]]
     *           |     |     |             | \_Filter[?c &lt; 100[INTEGER]]
     *           |     |     |             |   \_LookupJoin[LEFT,[?c],[?c],true,null]
     *           |     |     |             |     |_Eval[[?a * 2[INTEGER] AS b#14]]
     *           |     |     |             |     | \_Filter[?a &gt; 10[INTEGER]]
     *           |     |     |             |     |   \_UnresolvedRelation[]
     *           |     |     |             |     \_UnresolvedRelation[lookup_index]
     *           |     |     |             \_Eval[[fork2[KEYWORD] AS _fork#4]]
     *           |     |     |               \_Filter[?d &gt; 200[INTEGER]]
     *           |     |     |                 \_LookupJoin[LEFT,[?c],[?c],true,null]
     *           |     |     |                   |_Eval[[?a * 2[INTEGER] AS b#14]]
     *           |     |     |                   | \_Filter[?a &gt; 10[INTEGER]]
     *           |     |     |                   |   \_UnresolvedRelation[]
     *           |     |     |                   \_UnresolvedRelation[lookup_index]
     *           |     |     \_Subquery[]
     *           |     |       \_UnresolvedRelation[]
     *           |     \_UnresolvedRelation[lookup_index]
     *           \_Eval[[fork2[KEYWORD] AS _fork#21]]
     *             \_Filter[?d > 200[INTEGER]]
     *               \_LookupJoin[LEFT,[?c],[?c],true,null]
     *                 |_Eval[[?a * 2[INTEGER] AS b#18]]
     *                 | \_Filter[?a &gt; 10[INTEGER]]
     *                 |   \_UnionAll[[]]
     *                 |     |_UnresolvedRelation[]
     *                 |     |_Subquery[]
     *                 |     | \_Keep[[?g]]
     *                 |     |   \_Drop[[?f]]
     *                 |     |     \_Limit[10[INTEGER],false]
     *                 |     |       \_OrderBy[[Order[?cnt,DESC,FIRST]]]
     *                 |     |         \_Aggregate[[?e],[?COUNT[*] AS cnt#8, ?e]]
     *                 |     |           \_Fork[[]]
     *                 |     |             |_Eval[[fork1[KEYWORD] AS _fork#4]]
     *                 |     |             | \_Filter[?c &lt; 100[INTEGER]]
     *                 |     |             |   \_LookupJoin[LEFT,[?c],[?c],true,null]
     *                 |     |             |     |_Eval[[?a * 2[INTEGER] AS b#14]]
     *                 |     |             |     | \_Filter[?a &gt; 10[INTEGER]]
     *                 |     |             |     |   \_UnresolvedRelation[]
     *                 |     |             |     \_UnresolvedRelation[lookup_index]
     *                 |     |             \_Eval[[fork2[KEYWORD] AS _fork#4]]
     *                 |     |               \_Filter[?d &gt; 200[INTEGER]]
     *                 |     |                 \_LookupJoin[LEFT,[?c],[?c],true,null]
     *                 |     |                   |_Eval[[?a * 2[INTEGER] AS b#14]]
     *                 |     |                   | \_Filter[?a &gt; 10[INTEGER]]
     *                 |     |                   |   \_UnresolvedRelation[]
     *                 |     |                   \_UnresolvedRelation[lookup_index]
     *                 |     \_Subquery[]
     *                 |       \_UnresolvedRelation[]
     *                 \_UnresolvedRelation[lookup_index]
     */
    public void testMultipleSubqueriesWithProcessingCommands() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainIndexPattern1 = randomIndexPatterns();
        var mainIndexPattern2 = randomIndexPatterns();
        var subqueryIndexPattern1 = randomIndexPatterns();
        var subqueryIndexPattern2 = randomIndexPatterns();
        var joinIndexPattern = "lookup_index";
        var combinedIndexPattern = unquoteIndexPattern(mainIndexPattern1) + "," + unquoteIndexPattern(mainIndexPattern2);
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}
                              | WHERE a > 10
                              | EVAL b = a * 2
                              | LOOKUP JOIN {} ON c
                              | FORK (WHERE c < 100) (WHERE d > 200)
                              | STATS cnt = COUNT(*) BY e
                              | SORT cnt desc
                              | LIMIT 10
                              | DROP f
                              | KEEP g)
             , {}, (FROM {})
              | WHERE a > 10
              | EVAL b = a * 2
              | LOOKUP JOIN {} ON c
              | FORK (WHERE c < 100) (WHERE d > 200)
              | STATS cnt = COUNT(*) BY e
              | SORT cnt desc
              | LIMIT 10
              | DROP f
              | KEEP g
            """, mainIndexPattern1, subqueryIndexPattern1, joinIndexPattern, mainIndexPattern2, subqueryIndexPattern2, joinIndexPattern);

        LogicalPlan plan = query(query);
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
            assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
            Filter filter = as(eval.child(), Filter.class);

            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(3, children.size());

            UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(combinedIndexPattern), unresolvedRelation.indexPattern().indexPattern());
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
                joinRelation = as(subqueryLookupJoin.right(), UnresolvedRelation.class);
                assertEquals(unquoteIndexPattern(joinIndexPattern), joinRelation.indexPattern().indexPattern());
                Filter subqueryFilter = as(subqueryEval.child(), Filter.class);
                unresolvedRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
                assertEquals(unquoteIndexPattern(subqueryIndexPattern1), unresolvedRelation.indexPattern().indexPattern());
            }

            Subquery subquery2 = as(children.get(2), Subquery.class);
            unresolvedRelation = as(subquery2.plan(), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(subqueryIndexPattern2), unresolvedRelation.indexPattern().indexPattern());
        }
    }

    /**
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_UnionAll[[]]
     *         |_UnresolvedRelation[]
     *         \_Subquery[]
     *           \_UnresolvedRelation[]
     */
    public void testSimpleNestedSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var indexPattern1 = randomIndexPatterns();
        var indexPattern2 = randomIndexPatterns();
        var indexPattern3 = randomIndexPatterns();
        var indexPattern4 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}, (FROM {}, (FROM {})))
            """, indexPattern1, indexPattern2, indexPattern3, indexPattern4);

        LogicalPlan plan = query(query);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern1), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery1 = as(children.get(1), Subquery.class);
        unionAll = as(subquery1.plan(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());

        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern2), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery2 = as(children.get(1), Subquery.class);
        unionAll = as(subquery2.plan(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());

        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern3), unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery3 = as(children.get(1), Subquery.class);
        unresolvedRelation = as(subquery3.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern4), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * LogicalPlanBuilder does not flatten nested subqueries with processing commands,
     * the structure of the nested subqueries s preserved in the parsed plan.
     *
     * Limit[10[INTEGER],false]
     * \_UnionAll[[]]
     *   |_UnresolvedRelation[]
     *   \_Subquery[]
     *     \_Aggregate[[?e],[?COUNT[*] AS cnt#7, ?e]]
     *       \_UnionAll[[]]
     *         |_UnresolvedRelation[]
     *         \_Subquery[]
     *           \_Eval[[?a * 2[INTEGER] AS b#4]]
     *             \_UnionAll[[]]
     *               |_UnresolvedRelation[]
     *               \_Subquery[]
     *                 \_Filter[?a > 10[INTEGER]]
     *                   \_UnresolvedRelation[]
     */
    public void testNestedSubqueryWithProcessingCommands() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var indexPattern1 = randomIndexPatterns();
        var indexPattern2 = randomIndexPatterns();
        var indexPattern3 = randomIndexPatterns();
        var indexPattern4 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}, (FROM {}, (FROM {}
                                                                | WHERE a > 10)
                                               | EVAL b = a * 2)
                              |STATS cnt = COUNT(*) BY e)
            | LIMIT 10
            """, indexPattern1, indexPattern2, indexPattern3, indexPattern4);

        LogicalPlan plan = query(query);
        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern1), unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery1 = as(children.get(1), Subquery.class);
        Aggregate aggregate = as(subquery1.plan(), Aggregate.class);
        unionAll = as(aggregate.child(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern2), unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery2 = as(children.get(1), Subquery.class);
        Eval eval = as(subquery2.plan(), Eval.class);
        unionAll = as(eval.child(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern3), unresolvedRelation.indexPattern().indexPattern());
        Subquery subquery3 = as(children.get(1), Subquery.class);
        Filter filter = as(subquery3.plan(), Filter.class);
        unresolvedRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern4), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * The medatada options from the main query are not propagated into subqueries.
     *
     * Aggregate[[?a],[?COUNT[*] AS cnt#6, ?a]]
     * \_UnionAll[[]]
     *   |_UnresolvedRelation[]
     *   \_Subquery[]
     *     \_Filter[?a &gt; 10[INTEGER]]
     *       \_UnionAll[[]]
     *         |_UnresolvedRelation[]
     *         \_Subquery[]
     *           \_UnresolvedRelation[]
     */
    public void testSubqueriesWithMetadada() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var indexPattern1 = randomIndexPatterns();
        var indexPattern2 = randomIndexPatterns();
        var indexPattern3 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             FROM {}, (FROM {}, (FROM {}) metadata _score | WHERE a > 10) metadata _index
             | STATS cnt = COUNT(*) BY a
            """, indexPattern1, indexPattern2, indexPattern3);

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        UnionAll unionAll = as(aggregate.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // main statement
        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern1), mainRelation.indexPattern().indexPattern());
        List<NamedExpression> metadata = mainRelation.metadataFields();
        assertEquals(1, metadata.size());
        MetadataAttribute metadataAttribute = as(metadata.get(0), MetadataAttribute.class);
        assertEquals("_index", metadataAttribute.name());
        // subquery1
        Subquery subquery = as(children.get(1), Subquery.class);
        Filter filter = as(subquery.plan(), Filter.class);
        unionAll = as(filter.child(), UnionAll.class);
        children = unionAll.children();
        assertEquals(2, children.size());
        UnresolvedRelation subqueryRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern2), subqueryRelation.indexPattern().indexPattern());
        metadata = subqueryRelation.metadataFields();
        assertEquals(1, metadata.size());
        metadataAttribute = as(metadata.get(0), MetadataAttribute.class);
        assertEquals("_score", metadataAttribute.name());
        // subquery2
        subquery = as(children.get(1), Subquery.class);
        subqueryRelation = as(subquery.plan(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern3), subqueryRelation.indexPattern().indexPattern());
        metadata = subqueryRelation.metadataFields();
        assertEquals(0, metadata.size());
    }

    /**
     * Aggregate[[?a],[?COUNT[*] AS cnt#4, ?a]]
     * \_UnionAll[[]]
     *   |_UnresolvedRelation[]
     *   \_Subquery[]
     *     \_Filter[?a &gt; 10[INTEGER]]
     *       \_UnresolvedRelation[]
     */
    public void testSubqueryWithRemoteCluster() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainRemoteIndexPattern = randomIndexPatterns(CROSS_CLUSTER);
        var mainIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var combinedMainIndexPattern = unquoteIndexPattern(mainRemoteIndexPattern) + "," + unquoteIndexPattern(mainIndexPattern);
        var subqueryRemoteIndexPattern = randomIndexPatterns(CROSS_CLUSTER);
        var subqueryIndexPattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var combinedSubqueryIndexPattern = unquoteIndexPattern(subqueryRemoteIndexPattern)
            + ","
            + unquoteIndexPattern(subqueryIndexPattern);
        String query = LoggerMessageFormat.format(null, """
             FROM {}, {}, (FROM {}, {} | WHERE a > 10)
             | STATS cnt = COUNT(*) BY a
            """, mainRemoteIndexPattern, mainIndexPattern, subqueryRemoteIndexPattern, subqueryIndexPattern);

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        UnionAll unionAll = as(aggregate.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // main statement
        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(combinedMainIndexPattern, mainRelation.indexPattern().indexPattern());
        // subquery
        Subquery subquery = as(children.get(1), Subquery.class);
        Filter filter = as(subquery.plan(), Filter.class);
        UnresolvedRelation unresolvedRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals(combinedSubqueryIndexPattern, unresolvedRelation.indexPattern().indexPattern());
    }

    public void testTimeSeriesWithSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var mainIndexPattern = randomIndexPatterns();
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
             TS index1, (FROM index2)
            """, mainIndexPattern, subqueryIndexPattern);

        expectThrows(ParsingException.class, containsString("line 1:2: Subqueries are not supported in TS command"), () -> query(query));
    }

    // ---- WHERE IN subquery tests ----

    /**
     * Basic WHERE x IN (FROM subquery_index) test.
     *
     * Filter[InSubquery[?x, subquery_plan]]
     *   \_UnresolvedRelation[]
     */
    public void testWhereInSubqueryBasic() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        String query = "FROM main_index | WHERE x IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * WHERE x NOT IN (FROM subquery_index) test.
     *
     * Filter[Not[InSubquery[?x, subquery_plan]]]
     *   \_UnresolvedRelation[]
     */
    public void testWhereNotInSubquery() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        String query = "FROM main_index | WHERE x NOT IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Not not = as(filter.condition(), Not.class);
        InSubquery inSubquery = as(not.field(), InSubquery.class);
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
    }

    /**
     * (NOT) IN subquery with multiple processing commands in the subquery.
     */
    public void testWhereInSubqueryMultipleProcessingCommands() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT" : "";
        String query = LoggerMessageFormat.format(null, """
            FROM main_index
            | WHERE x {} IN (FROM sub_index
                          | WHERE a > 1
                          | EVAL b = a * 2
                          | FORK (WHERE c < 100) (WHERE d > 200)
                          | STATS cnt = COUNT(*) BY e
                          | INLINE STATS max_e = MAX(e) BY f
                          | DISSECT g "%{b} %{c}"
                          | GROK h "%{WORD:word} %{NUMBER:number}"
                          | SORT cnt desc
                          | LIMIT 10
                          | DROP i
                          | KEEP j
                          | RENAME k AS l
                          | MV_EXPAND m
                          | LOOKUP JOIN lookup_index ON n
                          | ENRICH clientip_policy ON client_ip WITH env
                          | CHANGE_POINT count ON @timestamp AS type, pvalue)
            """, notClause);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(filter.condition(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filter.condition(), InSubquery.class);
        }

        ChangePoint changePoint = as(inSubquery.subquery(), ChangePoint.class);
        Enrich enrich = as(changePoint.child(), Enrich.class);
        LookupJoin lookupJoin = as(enrich.child(), LookupJoin.class);
        UnresolvedRelation joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
        assertEquals("lookup_index", joinRelation.indexPattern().indexPattern());
        MvExpand mvExpand = as(lookupJoin.left(), MvExpand.class);
        Rename rename = as(mvExpand.child(), Rename.class);
        Keep keep = as(rename.child(), Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Grok grok = as(orderBy.child(), Grok.class);
        Dissect dissect = as(grok.child(), Dissect.class);
        InlineStats inlineStats = as(dissect.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        aggregate = as(aggregate.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        assertEquals(2, fork.children().size());
        // Each fork branch wraps the preceding pipeline: Eval(fork) -> Filter(fork) -> Eval -> Filter -> UnresolvedRelation
        for (LogicalPlan branch : fork.children()) {
            Eval forkEval = as(branch, Eval.class);
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter subqueryFilter = as(eval.child(), Filter.class);
            UnresolvedRelation subqueryRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
            assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
        }
        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * WHERE (NOT) IN subquery ends with different modes to verify lexer mode transitions.
     */
    public void testWhereInSubqueryEndsWithDifferentModes() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        List<String> processingCommands = List.of(
            "WHERE a > 10",
            "EVAL b = a * 2",
            "KEEP x",
            "DROP y",
            "SORT a",
            "LIMIT 10",
            "STATS cnt = COUNT(*) BY a",
            "RENAME a AS b",
            "MV_EXPAND m",
            "CHANGE_POINT a ON b",
            "ENRICH my_policy ON x",
            "FORK (WHERE a > 1)(WHERE a < 10)",
            "INLINE STATS cnt = COUNT(*) BY a",
            "LOOKUP JOIN lookup_index ON x"
        );
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT" : "";
        for (String processingCommand : processingCommands) {
            String query = LoggerMessageFormat.format(null, """
                FROM main_index | WHERE x {} IN (FROM sub_index | {})
                """, notClause, processingCommand);

            LogicalPlan plan = query(query);
            Filter filter = as(plan, Filter.class);
            InSubquery inSubquery;
            if (negated) {
                Not not = as(filter.condition(), Not.class);
                inSubquery = as(not.field(), InSubquery.class);
            } else {
                inSubquery = as(filter.condition(), InSubquery.class);
            }
            UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals("main_index", mainRelation.indexPattern().indexPattern());
        }
    }

    /**
     * WHERE IN subquery combined with other boolean expressions.
     *
     * Filter[And[GreaterThan[?a, 5], InSubquery[?x, ...]]]
     *   \_UnresolvedRelation[]
     */
    public void testWhereInSubqueryWithOtherConditions() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        String query = "FROM main_index | WHERE a > 5 AND x IN (FROM sub_index | KEEP y)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);
        as(and.left(), GreaterThan.class);
        InSubquery inSubquery = as(and.right(), InSubquery.class);
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());
    }

    /**
     * Existing value list IN still works after the grammar changes.
     */
    public void testWhereInValueListStillWorks() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        String query = "FROM main_index | WHERE x IN (1, 2, 3)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        In in = as(filter.condition(), In.class);
        Attribute value = as(in.value(), Attribute.class);
        assertEquals("x", value.name());
        assertEquals(3, in.list().size());
    }

    /**
     * IN value-list with a mix of constants and field references:
     * {@code WHERE x IN (1, y, "hello", z)}
     */
    public void testWhereInListMixedConstantsAndFields() {
        String query = "FROM main_index | WHERE x IN (1, y, \"hello\", z)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        In in = as(filter.condition(), In.class);

        UnresolvedAttribute value = as(in.value(), UnresolvedAttribute.class);
        assertEquals("x", value.name());

        List<Expression> list = in.list();
        assertEquals(4, list.size());
        Literal lit1 = as(list.get(0), Literal.class);
        assertEquals(1, lit1.value());
        UnresolvedAttribute fieldY = as(list.get(1), UnresolvedAttribute.class);
        assertEquals("y", fieldY.name());
        Literal litHello = as(list.get(2), Literal.class);
        assertEquals(new BytesRef("hello"), litHello.value());
        UnresolvedAttribute fieldZ = as(list.get(3), UnresolvedAttribute.class);
        assertEquals("z", fieldZ.name());
    }

    /**
     * Multiple IN and/or NOT IN subqueries in the same WHERE clause, combined with AND or OR.
     */
    public void testMultipleInSubqueries() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        boolean firstNegated = randomBoolean();
        boolean secondNegated = randomBoolean();
        boolean useAnd = randomBoolean();
        String first = firstNegated ? "NOT IN" : "IN";
        String second = secondNegated ? "NOT IN" : "IN";
        String op = useAnd ? "AND" : "OR";
        String query = LoggerMessageFormat.format(null, """
            FROM main_index
            | WHERE x {} (FROM sub1 | KEEP a) {} y {} (FROM sub2 | KEEP b)
            """, first, op, second);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Expression condition = filter.condition();

        Expression left;
        Expression right;
        if (useAnd) {
            And and = as(condition, And.class);
            left = and.left();
            right = and.right();
        } else {
            Or or = as(condition, Or.class);
            left = or.left();
            right = or.right();
        }

        // Verify first IN/NOT IN subquery
        InSubquery firstIn;
        if (firstNegated) {
            firstIn = as(as(left, Not.class).field(), InSubquery.class);
        } else {
            firstIn = as(left, InSubquery.class);
        }
        Keep firstKeep = as(firstIn.subquery(), Keep.class);
        UnresolvedRelation firstRelation = as(firstKeep.child(), UnresolvedRelation.class);
        assertEquals("sub1", firstRelation.indexPattern().indexPattern());

        // Verify second IN/NOT IN subquery
        InSubquery secondIn;
        if (secondNegated) {
            secondIn = as(as(right, Not.class).field(), InSubquery.class);
        } else {
            secondIn = as(right, InSubquery.class);
        }
        Keep secondKeep = as(secondIn.subquery(), Keep.class);
        UnresolvedRelation secondRelation = as(secondKeep.child(), UnresolvedRelation.class);
        assertEquals("sub2", secondRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * Two IN predicates in the same WHERE clause, each randomly an IN value-list or IN subquery, with random NOT.
     */
    public void testWhereInSubqueryMixedWithInList() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        boolean leftIsSubquery = randomBoolean();
        boolean rightIsSubquery = randomBoolean();
        boolean leftNegated = randomBoolean();
        boolean rightNegated = randomBoolean();

        String leftPart = "x " + (leftNegated ? "NOT " : "") + (leftIsSubquery ? "IN (FROM sub1 | KEEP a)" : "IN (1, 2, 3)");
        String rightPart = "y " + (rightNegated ? "NOT " : "") + (rightIsSubquery ? "IN (FROM sub2 | KEEP b)" : "IN (4, 5, 6)");
        String query = LoggerMessageFormat.format(null, """
            FROM main_index | WHERE {} AND {}
            """, leftPart, rightPart);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);

        assertInPredicate(and.left(), leftNegated, leftIsSubquery, "sub1");
        assertInPredicate(and.right(), rightNegated, rightIsSubquery, "sub2");

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    private void assertInPredicate(Expression expr, boolean negated, boolean isSubquery, String expectedIndex) {
        Expression inner = negated ? as(expr, Not.class).field() : expr;
        if (isSubquery) {
            InSubquery inSubquery = as(inner, InSubquery.class);
            Keep keep = as(inSubquery.subquery(), Keep.class);
            UnresolvedRelation relation = as(keep.child(), UnresolvedRelation.class);
            assertEquals(expectedIndex, relation.indexPattern().indexPattern());
        } else {
            as(inner, In.class);
        }
    }

    /**
     * IN subquery where the subquery's FROM command includes METADATA fields.
     */
    public void testWhereInSubqueryWithMetadata() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        String query = """
            FROM main_index
            | WHERE x IN (FROM sub_index METADATA _id, _index | KEEP _id)
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);

        Keep keep = as(inSubquery.subquery(), Keep.class);
        UnresolvedRelation subRelation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub_index", subRelation.indexPattern().indexPattern());
        List<String> metadataFieldNames = subRelation.metadataFields().stream().map(NamedExpression::name).toList();
        assertEquals(List.of("_id", "_index"), metadataFieldNames);
    }

    /**
     * IN subquery where the subquery's FROM command references a remote cluster index.
     */
    public void testWhereInSubqueryWithRemoteCluster() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        String query = """
            FROM main_index
            | WHERE x IN (FROM remote_cluster:sub_index | KEEP a)
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);

        Keep keep = as(inSubquery.subquery(), Keep.class);
        UnresolvedRelation subRelation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("remote_cluster:sub_index", subRelation.indexPattern().indexPattern());
    }

    /**
     * IN subquery whose FROM command contains a nested FROM-subquery:
     * {@code FROM main | WHERE x IN (FROM sub1, (FROM sub2) | KEEP a)}
     */
    public void testWhereInSubqueryWithNestedFromSubquery() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM main_index
            | WHERE x IN (FROM sub1, (FROM sub2) | KEEP a)
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);

        Keep keep = as(inSubquery.subquery(), Keep.class);
        UnionAll unionAll = as(keep.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
        UnresolvedRelation sub1 = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());
        Subquery nested = as(unionAll.children().get(1), Subquery.class);
        UnresolvedRelation sub2 = as(nested.child(), UnresolvedRelation.class);
        assertEquals("sub2", sub2.indexPattern().indexPattern());
    }

    /**
     * Nested IN subqueries: the inner subquery itself contains a WHERE IN subquery:
     * {@code FROM main | WHERE x IN (FROM sub1 | WHERE y IN (FROM sub2 | KEEP b) | KEEP a)}
     */
    public void testWhereInSubqueryWithNestedInSubquery() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        String query = """
            FROM main_index
            | WHERE x IN (FROM sub1 | WHERE y IN (FROM sub2 | KEEP b) | KEEP a)
            """;

        LogicalPlan plan = query(query);
        Filter outerFilter = as(plan, Filter.class);
        InSubquery outerIn = as(outerFilter.condition(), InSubquery.class);

        Keep outerKeep = as(outerIn.subquery(), Keep.class);
        Filter innerFilter = as(outerKeep.child(), Filter.class);
        InSubquery innerIn = as(innerFilter.condition(), InSubquery.class);

        Keep innerKeep = as(innerIn.subquery(), Keep.class);
        UnresolvedRelation sub2 = as(innerKeep.child(), UnresolvedRelation.class);
        assertEquals("sub2", sub2.indexPattern().indexPattern());

        UnresolvedRelation sub1 = as(innerFilter.child(), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());

        UnresolvedRelation main = as(outerFilter.child(), UnresolvedRelation.class);
        assertEquals("main_index", main.indexPattern().indexPattern());
    }

    /**
     * FROM subquery where one branch contains a WHERE IN subquery:
     * {@code FROM main, (FROM sub1 | WHERE x IN (FROM sub2 | KEEP a) | KEEP x)}
     */
    public void testFromSubqueryWithWhereInSubqueryInside() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM main,
                 (FROM sub1 | WHERE x IN (FROM sub2 | KEEP a) | KEEP x)
            """;

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // main query
        UnresolvedRelation mainRelation = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals("main", mainRelation.indexPattern().indexPattern());

        // FROM subquery branch: Subquery -> Keep -> Filter(InSubquery) -> UnresolvedRelation
        Subquery subquery = as(unionAll.children().get(1), Subquery.class);
        Keep keep = as(subquery.plan(), Keep.class);
        Filter filter = as(keep.child(), Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);

        // the IN subquery's plan
        Keep innerKeep = as(inSubquery.subquery(), Keep.class);
        UnresolvedRelation sub2 = as(innerKeep.child(), UnresolvedRelation.class);
        assertEquals("sub2", sub2.indexPattern().indexPattern());

        // the FROM of the branch
        UnresolvedRelation sub1 = as(filter.child(), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());
    }

    // ---- WHERE IN subquery negative tests ----

    public void testWhereInSubqueryRejectsTsSourceCommand() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (TS sub_index)"));
        assertThat(e.getMessage(), containsString("no viable alternative at input 'x IN (TS'"));
    }

    public void testWhereInSubqueryRejectsRowSourceCommand() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (ROW a = 1)"));
        assertThat(e.getMessage(), containsString("no viable alternative at input 'x IN (ROW'"));
    }

    public void testWhereInSubqueryRejectsShowSourceCommand() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (SHOW INFO)"));
        assertThat(e.getMessage(), containsString("no viable alternative at input 'x IN (SHOW'"));
    }

    public void testWhereInSubqueryRejectsPromqlSourceCommand() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (PROMQL 'up')"));
        assertThat(e.getMessage(), containsString("no viable alternative at input 'x IN (PROMQL'"));
    }

    public void testWhereInSubqueryRejectsSubqueryWithTrailingTokens() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        var e1 = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (FROM sub | KEEP a, 1)"));
        assertThat(e1.getMessage(), containsString("token recognition error at: '1'"));
        var e2 = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (FROM sub | KEEP a KEEP b)"));
        assertThat(e2.getMessage(), containsString("extraneous input 'KEEP' expecting {'|', ')'}"));
    }

    public void testWhereInSubqueryRejectsMissingClosingParen() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (FROM sub"));
        assertThat(e.getMessage(), containsString("mismatched input '<EOF>' expecting {'|', ')'}"));
    }

    public void testWhereInSubqueryRejectsEmptySubquery() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN ()"));
        assertThat(e.getMessage(), containsString("no viable alternative at input 'x IN ()'"));
    }

    public void testWhereInSubqueryRejectsMultipleFromCommands() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.IN_SUBQUERY.isEnabled());
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (FROM sub1 | FROM sub2)"));
        assertThat(e.getMessage(), containsString("mismatched input 'FROM'"));
    }
}
