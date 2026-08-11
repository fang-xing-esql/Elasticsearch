/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizerTests.relation;

/**
 * Unit tests for {@link FlattenNestedSubqueries#flattenUnionAllWithOneChild}.
 *
 * <p>Tests build synthetic {@link UnionAll}/{@link ViewUnionAll} plans directly
 * (rather than going through the full analyzer) so that each branch of the method
 * can be exercised independently, including edge cases that the analyzer would not
 * normally produce.
 */
public class FlattenNestedSubqueriesTests extends ESTestCase {

    private static final FlattenNestedSubqueries RULE = new FlattenNestedSubqueries();

    private static LogicalPlan apply(UnionAll unionAll) {
        return RULE.apply(unionAll);
    }

    // ----- guards that leave the plan unchanged -----

    /**
     * {@link ViewUnionAll} (FORK output) must never be flattened.
     */
    public void testViewUnionAllIsNotFlattened() {
        ReferenceAttribute unionAttr = referenceAttribute("foo", INTEGER);
        FieldAttribute childAttr = fieldAttr("foo", INTEGER);
        var child = relation(childAttr);
        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("branch1", child);
        var vua = new ViewUnionAll(EMPTY, children, List.of(unionAttr));
        assertSame(vua, apply(vua));
    }

    /**
     * A two-branch {@link UnionAll} must be left unchanged.
     */
    public void testMultiChildUnionAllIsNotFlattened() {
        ReferenceAttribute unionAttr = referenceAttribute("foo", INTEGER);
        FieldAttribute childAttr = fieldAttr("foo", INTEGER);
        var child1 = relation(childAttr);
        var child2 = relation(childAttr);
        var unionAll = new UnionAll(EMPTY, List.of(child1, child2), List.of(unionAttr));
        assertSame(unionAll, apply(unionAll));
    }

    /**
     * Different output sizes: the union declares more columns than its single child.
     */
    public void testSizeMismatchIsNotFlattened() {
        ReferenceAttribute unionFoo = referenceAttribute("foo", INTEGER);
        ReferenceAttribute unionBar = referenceAttribute("bar", INTEGER);
        FieldAttribute childFoo = fieldAttr("foo", INTEGER);
        var child = relation(childFoo);
        var wide = new UnionAll(EMPTY, List.of(child), List.of(unionFoo, unionBar));
        assertSame(wide, apply(wide));
    }

    /**
     * Same names but different data types: must not flatten to avoid silently changing types.
     */
    public void testTypeMismatchIsNotFlattened() {
        ReferenceAttribute unionAttr = referenceAttribute("foo", KEYWORD);
        FieldAttribute childAttr = fieldAttr("foo", INTEGER);
        var child = relation(childAttr);
        var unionAll = new UnionAll(EMPTY, List.of(child), List.of(unionAttr));
        assertSame(unionAll, apply(unionAll));
    }

    /**
     * Duplicate attribute name in the child output: must not flatten, because the name-keyed
     * lookup used for correlation is ambiguous.
     */
    public void testChildDuplicateNameIsNotFlattened() {
        ReferenceAttribute unionFoo1 = referenceAttribute("foo", INTEGER);
        ReferenceAttribute unionFoo2 = referenceAttribute("foo", INTEGER);
        FieldAttribute childFoo1 = fieldAttr("foo", INTEGER);
        FieldAttribute childFoo2 = fieldAttr("foo", INTEGER);
        var child = relation(childFoo1, childFoo2);
        var unionAll = new UnionAll(EMPTY, List.of(child), List.of(unionFoo1, unionFoo2));
        assertSame(unionAll, apply(unionAll));
    }

    /**
     * Regression test for Bug 3: duplicate attribute name in the union output must not flatten.
     *
     * <p>Before the fix, the loop over {@code unionOutput} had no duplicate-name guard.
     * When two union attributes shared the same name (e.g. both named "foo"), the name-lookup
     * returned the same child attribute for both, producing a {@link Project} that:
     * <ul>
     *   <li>aliased two distinct union IDs to the same child attribute, and</li>
     *   <li>silently dropped the unrelated child column ("bar") from the projection.</li>
     * </ul>
     *
     * <p>Concrete failure before the fix:
     * <pre>
     * unionOutput = [foo{r}#1, foo{r}#2]   ← duplicate name "foo"
     * childOutput = [foo{f}#3, bar{f}#4]
     * sizes match (2 == 2); child has no duplicates → old guards pass
     * loop produced: Project[foo{f}#3 AS foo#1, foo{f}#3 AS foo#2]  ← "bar{f}#4" lost
     * </pre>
     * After the fix the union is returned unchanged.
     */
    public void testUnionDuplicateNameIsNotFlattened() {
        ReferenceAttribute unionFoo1 = referenceAttribute("foo", INTEGER);
        ReferenceAttribute unionFoo2 = referenceAttribute("foo", INTEGER);
        FieldAttribute childFoo = fieldAttr("foo", INTEGER);
        FieldAttribute childBar = fieldAttr("bar", INTEGER);
        var child = relation(childFoo, childBar);
        // unionOutput has two "foo" attrs; childOutput has "foo" and "bar" (no dups, same size)
        var unionAll = new UnionAll(EMPTY, List.of(child), List.of(unionFoo1, unionFoo2));
        assertSame(unionAll, apply(unionAll));
    }

    // ----- cases that do flatten -----

    /**
     * When union and child outputs are identical (same attribute instances), the union is replaced
     * by the child directly with no intervening {@link Project}.
     */
    public void testIdenticalOutputsFlattensWithoutProject() {
        FieldAttribute attr = fieldAttr("foo", INTEGER);
        var child = relation(attr);
        var unionAll = new UnionAll(EMPTY, List.of(child), List.of(attr));
        assertSame(child, apply(unionAll));
    }

    /**
     * When union and child attributes have the same name and type but different IDs (the common
     * post-analysis case), the rule inserts a correlating {@link Project} and removes the union.
     *
     * <p>Expected output:
     * <pre>
     * Project[childFoo AS foo#(unionId)]
     * └─ child
     * </pre>
     */
    public void testMismatchedIdsInsertsProject() {
        ReferenceAttribute unionFoo = referenceAttribute("foo", INTEGER);
        FieldAttribute childFoo = fieldAttr("foo", INTEGER);
        var child = relation(childFoo);
        var unionAll = new UnionAll(EMPTY, List.of(child), List.of(unionFoo));

        Project project = as(apply(unionAll), Project.class);
        assertSame(child, project.child());

        List<? extends NamedExpression> projections = project.projections();
        assertEquals(1, projections.size());
        Alias alias = as(projections.get(0), Alias.class);
        // The alias child is the child relation's attribute
        assertSame(childFoo, alias.child());
        // The alias carries the union attribute's ID so upstream references still resolve
        assertEquals(unionFoo.id(), alias.id());
        assertEquals("foo", alias.name());
    }

    /**
     * Multi-column case: all columns correlate correctly and a single {@link Project} is emitted.
     */
    public void testMultiColumnMismatchedIdsInsertsProject() {
        ReferenceAttribute unionFoo = referenceAttribute("foo", INTEGER);
        ReferenceAttribute unionBar = referenceAttribute("bar", KEYWORD);
        FieldAttribute childFoo = fieldAttr("foo", INTEGER);
        FieldAttribute childBar = fieldAttr("bar", KEYWORD);
        var child = relation(childFoo, childBar);
        var unionAll = new UnionAll(EMPTY, List.of(child), List.of(unionFoo, unionBar));

        Project project = as(apply(unionAll), Project.class);
        assertSame(child, project.child());
        assertEquals(2, project.projections().size());

        Alias aliasFoo = as(project.projections().get(0), Alias.class);
        assertEquals(unionFoo.id(), aliasFoo.id());
        assertSame(childFoo, aliasFoo.child());

        Alias aliasBar = as(project.projections().get(1), Alias.class);
        assertEquals(unionBar.id(), aliasBar.id());
        assertSame(childBar, aliasBar.child());
    }

    // ----- helpers -----

    private static FieldAttribute fieldAttr(String name, DataType type) {
        return new FieldAttribute(EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
