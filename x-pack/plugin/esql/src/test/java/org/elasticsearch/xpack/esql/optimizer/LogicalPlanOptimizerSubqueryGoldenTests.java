/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.EnumSet;

/**
 * Captures the analyzed and logically-optimized plans for nested subquery scenarios.
 */
public class LogicalPlanOptimizerSubqueryGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public LogicalPlanOptimizerSubqueryGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    public void testSingleBranchUnionAllIsFlattened() {
        runGoldenTest("""
            FROM employees, (FROM languages)
            | WHERE emp_no > 10000
            """, STAGES);
    }

    public void testMultipleNestedSingleBranchUnionAllsAreFlattened() {
        runGoldenTest("""
            FROM languages,
                 (FROM languages,
                      (FROM languages, (FROM employees | WHERE salary > 0)))
            | WHERE emp_no > 10000
            """, STAGES);
    }

    public void testNestedSubqueries() {
        runGoldenTest("""
            FROM employees, (FROM employees, (FROM employees | WHERE salary > 0))
            | WHERE emp_no > 10000
            """, STAGES);
    }

    public void testNestedSubqueriesWithUnionAllOnTopOfMultipleUnionAlls() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees, (FROM languages | WHERE language_code > 0)),
                 (FROM languages, (FROM employees | WHERE salary > 0))
            """, STAGES);
    }

    public void testNestedSubqueriesWithUnionAllOnTopOfMultipleUnionAllsWithPredicatePushdown() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees, (FROM languages | WHERE language_code > 0)),
                 (FROM languages, (FROM employees | WHERE salary > 0))
            | WHERE emp_no > 10000
            """, STAGES);
    }
}
