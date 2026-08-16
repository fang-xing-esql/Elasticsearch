/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Context threaded through the random query generator.
 */
public final class GenerationContext {

    /**
     * Maximum nesting depth for IN subqueries. Beyond this depth, IN subquery generation is suppressed
     * to keep queries finite and avoid pathological nesting.
     */
    public static final int MAX_IN_SUBQUERY_NESTING_DEPTH = 2;

    private final int subqueryDepth;
    private final boolean withinFromSubquery;
    /**
     * Shared mutable flag across the entire query tree (including subqueries). ES|QL only allows one FORK
     * per query tree, so once any generator sets this flag, all subsequent ForkGenerator calls bail out.
     */
    private final AtomicBoolean hasFork;
    /**
     * Shared mutable flag across the entire query tree. Set to {@code true} the first time an
     * {@code IN (subquery)} predicate is successfully generated. When {@link GenerativeFeature#IN_SUBQUERY}
     * is enabled, the probability gate in {@code maybeInSubqueryBooleanExpression} is bypassed until
     * this flag is set, so the first suitable boolean-expression position attempts generation.
     */
    private final AtomicBoolean hasGeneratedInSubquery;
    private final Set<GenerativeFeature> features;

    private GenerationContext(
        int subqueryDepth,
        boolean withinFromSubquery,
        AtomicBoolean hasFork,
        AtomicBoolean hasGeneratedInSubquery,
        Set<GenerativeFeature> features
    ) {
        this.subqueryDepth = subqueryDepth;
        this.withinFromSubquery = withinFromSubquery;
        this.hasFork = hasFork;
        this.hasGeneratedInSubquery = hasGeneratedInSubquery;
        this.features = features;
    }

    /**
     * Root context for a top-level query with the given opt-in features.
     */
    public static GenerationContext root(Set<GenerativeFeature> features) {
        return new GenerationContext(0, false, new AtomicBoolean(false), new AtomicBoolean(false), features);
    }

    /**
     * How deeply nested the current generation is inside subqueries.
     * E.g. 0 for the root query, 1+ inside a subquery.
     */
    public int subqueryDepth() {
        return subqueryDepth;
    }

    /**
     * Returns {@code true} if generation is happening inside a subquery body.
     */
    public boolean isWithinASubquery() {
        return subqueryDepth > 0;
    }

    /**
     * Returns {@code true} if generation is happening inside a FROM subquery body.
     * FORK is forbidden inside FROM subqueries but allowed inside WHERE IN subquery bodies.
     */
    public boolean withinFromSubquery() {
        return withinFromSubquery;
    }

    /**
     * Returns {@code true} if a FORK has already been generated anywhere in this query tree (including subqueries).
     * ES|QL only allows one FORK per query tree.
     */
    public boolean hasFork() {
        return hasFork.get();
    }

    /**
     * Marks that a FORK has been generated in this query tree. All derived contexts share the same flag,
     * so this is visible to any generator holding a context derived from the same root.
     */
    public void setHasFork() {
        hasFork.set(true);
    }

    /**
     * Returns {@code true} if an {@code IN (subquery)} predicate has already been generated anywhere
     * in this query tree. Used to bypass the probability gate until the first suitable position succeeds.
     */
    public boolean hasGeneratedInSubquery() {
        return hasGeneratedInSubquery.get();
    }

    /**
     * Marks that an IN subquery has been generated. All derived contexts share the same flag.
     */
    public void setHasGeneratedInSubquery() {
        hasGeneratedInSubquery.set(true);
    }

    /**
     * Returns {@code true} if the given feature is enabled in this context.
     */
    public boolean isFeatureEnabled(GenerativeFeature feature) {
        return features.contains(feature);
    }

    /**
     * Returns a copy of this context with the given subquery nesting depth. Preserves {@code withinFromSubquery}
     * and shares the same {@code hasFork} and {@code hasGeneratedInSubquery} references so flags span the entire
     * query tree.
     */
    public GenerationContext withSubqueryDepth(int subqueryDepth) {
        return new GenerationContext(subqueryDepth, withinFromSubquery, hasFork, hasGeneratedInSubquery, features);
    }

    /**
     * Returns a copy of this context marked as being inside a FROM subquery body.
     * Shares the same {@code hasFork} and {@code hasGeneratedInSubquery} references.
     */
    public GenerationContext withInFromSubquery() {
        return new GenerationContext(subqueryDepth, true, hasFork, hasGeneratedInSubquery, features);
    }
}
