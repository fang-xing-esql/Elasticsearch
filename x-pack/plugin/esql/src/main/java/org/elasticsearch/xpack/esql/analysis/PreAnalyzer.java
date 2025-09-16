/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is part of the planner.  Acts somewhat like a linker, to find the indices and enrich policies referenced by the query.
 */
public class PreAnalyzer {

    public record PreAnalysis(IndexMode indexMode, IndexPattern index, List<Enrich> enriches, List<IndexPattern> lookupIndices,
                              Set<IndexPattern> subqueryIndices) {
        public static final PreAnalysis EMPTY = new PreAnalysis(null, null, List.of(), List.of(), Set.of());
    }

    public PreAnalysis preAnalyze(LogicalPlan plan) {
        if (plan.analyzed()) {
            return PreAnalysis.EMPTY;
        }

        return doPreAnalyze(plan);
    }

    protected PreAnalysis doPreAnalyze(LogicalPlan plan) {

        Holder<IndexMode> indexMode = new Holder<>();
        Holder<IndexPattern> index = new Holder<>();

        List<Enrich> unresolvedEnriches = new ArrayList<>();
        List<IndexPattern> lookupIndices = new ArrayList<>();
        Set<IndexPattern> subqueryIndices = new HashSet<>();

        plan.forEachUp(UnresolvedRelation.class, p -> {
            if (p.indexMode() == IndexMode.LOOKUP) { // index on the RHS of lookup join
                lookupIndices.add(p.indexPattern());
            } else if (indexMode.get() == null || indexMode.get() == p.indexMode()) {
                indexMode.set(p.indexMode());
                index.setIfAbsent(p.indexPattern());  // only the index pattern from main query is set
            } else { // mixed index modes(from and ts) in the same query is not allowed except for lookup joins
                throw new IllegalStateException("index mode is already set");
            }
        });

        plan.forEachUp(Enrich.class, unresolvedEnriches::add);

        // walk through the plan to find indices referenced by the FROM command in subqueries
        plan.forEachUp(UnionAll.class, unionAll -> {
            unionAll.children().forEach(child -> {
                if (child instanceof UnresolvedRelation unresolvedRelation) {
                    collectSubqueryIndexPattern(unresolvedRelation, subqueryIndices, index.get());
                }
                // check each subquery leg and collect index patterns from each subquery
                if (child instanceof Subquery subquery && subquery.preAnalyzed()==false) {
                    subquery.forEachUp(UnresolvedRelation.class, unresolvedRelation -> {
                       collectSubqueryIndexPattern(unresolvedRelation, subqueryIndices, index.get());
                    });
                }
            });
            unionAll.forEachUp(LogicalPlan::setPreAnalyzed);
        });

        // mark plan as preAnalyzed (if it were marked, there would be no analysis)
        plan.forEachUp(LogicalPlan::setPreAnalyzed);

        return new PreAnalysis(indexMode.get(), index.get(), unresolvedEnriches, lookupIndices, subqueryIndices);
    }

    private void collectSubqueryIndexPattern(UnresolvedRelation relation,
                                     Set<IndexPattern> subqueryIndices,
                                     IndexPattern mainIndexPattern) {
        if (relation.preAnalyzed()) {
            return;
        }

        IndexPattern pattern = relation.indexPattern();
        boolean isLookup = relation.indexMode() == IndexMode.LOOKUP;
        boolean isMainIndexPattern = pattern == mainIndexPattern;

        if (isLookup || isMainIndexPattern) {
            return;
        }
        subqueryIndices.add(pattern);
    }
}
