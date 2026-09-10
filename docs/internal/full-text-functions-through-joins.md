# Full-text functions above join nodes

## Problem

QSTR and KQL fail with `[QSTR] function cannot be used after LANGUAGE.CODE` when they
appear in an OR expression alongside an IN subquery attribute.

The failure happens at **plan-validation time**, not at execution time.  MATCH, KNN, and
MATCH_PHRASE (on index-mapped fields) already work in exactly these positions because they
use a block-list validation rule that does not include join nodes.  QSTR and KQL use a
stricter allow-list rule, and join nodes are not in that allow-list.

### Validation rules compared

| Function | Rule type | Safe nodes listed |
|---|---|---|
| `qstr`, `kql` | Allow-list (positive) | `Filter`, `OrderBy`, `EsRelation`, `ParameterizedQuery`, `Sample` |
| `match`, `match_phrase`, `knn` | Block-list (negative) | `Limit`, `Aggregate`, `MvExpand`, `Fork`, `LimitBy`, `TopNBy`, `Dedup` |

`MarkJoin` (produced by IN subquery under OR) is absent from both lists.  That means
block-list functions **pass** while allow-list functions **fail** when a `MarkJoin` sits
below the filter.

### Cases that currently fail for QSTR/KQL

| Query shape | Join produced | Stage where failure occurs |
|---|---|---|
| `WHERE key IN (sub) OR qstr("…")` | `MarkJoin` | post-optimisation verification |
| `WHERE (key IN (sub) AND A) OR qstr("…")` | `MarkJoin` | post-optimisation verification |

---

## Why QSTR/KQL can stay above a join: the row-level evaluator

The original assumption that triggered the Option A design ("there is no row-level
evaluation path") is incorrect.  `FullTextFunction.toEvaluator()` is implemented and
returns `LuceneQueryExpressionEvaluator`:

```java
// FullTextFunction.java line 602
@Override
public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
    return new LuceneQueryExpressionEvaluator.Factory(toShardConfigs(toEvaluator.shardContexts()));
}
```

`LuceneQueryExpressionEvaluator` evaluates the full-text query against specific doc IDs
extracted from the `DocVector` on each page using the shard's existing `IndexSearcher`.
It is not a pure row predicate (it does not read a column value) — it re-runs the Lucene
query against a set of doc IDs — but it produces a correct per-row boolean result.

This is the path already taken by `match`, `match_phrase` (index-mapped field), and `knn`
when they appear in an OR with a non-pushable predicate.

### Execution path when QSTR/KQL stays above a MarkJoin

For `WHERE qstr("fox") OR emp_no IN (FROM employees | KEEP emp_no)`:

1. After `InSubqueryResolver`: `Filter[qstr("fox") OR $mark] → MarkJoin → EsRelation`
2. `PushDownAndCombineFilters` cannot push: `qstr OR $mark` is not a left-scoped
   AND-conjunct (the OR references `$mark`, a right-side attribute).
3. `PushFiltersToSource` classifies `translatable(qstr OR $mark)`:
   - `qstr.translatable()` = `YES`
   - `$mark.translatable()` = `NO` (it is a synthetic attribute, not a Lucene predicate)
   - `NO.merge(YES)` = `NO` → entire OR goes to `nonPushable`
4. `EsQueryExec` runs match_all (no Lucene pre-filter).
5. `FilterExec` evaluates `qstr("fox") OR $mark` per page:
   - `$mark`: boolean block from MarkJoin, evaluated directly.
   - `qstr("fox")`: `LuceneQueryExpressionEvaluator` uses the shard's `IndexSearcher` and
     the `DocVector` (preserved through MarkJoin's output pages) to produce a boolean block.
6. Result: correct.  Performance: no Lucene pre-filter (full scan), same trade-off as
   `match OR IN subquery` which already works.

---

## Solution: fix the allow-list validation

No new plan nodes, no new execution infrastructure, no named-query mechanism.
The only change needed is in the validation logic inside `FullTextFunction.java`.

### Phase 1 — Fix the allow-list traversal for QSTR/KQL

**File**: `FullTextFunction.java`, method `checkCommandsBeforeExpression`

The current traversal is:
```java
plan.forEachDown(LogicalPlan.class, lp -> {
    if (commandCheck.test(lp) == false) {
        failures.add(fail(plan, ...));
    }
});
```

`plan.forEachDown` descends into **all** children of every node, including the right
(subquery) child of `AbstractSubqueryJoin`.  That subquery subtree contains `Project`,
`Limit`, and other nodes that are not in the QSTR/KQL allow-list, causing spurious
failures.

The fix has two parts:

**1a.** Add `AbstractSubqueryJoin` to the QSTR/KQL allow-list predicate:

```java
lp -> (lp instanceof Filter
    || lp instanceof OrderBy
    || lp instanceof EsRelation
    || lp instanceof ParameterizedQuery
    || lp instanceof Sample
    || lp instanceof AbstractSubqueryJoin)  // NEW
```

**1b.** Prune the traversal so it does not descend into the right (subquery) child of
`AbstractSubqueryJoin` nodes.  Collect all plan nodes reachable exclusively through the
right side before the traversal, then skip them during the allow-list walk:

```java
Set<LogicalPlan> subqueryDescendants = new HashSet<>();
plan.forEachDown(LogicalPlan.class, p -> {
    if (p instanceof AbstractSubqueryJoin join) {
        join.right().forEachDown(LogicalPlan.class, subqueryDescendants::add);
    }
});

plan.forEachDown(LogicalPlan.class, lp -> {
    if (subqueryDescendants.contains(lp)) {
        return;   // skip nodes reachable only through the subquery side
    }
    if (commandCheck.test(lp) == false) {
        failures.add(fail(plan, ...));
    }
});
```

This guarantees that only the **main-index side** of the join is checked against the
allow-list.  QSTR/KQL query the main index and are always valid above a join as long as
they remain in the left (main) subtree.

---

## Cases resolved by this fix

| Query shape | Before | After |
|---|---|---|
| `WHERE key IN (sub) OR qstr("…")` | fails post-opt verification | passes; QSTR evaluated via `LuceneQueryExpressionEvaluator` |
| `WHERE (key IN (sub) AND A) OR qstr("…")` | fails post-opt verification | passes; same path |
| `WHERE match(…) OR key IN (sub)` | already works | unchanged |
| `WHERE knn(…) OR key IN (sub)` | already works | unchanged |

---

---

## Key classes for reference

| Class | Path | Relevance |
|---|---|---|
| `FullTextFunction` | `…/expression/function/fulltext/FullTextFunction.java` | `checkCommandsBeforeExpression`, `hasSubqueryInChildrenPlans`, `toEvaluator()` (returns `LuceneQueryExpressionEvaluator`) |
| `LuceneQueryExpressionEvaluator` | `…/compute/lucene/query/LuceneQueryExpressionEvaluator.java` | Evaluates the full-text query against specific doc IDs via `IndexSearcher`; the existing per-row execution path |
| `LuceneQueryEvaluator` | `…/compute/lucene/query/LuceneQueryEvaluator.java` | Base class; `executeQuery(Page page)` reads `DocVector`, builds Lucene `Weight`, runs `BulkScorer` or `Scorer` per segment |
| `TranslationAware` | `…/capabilities/TranslationAware.java` | `Translatable` enum; `NO.merge(YES)` = `NO` explains why the whole OR condition is non-pushable |
| `PushFiltersToSource` | `…/optimizer/rules/physical/local/PushFiltersToSource.java` | `classifyFilters()` splits by AND only; an OR with a NO-translatable side stays in `FilterExec` |
| `PushDownAndCombineFilters` | `…/optimizer/rules/logical/PushDownAndCombineFilters.java` | Pushes left-scoped AND-conjuncts past joins; does not push OR expressions |
| `InSubqueryResolver` | `…/analysis/InSubqueryResolver.java` | Rewrites IN subqueries to SemiJoin / AntiJoin / MarkJoin |
