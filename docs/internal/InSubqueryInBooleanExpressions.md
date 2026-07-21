IN Subquery in All Boolean Expression Positions
=================================================

## Background

`InSubquery` (`x IN (FROM sub)`) is already a fully-typed boolean expression:
`InSubquery.dataType()` returns `DataType.BOOLEAN`. The grammar rule
`booleanExpression → valueExpression (NOT)? IN subquery` is part of the top-level
expression grammar shared by every ES|QL command. Syntactically an
`EVAL b = x IN (FROM sub)` or `SORT x IN (FROM sub)` already parses without error —
the `InSubquery` node is placed inside the enclosing plan node's expression tree.
The block is purely semantic: `InSubqueryResolver` only processes `Filter` (WHERE)
plan nodes and `verify()` rejects `InSubquery` anywhere else.

**Goal**: lift that semantic restriction so that `InSubquery` is allowed wherever
any other boolean expression is allowed:

| Context | Status |
|---|---|
| `WHERE x IN (sub)` | Already supported (SemiJoin/AntiJoin/MarkJoin) |
| `EVAL b = x IN (sub)` | **New** |
| `EVAL b = CASE(WHEN x IN (sub) THEN ...)` | **New** |
| `EVAL b = fn(x IN (sub))` (boolean fn arg) | **New** |
| `WHERE CASE(WHEN x IN (sub) THEN true ...)` | **New** (extends existing WHERE) |
| `STATS agg() WHERE x IN (sub)` | **New** |
| `INLINESTATS agg() WHERE x IN (sub)` | Future work |
| `SORT x IN (sub)` | **New** |
| `STATS BY x IN (sub)` | **New** |
| `LIMIT N BY x IN (sub)` | **New** |

No grammar, `LogicalPlanBuilder`, or `ExpressionBuilder` changes are needed.
The grammar already produces `InSubquery` nodes in the correct positions; the parser
rejects nothing. All enforcement is semantic.

---

## Current Pipeline

```
parse
  → InSubqueryResolver.hasInSubqueryInFilter      ← telemetry check (Filter only)
  → ViewResolver.replaceViews
      case Filter → InSubqueryResolver.resolveInSubqueryInFilter
  → InSubqueryResolver.verify                     ← rejects InSubquery in non-Filter
  → ViewCompaction.compact
  → PreAnalyzer / FieldNameUtils                  ← runs AFTER InSubqueryResolver;
  → Analyzer (ResolveRefs fills join fields)         sees MarkJoin, not InSubquery
```

---

## Key Data Structures

### `InSubquery` (parse-time only)

- `dataType()` → `BOOLEAN`; `nullable()` → `Nullability.UNKNOWN` (three-valued)
- Children: `[value]` (the LHS); `subquery` stored as a separate `LogicalPlan` field
- Not serializable (`writeTo` throws) — must be fully resolved before the plan leaves
  the analysis phase

### `FilteredExpression` (STATS WHERE parse-time wrapper)

`STATS COUNT(*) WHERE cond` creates `FilteredExpression(delegate=COUNT(*), filter=cond)`
at parse time (see `ExpressionBuilder.visitAggField`).
- It is transient: the optimizer rule `SubstituteFilteredExpression` fuses it into the
  `AggregateFunction` via `AggregateFunction.withFilter()` after analysis.
- Its children are `[delegate, filter]` in that order.
- `Aggregate.checkInvalidNamedExpressionUsage` calls `checkFilterConditionDataType`
  on the filter — accepts BOOLEAN or NULL.

### MarkJoin rewriting pattern

`rewriteAsMarkJoin(InSubquery)` allocates a synthetic
`ReferenceAttribute(name="$$in_subquery_mark$…", type=BOOLEAN, synthetic=true)`,
records a `MarkJoinSpec`, and returns the attribute as a replacement expression.
The MarkJoin is stacked below the plan node whose expressions were rewritten, so the
synthetic mark attribute is in scope when the plan node evaluates its expressions.
The `synthetic=true` flag causes `planWithoutSyntheticAttributes` to drop the mark
attribute from the plan's visible output schema.

---

## Post-Change Plan Shapes

### EVAL — direct boolean value

```
Query:  EVAL b = x IN (FROM sub)
Before: Eval[b = InSubquery(x, sub)]
        └── Source
After:  Eval[b = $$mark]
        └── MarkJoin[left=x, right=sub, mark=$$mark]
           ├── Source
           └── subquery plan
```

### EVAL — compound boolean / function argument

```
Query:  EVAL b = (x IN (sub1)) AND (y IN (sub2))
After:  Eval[b = And($$mark1, $$mark2)]
        └── MarkJoin[y, sub2, $$mark2]
           └── MarkJoin[x, sub1, $$mark1]
              └── Source
```

### EVAL — InSubquery inside CASE WHEN

```
Query:  EVAL label = CASE(WHEN x IN (sub) THEN "yes" ELSE "no" END)
After:  Eval[label = Case($$mark, "yes", "no")]
        └── MarkJoin[x, sub, $$mark]
           └── Source
```

### WHERE — InSubquery inside CASE WHEN (extension to existing support)

```
Query:  WHERE CASE(WHEN x IN (sub) THEN true ELSE false END)
After:  Filter[Case($$mark, true, false)]
        └── MarkJoin[x, sub, $$mark]
           └── Source
```

### STATS WHERE

```
Query:  STATS cnt = COUNT(*) WHERE emp_no IN (FROM sub)
Before: Aggregate[cnt = FilteredExpression(COUNT(*), InSubquery(emp_no, sub))]
        └── Source
After:  Aggregate[cnt = FilteredExpression(COUNT(*), $$mark)]
        └── MarkJoin[emp_no, sub, $$mark]
           └── Source
```
After `SubstituteFilteredExpression` optimizer:
```
        Aggregate[cnt = Count(*, filter=$$mark)]
        └── MarkJoin[emp_no, sub, $$mark]
           └── Source
```

The aggregate counts only rows where `$$mark` is true (i.e. where `emp_no` is in the
subquery result set). Three-valued logic: the mark is `Nullability.TRUE`, so rows
where the LHS had no match produce `false` (not null), consistent with `IN` semantics.

### STATS BY

```
Query:  STATS cnt = COUNT(*) BY emp_no IN (FROM sub)
Before: Aggregate[groupings=[Alias("emp_no IN (FROM sub)", InSubquery(emp_no, sub))]]
        └── Source
After:  Aggregate[groupings=[Alias("emp_no IN (FROM sub)", $$mark)]]
        └── MarkJoin[left=emp_no, right=sub, mark=$$mark]
           └── Source
```

The grouping Alias wraps the `$$mark` attribute instead of the raw `InSubquery`. The
auto-appended `ReferenceAttribute` in `aggregates` (created by `ParserUtils.buildStats`
via `Expressions.attribute(groupingAlias)`) shares the same `NameId` as the grouping
Alias, so `ResolveRefs` can match them correctly after rewriting. The `$$mark`
attribute is `synthetic=true`; `planWithoutSyntheticAttributes` in the Analyzer strips
it from the final visible output, but the Aggregate's own output (derived from
`aggregates`) still exposes the grouping column via its `ReferenceAttribute`.

The `TimeSeriesAggregate` subclass guard (`aggregate.getClass() != Aggregate.class`)
ensures this rewrite does not attempt to reconstruct a `TimeSeriesAggregate` using
the base `Aggregate` constructor.

### SORT

```
Query:  FROM idx | SORT emp_no IN (FROM sub)
Before: OrderBy[Order(InSubquery(emp_no, sub), ASC)]
        └── Source
After:  OrderBy[Order($$mark, ASC)]
        └── MarkJoin[emp_no, sub, $$mark]
           └── Source
```

Sorts rows by their `IN` membership: rows where `emp_no` is in the subquery come
first (true sorts before false in ASC order). `DataType.isSortable(BOOLEAN)` is true,
so `OrderBy.postAnalysisVerification` accepts `$$mark` without change.

### LIMIT BY

```
Query:  FROM idx | SORT emp_no | LIMIT 3 BY emp_no IN (FROM sub)
Before: LimitBy[3, groupings=[InSubquery(emp_no, sub)]]
        └── OrderBy[emp_no ASC]
           └── Source
After:  LimitBy[3, groupings=[$$mark]]
        └── MarkJoin[left=emp_no, right=sub, mark=$$mark]
           └── OrderBy[emp_no ASC]
              └── Source
```

`LIMIT N BY x IN (sub)` groups rows by whether `x` is in the subquery result set
and keeps at most N rows per group. Since the grouping key is boolean-valued there
are at most two groups (true, false), so this is equivalent to "keep at most N rows
from each membership bucket." The `$$mark` attribute is `synthetic=true`; it drives
the grouping but is not exposed in the output (the output schema of `LimitBy` is
identical to that of its child).

**Key differences from STATS BY:**

- `LimitBy.groupings` is `List<Expression>` with **no Alias wrapping** — the parser
  passes the raw `booleanExpression()` result directly. So when an `InSubquery` in a
  grouping key is replaced by `$$mark`, no Alias needs to be created or updated.
- There is no parallel `aggregates` list mirroring the groupings (unlike
  `Aggregate`), so there is no linked `ReferenceAttribute` whose `NameId` must be
  kept in sync.
- No subclass guard is needed: `LimitBy` has no known subclasses, so the base
  constructor can always be used for reconstruction.

**`postAnalysisVerification` compatibility:** `LimitBy.postAnalysisVerification`
calls `Aggregate.checkUnsupportedGroupingType`. Verify before implementing that
`BOOLEAN` is not rejected there; since `STATS BY b` (where `b` is already boolean)
already works today, `BOOLEAN` should pass.

---

## Required Changes

### 1. `InSubqueryResolver.java`

#### 1a. New private method: `rewriteAllInSubqueries`

A fully-recursive variant of the existing `rewriteOrContextInSubqueries`. Where the
existing method stops recursing when it hits a non-And/Or/Not expression (intentionally
conservative for the WHERE case), the new method recurses through **all** expression
nodes. Safe for EVAL/SORT/STATS-filter contexts because a misplaced `InSubquery` in
a non-boolean position is caught by the type-checker after the mark attribute
substitution.

```java
private static Expression rewriteAllInSubqueries(
    Expression expr, List<MarkJoinSpec> joins, List<Alias> syntheticEvals
) {
    if (expr instanceof InSubquery inSubquery) {
        return rewriteAsMarkJoin(inSubquery, joins, syntheticEvals);
    }
    List<Expression> children = expr.children();
    List<Expression> rewritten = new ArrayList<>(children.size());
    boolean changed = false;
    for (Expression child : children) {
        Expression r = rewriteAllInSubqueries(child, joins, syntheticEvals);
        rewritten.add(r);
        changed |= r != child;
    }
    return changed ? expr.replaceChildren(rewritten) : expr;
}
```

#### 1b. New public method: `resolveInSubqueryInEval`

Public so `ViewResolver` can call it directly (mirroring `resolveInSubqueryInFilter`).

```java
public static LogicalPlan resolveInSubqueryInEval(Eval eval) {
    List<MarkJoinSpec> markJoins = new ArrayList<>();
    List<Alias> syntheticEvals = new ArrayList<>();

    List<Alias> rewrittenFields = new ArrayList<>(eval.fields().size());
    boolean changed = false;
    for (Alias alias : eval.fields()) {
        Expression r = rewriteAllInSubqueries(alias.child(), markJoins, syntheticEvals);
        Alias rewrittenAlias = r == alias.child() ? alias
            : (Alias) alias.replaceChildren(List.of(r));
        rewrittenFields.add(rewrittenAlias);
        changed |= rewrittenAlias != alias;
    }

    if (markJoins.isEmpty()) return eval;

    LogicalPlan current = eval.child();
    if (syntheticEvals.isEmpty() == false) {
        current = new Eval(eval.source(), current, syntheticEvals);
    }
    for (MarkJoinSpec mj : markJoins) {
        current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
    }
    return new Eval(eval.source(), current, rewrittenFields);
}
```

#### 1c. New public method: `resolveInSubqueryInAggregate`

Targets two places inside an `Aggregate`:

1. **STATS WHERE** — the `FilteredExpression.filter()` inside each aggregate expression.
   Only the filter half is rewritten; the delegate (aggregate function itself) is left
   untouched.
2. **STATS BY** — the grouping expressions in `aggregate.groupings()`. Each grouping
   expression is passed to `rewriteAllInSubqueries` so that an `InSubquery` inside a
   grouping key is replaced by a `$$mark` attribute. A `TimeSeriesAggregate` guard
   (`aggregate.getClass() != Aggregate.class`) prevents incorrect reconstruction using
   the base `Aggregate` constructor.

```java
public static LogicalPlan resolveInSubqueryInAggregate(Aggregate aggregate) {
    List<MarkJoinSpec> markJoins = new ArrayList<>();
    List<Alias> syntheticEvals = new ArrayList<>();

    List<NamedExpression> rewrittenAggregates = new ArrayList<>();
    boolean changed = false;
    for (NamedExpression agg : aggregate.aggregates()) {
        NamedExpression rewritten = (NamedExpression) agg.transformDown(
            FilteredExpression.class, fe -> {
                Expression rewrittenFilter =
                    rewriteAllInSubqueries(fe.filter(), markJoins, syntheticEvals);
                if (rewrittenFilter == fe.filter()) return fe;
                // children order is [delegate, filter]
                return fe.replaceChildren(List.of(fe.delegate(), rewrittenFilter));
            }
        );
        rewrittenAggregates.add(rewritten);
        changed |= rewritten != agg;
    }

    if (markJoins.isEmpty()) return aggregate;

    LogicalPlan current = aggregate.child();
    if (syntheticEvals.isEmpty() == false) {
        current = new Eval(aggregate.source(), current, syntheticEvals);
    }
    for (MarkJoinSpec mj : markJoins) {
        current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
    }
    // Verify exact constructor/factory; options include:
    //   new Aggregate(source, newChild, aggregatingType, rewrittenAggregates, groupings)
    // or aggregate.with(rewrittenAggregates, aggregate.groupings(), newChild)
    return new Aggregate(
        aggregate.source(), current, aggregate.aggregatingType(),
        rewrittenAggregates, aggregate.groupings()
    );
}
```

**`InlineStats`**: `InlineStats` wraps an `Aggregate` as its `UnaryPlan` child (the
inner `Aggregate` is accessible via `InlineStats.aggregate()`). Because
`transformUp(Aggregate.class, handler)` processes children before parents, a naive
pass would also rewrite the inner `Aggregate`, incorrectly resolving `InSubquery` in
`INLINESTATS`. To prevent this, collect all inner `Aggregate` instances from
`InlineStats` nodes (by identity via `IdentityHashMap`) before running the
`transformUp` pass and skip those instances in the handler. `InlineStats` support is
deferred; only the exclusion guard is needed now.

#### 1d. New public method: `resolveInSubqueryInLimitBy`

Public so `ViewResolver` can call it directly.

```java
public static LogicalPlan resolveInSubqueryInLimitBy(LimitBy limitBy) {
    List<MarkJoinSpec> markJoins = new ArrayList<>();
    List<Alias> syntheticEvals = new ArrayList<>();

    List<Expression> rewrittenGroupings = new ArrayList<>(limitBy.groupings().size());
    for (Expression grouping : limitBy.groupings()) {
        rewrittenGroupings.add(rewriteAllInSubqueries(grouping, markJoins, syntheticEvals));
    }

    if (markJoins.isEmpty()) return limitBy;

    LogicalPlan current = limitBy.child();
    if (syntheticEvals.isEmpty() == false) {
        current = new Eval(limitBy.source(), current, syntheticEvals);
    }
    for (MarkJoinSpec mj : markJoins) {
        current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
    }
    return new LimitBy(limitBy.source(), limitBy.limitPerGroup(), current, rewrittenGroupings);
}
```

No Alias wrapping is needed: `LimitBy.groupings` stores raw expressions and the
resolver replaces each `InSubquery` directly with the `$$mark` attribute. The
`limitPerGroup` expression (the integer literal N) is passed through unchanged.
Reconstruction uses the four-argument constructor:
`LimitBy(Source, Expression limitPerGroup, LogicalPlan child, List<Expression> groupings)`.

**`Aggregate.checkInvalidNamedExpressionUsage` compatibility**: After rewriting,
`FilteredExpression.filter()` holds `$$mark` (a `ReferenceAttribute` of type
BOOLEAN). The verification at `Aggregate.postAnalysisVerification` calls
`checkFilterConditionDataType` on this expression — `BOOLEAN` passes. The
`forEachDown` check for nested `AggregateFunction` / `GroupingFunction` finds
nothing inside a `ReferenceAttribute`. No changes to verification needed.

#### 1e. New public method: `resolveInSubqueryInOrderBy`

```java
public static LogicalPlan resolveInSubqueryInOrderBy(OrderBy orderBy) {
    List<MarkJoinSpec> markJoins = new ArrayList<>();
    List<Alias> syntheticEvals = new ArrayList<>();

    List<Order> rewrittenOrders = new ArrayList<>(orderBy.order().size());
    boolean changed = false;
    for (Order order : orderBy.order()) {
        Expression r = rewriteAllInSubqueries(order.child(), markJoins, syntheticEvals);
        Order rewrittenOrder = r == order.child() ? order
            : new Order(order.source(), r, order.direction(), order.nullsPosition());
        rewrittenOrders.add(rewrittenOrder);
        changed |= rewrittenOrder != order;
    }

    if (markJoins.isEmpty()) return orderBy;

    LogicalPlan current = orderBy.child();
    if (syntheticEvals.isEmpty() == false) {
        current = new Eval(orderBy.source(), current, syntheticEvals);
    }
    for (MarkJoinSpec mj : markJoins) {
        current = new MarkJoin(mj.source(), current, mj.subquery(), mj.config(), mj.markAttribute());
    }
    return new OrderBy(orderBy.source(), current, rewrittenOrders);
}
```

`OrderBy.postAnalysisVerification` checks `DataType.isSortable(order.dataType())`
only. `$$mark` has type `BOOLEAN`, which is sortable; no verification changes needed.

#### 1f. Extend `rewriteOrContextInSubqueries` to handle `Case`

The existing method walks `And`/`Or`/`Not` but stops at all other expressions.
Extend it to also recurse into `Case` WHEN conditions so that
`WHERE CASE(WHEN x IN (sub) THEN true ELSE false END)` is rewritten in the WHERE
context.

`Case` stores conditions and results as interleaved children. Verify the exact child
order from `Case.java` before implementing; the pattern is roughly:
`[cond1, result1, cond2, result2, ..., elseResult]` where even-indexed children
(except the last) are conditions. Recurse only into condition positions using
`rewriteOrContextInSubqueries`; leave result/else positions unchanged.

```java
if (expr instanceof Case caseExpr) {
    List<Expression> children = caseExpr.children();
    List<Expression> rewritten = new ArrayList<>(children.size());
    boolean caseChanged = false;
    // Verify exact interleaving against Case.java before using this index pattern
    for (int i = 0; i < children.size(); i++) {
        boolean isCondition = (i % 2 == 0) && (i < children.size() - 1);
        Expression child = children.get(i);
        Expression r = isCondition
            ? rewriteOrContextInSubqueries(child, joins, syntheticEvals)
            : child;
        rewritten.add(r);
        caseChanged |= r != child;
    }
    return caseChanged ? caseExpr.replaceChildren(rewritten) : caseExpr;
}
```

#### 1g. Update `resolveInSubqueries` dispatcher

```java
// Before:
private static LogicalPlan resolveInSubqueries(LogicalPlan plan) {
    return plan.transformUp(Filter.class, InSubqueryResolver::resolveInSubqueryInFilter);
}

// After:
private static LogicalPlan resolveInSubqueries(LogicalPlan plan) {
    LogicalPlan afterFilterPass = plan.transformUp(Filter.class, InSubqueryResolver::resolveInSubqueryInFilter);
    // Guard: collect InlineStats's inner Aggregates by identity so the Aggregate pass skips them.
    Set<Aggregate> inlineStatsAggregates = Collections.newSetFromMap(new IdentityHashMap<>());
    afterFilterPass.forEachDown(InlineStats.class, ils -> inlineStatsAggregates.add(ils.aggregate()));
    LogicalPlan afterAggPass = afterFilterPass.transformUp(
        Aggregate.class,
        agg -> inlineStatsAggregates.contains(agg) ? agg : resolveInSubqueryInAggregate(agg)
    );
    LogicalPlan afterLimitByPass = afterAggPass.transformUp(LimitBy.class,
        InSubqueryResolver::resolveInSubqueryInLimitBy);
    return afterLimitByPass.transformUp(p -> switch (p) {
        case Eval eval       -> resolveInSubqueryInEval(eval);
        case OrderBy orderBy -> resolveInSubqueryInOrderBy(orderBy);
        default              -> p;
    });
}
```

The multi-pass structure keeps each handler focused: Filter and Aggregate need
special ordering constraints (InlineStats guard), while Eval and OrderBy can share
a single `transformUp` pass. Add `InlineStats` to its own pass when its handler is
implemented (after verifying `FilteredExpression` usage and `InlineStats` constructor
signature).

#### 1h. Update `resolveNestedInSubqueries`

Nested subquery plans may themselves contain any of the newly-supported plan nodes.
Apply the same structure, including the `InlineStats` guard for the Aggregate pass.

```java
private static LogicalPlan resolveNestedInSubqueries(LogicalPlan subqueryPlan) {
    LogicalPlan afterFilterPass = subqueryPlan.transformUp(Filter.class, InSubqueryResolver::resolveInSubqueryInFilter);
    Set<Aggregate> inlineStatsAggregates = Collections.newSetFromMap(new IdentityHashMap<>());
    afterFilterPass.forEachDown(InlineStats.class, ils -> inlineStatsAggregates.add(ils.aggregate()));
    LogicalPlan afterAggPass = afterFilterPass.transformUp(
        Aggregate.class,
        agg -> inlineStatsAggregates.contains(agg) ? agg : resolveInSubqueryInAggregate(agg)
    );
    LogicalPlan afterLimitByPass = afterAggPass.transformUp(LimitBy.class,
        InSubqueryResolver::resolveInSubqueryInLimitBy);
    return afterLimitByPass.transformUp(p -> switch (p) {
        case Eval eval       -> resolveInSubqueryInEval(eval);
        case OrderBy orderBy -> resolveInSubqueryInOrderBy(orderBy);
        default              -> p;
    });
}
```

#### 1i. Generalize `hasInSubqueryInFilter` → `hasInSubquery`

```java
// New general-purpose check used by ViewResolver early-return guard and telemetry:
public static boolean hasInSubquery(LogicalPlan plan) {
    return plan.anyMatch(p -> {
        if (p instanceof Filter filter) {
            return filter.condition().anyMatch(e -> e instanceof InSubquery);
        }
        if (p instanceof Aggregate agg) {
            if (agg.aggregates().stream().anyMatch(a -> a.anyMatch(e -> e instanceof InSubquery))) return true;
            return agg.groupings().stream().anyMatch(g -> g.anyMatch(e -> e instanceof InSubquery));
        }
        if (p instanceof LimitBy limitBy) {
            return limitBy.groupings().stream().anyMatch(g -> g.anyMatch(e -> e instanceof InSubquery));
        }
        if (p instanceof Eval eval) {
            return eval.fields().stream().anyMatch(f -> f.anyMatch(e -> e instanceof InSubquery));
        }
        if (p instanceof OrderBy orderBy) {
            return orderBy.order().stream().anyMatch(o -> o.anyMatch(e -> e instanceof InSubquery));
        }
        return false;
    });
}

// Preserve hasInSubqueryInFilter only if the WHERE telemetry counter needs
// to be specifically restricted to Filter nodes; otherwise replace all callers.
```

#### 1j. Update `checkInSubqueryUsage` in `verify`

Remove the blanket rejection of `InSubquery` in the newly-supported plan node
types. After the fix, any `InSubquery` surviving in an `Eval`, `Aggregate`, or
`OrderBy` after the resolver ran is a resolver bug — the serialization guard
(`InSubquery.writeTo` throws `UnsupportedOperationException`) will catch it if it
somehow reaches execution.

```java
private static void checkInSubqueryUsage(LogicalPlan plan, Failures failures) {
    plan.forEachDown(p -> {
        if (p instanceof Filter filter) {
            checkInFilterCondition(filter, filter.condition(), null, failures);
        } else if (p instanceof Aggregate agg) {
            // Surviving InSubquery means the resolver could not rewrite it (e.g. complex LHS).
            for (Expression aggExpr : agg.aggregates()) {
                aggExpr.forEachDown(InSubquery.class, inSub ->
                    failures.add(fail(inSub, "Complicated IN subquery is not yet supported in [{}]", agg.sourceText())));
            }
            for (Expression grouping : agg.groupings()) {
                grouping.forEachDown(InSubquery.class, inSub ->
                    failures.add(fail(inSub, "Complicated IN subquery is not yet supported in [{}]", agg.sourceText())));
            }
        } else if (p instanceof LimitBy limitBy) {
            // Surviving InSubquery means the resolver could not rewrite it (e.g. complex LHS).
            for (Expression grouping : limitBy.groupings()) {
                grouping.forEachDown(InSubquery.class, inSub ->
                    failures.add(fail(inSub, "Complicated IN subquery is not yet supported in [{}]", limitBy.sourceText())));
            }
        } else if (p instanceof Eval || p instanceof OrderBy) {
            // Supported — surviving InSubquery here is a resolver bug, not a user error.
        } else {
            p.forEachExpression(
                InSubquery.class,
                inSub -> failures.add(fail(inSub, "IN subquery is not supported in [{}]", p.sourceText()))
            );
        }
    });
}
```

Add `InlineStats` to the allowed set when its handler is implemented.

#### 1k. Update class Javadoc

Update the class-level Javadoc to describe all supported contexts (Filter, Eval,
Aggregate, OrderBy) and the `rewriteAllInSubqueries` vs `rewriteOrContextInSubqueries`
distinction.

---

### 2. `ViewResolver.java`

#### 2a. Add cases to the `replaceViews` switch

The switch in the private `replaceViews` method currently has a `Filter` case.
Add parallel cases for the newly-supported plan node types:

```java
case Eval eval -> {
    LogicalPlan resolved = InSubqueryResolver.resolveInSubqueryInEval(eval);
    if (resolved == eval) {
        planListener.onResponse(eval);
    } else {
        hasInSubquery.set(true);
        replaceViews(resolved, projectRouting, parser, seenInner,
            viewQueries, hasInSubquery, depth,
            planListener.delegateFailureAndWrap((l, result) -> {
                result.forEachDown(resolvedPlans::add);
                l.onResponse(result);
            }));
    }
}
case Aggregate agg -> {
    LogicalPlan resolved = InSubqueryResolver.resolveInSubqueryInAggregate(agg);
    if (resolved == agg) {
        planListener.onResponse(agg);
    } else {
        hasInSubquery.set(true);
        replaceViews(resolved, projectRouting, parser, seenInner,
            viewQueries, hasInSubquery, depth,
            planListener.delegateFailureAndWrap((l, result) -> {
                result.forEachDown(resolvedPlans::add);
                l.onResponse(result);
            }));
    }
}
case LimitBy limitBy -> {
    LogicalPlan resolved = InSubqueryResolver.resolveInSubqueryInLimitBy(limitBy);
    if (resolved == limitBy) {
        planListener.onResponse(limitBy);
    } else {
        hasInSubquery.set(true);
        replaceViews(resolved, projectRouting, parser, seenInner,
            viewQueries, hasInSubquery, depth,
            planListener.delegateFailureAndWrap((l, result) -> {
                result.forEachDown(resolvedPlans::add);
                l.onResponse(result);
            }));
    }
}
case OrderBy orderBy -> {
    LogicalPlan resolved = InSubqueryResolver.resolveInSubqueryInOrderBy(orderBy);
    if (resolved == orderBy) {
        planListener.onResponse(orderBy);
    } else {
        hasInSubquery.set(true);
        replaceViews(resolved, projectRouting, parser, seenInner,
            viewQueries, hasInSubquery, depth,
            planListener.delegateFailureAndWrap((l, result) -> {
                result.forEachDown(resolvedPlans::add);
                l.onResponse(result);
            }));
    }
}
```

Consider extracting a private helper `resolveAndContinue(LogicalPlan, ...)` to
avoid repeating the boilerplate across these cases and the existing Filter case.

#### 2b. Update the early-return guard (line 177)

```java
// Before:
if (noViews && InSubqueryResolver.hasInSubqueryInFilter(plan) == false) { ... }

// After:
if (noViews && InSubqueryResolver.hasInSubquery(plan) == false) { ... }
```

#### 2c. Update class Javadoc

The class Javadoc currently lists `Filter` as the only node type that triggers
InSubquery resolution. Update to list all supported types.

---

### 3. `EsqlSession.java`

Update `gatherInSubqueryMetrics` to use `hasInSubquery` (the broader check) so
telemetry correctly counts IN subquery usage in EVAL, STATS WHERE, and SORT as well
as WHERE. The existing `WHERE` feature metric counts `SemiJoin`/`AntiJoin`/`MarkJoin`
presence in the post-resolution plan and is unaffected.

---

### 4. `EsqlCapabilities.java`

**Before implementing**, read the Javadoc for `EsqlCapabilities.Cap` and
`FunctionDefinition.Builder#capabilities` to understand the capability registration
mechanism and the rule for choosing between the two.

Add a new capability entry gating all the new contexts together, or one per context
if granularity is preferred for controlled rollout. Suggested name:
`IN_SUBQUERY_IN_BOOLEAN_EXPRESSIONS` (covers EVAL + STATS WHERE + SORT + CASE
extension), or split into `IN_SUBQUERY_IN_EVAL`, `IN_SUBQUERY_IN_STATS_WHERE`,
`IN_SUBQUERY_IN_SORT` if separate feature flags are preferred.

---

### 5. `FieldNameUtils.java` — No changes expected

`FieldNameUtils` runs after `InSubqueryResolver` has rewritten all `InSubquery`
expressions into `MarkJoin` nodes. `mainQueryRequiresFieldCollection` already skips
the right (subquery) child of `AbstractSubqueryJoin` (which includes `MarkJoin`)
regardless of what plan node sits above the join. Verify this holds by running the
full test suite; only change if a specific traversal gap is discovered.

---

### 6. `Analyzer.java` — No changes expected

`Analyzer.ResolveRefs.resolveSubqueryJoin` handles `AbstractSubqueryJoin`
generically — it fills in `rightFields` and wraps the subquery right side in a
`Project` regardless of whether the join sits below a Filter, Eval, Aggregate, or
OrderBy. The `ImplicitCasting` rule in the Resolution batch likewise operates
generically on the plan tree.

---

## What Remains Out of Scope (Future Work)

| Context | Reason |
|---|---|
| `INLINESTATS agg() WHERE x IN (sub)` | `InlineStats` wraps an inner `Aggregate` as its child; the Aggregate pass must be guarded to exclude it. Deferred until the handler is tested in isolation. |
| InSubquery inside non-boolean function argument | Caught by type-checker after mark-attribute substitution; no special handling needed, correct error is produced |

---

## Test Plan

### CSV-spec (`in_subquery.csv-spec`) — new test cases

All new tests require the new capability flag(s).

**EVAL:**
- `EVAL b = x IN (FROM sub)` — basic EVAL
- `EVAL b = NOT (x IN (FROM sub))` — negation in EVAL
- `EVAL b = (x IN (sub1)) AND (y IN (sub2))` — compound EVAL
- `EVAL label = CASE(WHEN x IN (sub) THEN "yes" ELSE "no" END)` — CASE inside EVAL
- `EVAL b = x IN (FROM sub) | STATS COUNT(*) BY b` — EVAL result feeds downstream

**STATS WHERE:**
- `STATS cnt = COUNT(*) WHERE emp_no IN (FROM sub)` — basic
- `STATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM sub)` — NOT IN
- `STATS cnt = COUNT(*) WHERE emp_no IN (FROM sub) BY dept` — with grouping
- Multiple aggregate filters: `STATS a = COUNT(*) WHERE x IN (sub1), b = SUM(salary) WHERE y IN (sub2)`
- Verify empty subquery: count is 0 when subquery returns no rows

**SORT:**
- `SORT emp_no IN (FROM sub)` — sort by membership (true first)
- `SORT emp_no NOT IN (FROM sub)` — false first
- `SORT emp_no IN (FROM sub) DESC` — explicit direction
- `SORT emp_no IN (FROM sub) | LIMIT 10` — with LIMIT (avoids unbounded-sort error)

**WHERE CASE extension:**
- `WHERE CASE(WHEN emp_no IN (sub) THEN true ELSE false END)`

**LIMIT BY:**
- `SORT emp_no | LIMIT 3 BY emp_no IN (FROM sub)` — basic: at most 3 rows per membership bucket
- `SORT emp_no | LIMIT 3 BY emp_no NOT IN (FROM sub)` — NOT IN
- Multiple grouping keys: `LIMIT 2 BY dept, emp_no IN (FROM sub)` — boolean key combined with normal key
- Empty subquery: both groups produced by true/false bucket; true bucket is empty
- Verify `$$mark` is not present in output columns

### `InSubqueryResolverTests` — new unit test cases

- `resolveInSubqueryInEval`: verify plan shape — `$$mark` attribute in Eval, MarkJoin stacked below
- `resolveInSubqueryInEval` with foldable LHS: synthetic Eval inserted for constant
- `resolveInSubqueryInEval` with two InSubquery nodes: two MarkJoins
- `resolveInSubqueryInEval` with CASE WHEN: MarkJoin below Eval, Case uses mark
- `resolveInSubqueryInAggregate`: verify plan shape — `$$mark` in FilteredExpression filter
- `resolveInSubqueryInAggregate` NOT IN: verify anti/negation not applicable here (MarkJoin only)
- `resolveInSubqueryInOrderBy`: verify plan shape — Order uses `$$mark`, MarkJoin below OrderBy
- `testInSubqueryInLimitBy`: verify `LimitBy[$$mark] → MarkJoin → UnresolvedRelation` plan shape
- `testNotInSubqueryInLimitBy`: NOT IN variant; MarkJoin present, grouping contains negated mark
- Remove `testRejectsInSubqueryInLimitBy` (currently at line 829 of `InSubqueryResolverTests.java`)

### `AnalyzerInSubqueryTests` — new cases, update existing

- `testInSubqueryInEval`: verify type resolves to BOOLEAN
- `testInSubqueryInEval_typeIncompatible`: LHS and subquery column type mismatch → error
- `testInSubqueryInStatsWhere`: verify plan after analysis; check that FilteredExpression
  filter holds a `ReferenceAttribute` of type BOOLEAN
- `testInSubqueryInSort`: verify plan after analysis; Order dataType is BOOLEAN
- `testInSubqueryInLimitBy`: verify `LimitBy → MarkJoin → EsRelation` after full analysis
- `testNotInSubqueryInLimitBy`: NOT IN variant
- Remove / update the existing `testRejectsInSubqueryInStatsWhereFilter`,
  `testRejectsInSubqueryInSort`, `testRejectsInSubqueryInLimitBy`, and
  `testRejectsNotInSubqueryInLimitBy` tests that currently assert these are rejected
  (lines 623, 634 of `AnalyzerInSubqueryTests.java`)

### `AnalyzerInSubqueryGoldenTests` / `LogicalPlanOptimizerInSubqueryGoldenTests`

Add golden files for the EVAL, STATS WHERE, and SORT plan shapes, both pre- and
post-optimization (post-optimization: `SubstituteFilteredExpression` fuses the
FilteredExpression into `Count(*, filter=$$mark)` for the STATS WHERE case).

### `InSubqueryIT` / `InSubqueryFailureIT`

- End-to-end integration tests for each new context (EVAL, STATS WHERE, SORT)
- Verify results match equivalent hand-written queries using EVAL + WHERE

---

## Implementation Order

1. Read `EsqlCapabilities.Cap` Javadoc; add new capability entry
2. Implement `rewriteAllInSubqueries` in `InSubqueryResolver`
3. Implement `resolveInSubqueryInEval`; update dispatcher + nested-subquery handler
4. Update `checkInSubqueryUsage` to allow Eval; update `hasInSubquery`
5. Update `ViewResolver` for Eval case + early-return guard
6. Write EVAL CSV-spec tests and unit tests; verify green
7. Implement `resolveInSubqueryInAggregate` (verify `FilteredExpression` child order
   and `Aggregate` constructor signature before coding)
8. Update dispatcher, `checkInSubqueryUsage`, and `ViewResolver` for Aggregate
9. Verify `Aggregate.postAnalysisVerification` passes with `$$mark` in filter
10. Research and implement `InlineStats` handler (confirm it uses `FilteredExpression`)
11. Implement `resolveInSubqueryInOrderBy`; update dispatcher, verifier, ViewResolver
12. Extend `rewriteOrContextInSubqueries` for `Case` (confirm `Case.children()` order)
13. Update `EsqlSession.gatherInSubqueryMetrics`
14. Write STATS WHERE and SORT CSV-spec, unit, and integration tests
15. Update `InSubqueryResolverTests`, `AnalyzerInSubqueryTests`, golden tests
16. Run `spotlessApply` and full test suite
