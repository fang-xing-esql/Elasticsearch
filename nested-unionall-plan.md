# Plan: Allow Nested UNION ALL in ES|QL

> **Implementation status: DONE.** Implemented as planned except Steps 6–7, where the design changed
> after reading the execution code: a flat subplan list cannot carry the wiring between nesting levels,
> because each nested `MergeExec` needs its own `ExchangeSourceHandler` (the inner sub plans must feed the
> *outer branch segment's* exchange source, not the main plan's). Instead,
> `PlannerUtils.breakPlanIntoSubPlansAndMainPlan` now breaks only the **topmost** `MergeExec`s
> (`transformUp` → `transformDown`; the replacement `ExchangeSourceExec` is a leaf so nested merges inside
> sub plans stay intact), and `ComputeService.SubPlansExecutor` recurses: a sub plan that still contains a
> `MergeExec` gets a dedicated exchange source, its segment runs via `runCompute` (mirroring the main plan),
> and its nested sub plans run through a nested `SubPlansExecutor`. The nested executor is constructed
> (registering its empty sink) before the segment starts, resolving the sink pre-registration question.
> Steps 2–5 required no code changes beyond comment updates, as the plan anticipated. A new capability
> `NESTED_SUBQUERY_IN_FROM_COMMAND` gates the new tests, and the nested-subquery limitation was removed
> from the docs.

## Background

ES|QL's `UnionAll` node underlies multi-source `FROM` patterns (e.g. `FROM idx1, idx2`) and subquery FROM patterns. `UnionAll` extends `Fork`. Currently a `UnionAll` inside another `UnionAll` branch is blocked at the post-optimization verification stage. This plan details how to lift that restriction without grammar or parser changes, while leaving all `FORK`-command nesting restrictions exactly as they are.

---

## Scope and Goals

**In scope**: Allow a `UnionAll` node nested inside another `UnionAll` branch — i.e. a multi-source or subquery pattern used as a source for another such pattern.

**Explicitly out of scope / unchanged**:

| Restriction | Error | Status |
|---|---|---|
| `FORK (branch \| FORK ...) (other)` | "Only a single FORK command is supported…" | **KEEP** |
| `FROM idx1, idx2 \| FORK (...)` | "FORK after subquery is not supported" | **KEEP** |
| FORK inside a UnionAll branch | "FORK inside subquery is not supported" | **KEEP** |
| ViewUnionAll nested under UnionAll | "a pattern that expands to multiple sources…" | **KEEP** |
| Approximation queries with nested UnionAll | "approximation not supported…" | **KEEP** |

---

## Current Restriction Enforcement Points for Nested UnionAll

| # | Location | Line | Timing | Message | Action |
|---|---|---|---|---|---|
| 1 | `UnionAll.java` `checkNestedUnionAlls` | 142–151 | Post-optimization | "Nested subqueries are not supported" | **REMOVE** |
| 2 | `UnionAll.java` `nestedUnionAllFailure` | 165–181 | Post-optimization | (same, for UnionAll branch case) | **REMOVE** the UnionAll case; keep Fork and ViewUnionAll cases |
| 3 | `ResolveUnmapped.java` | 213–214 | Analysis | Comment-guarded early return | **UPDATE** |
| 4 | `FieldNameUtils.java` | 170–176 | Field resolution | `assert isNestedFork == false` (skipped for UnionAll per comment, but verify) | **VERIFY / UPDATE** |

---

## Implementation Plan

### Step 1 — `UnionAll.java`: Remove the nested UnionAll check

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnionAll.java`

1. In `checkNestedUnionAlls` (lines 142–151), the check walks each `UnionAll`'s descendants with `forEachDown(Fork.class, ...)` and fails for any nested `Fork`/`UnionAll`. Change it so it only fails for:
   - A `ViewUnionAll` nested under a `UnionAll` (keep existing message).
   - A bare `Fork` (i.e., `instanceof Fork && !(instanceof UnionAll)`) nested under a `UnionAll` (keep "FORK inside subquery is not supported").
   - A plain `UnionAll` nested under another `UnionAll` — **remove this check**.

2. `nestedUnionAllFailure` (lines 165–181) has three arms: `ViewUnionAll`, `UnionAll`, and `Fork`. Remove the `UnionAll` arm (the "Nested subqueries are not supported" branch). Keep the `ViewUnionAll` and `Fork` arms unchanged.

3. No changes to `checkUnionAll` (the post-analysis check at line 100) — it validates data type consistency per branch and does not check nesting.

### Step 2 — `FieldNameUtils.java`: Verify nested UnionAll field resolution

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/FieldNameUtils.java`

The comment at line 172–175 already states the assertion is "skipped for UnionAll cases" (the assertion on line 176 fires only for bare `Fork` nodes, not `UnionAll`). Confirm this by reading the exact condition that guards the assertion:

- If the assertion is guarded by `if (plan instanceof Fork && !(plan instanceof UnionAll))` (or equivalent), then nested UnionAll already passes without hitting the assertion. No change needed here beyond adding a test.
- If the assertion fires for UnionAll as well, update the guard to exclude UnionAll.

Additionally, audit the Fork-branch processing loop (lines 143–208) for correct behavior when a branch of a `UnionAll` is itself a `UnionAll`. Because analysis is bottom-up (`transformUp`), the inner `UnionAll` is resolved before the outer one processes its branches. The outer `UnionAll`'s branch list would already contain the resolved (post-analysis) inner `UnionAll` as a child. Verify that iterating the outer `UnionAll`'s branches in `resolveFieldNames` does not recurse into the inner `UnionAll`'s branches a second time, causing double-counting of field references.

If double-counting is found: add a guard that stops at a nested `UnionAll`/`Fork` boundary inside a branch (the inner `UnionAll`'s fields were already collected when it was processed).

### Step 3 — `Analyzer.java` / `resolveFork`: Verify recursive resolution for nested UnionAll

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Analyzer.java`

The `resolveFork` method (lines 1636–1763) is dispatched from the `ResolveRefs` switch via `transformUp`, which is bottom-up. This means an inner `UnionAll` is fully resolved before the outer `UnionAll` sees it as a branch child. The outer `UnionAll` then calls `Fork.outputUnion()` on its (already-resolved) branches.

The key question: does `outputUnion` correctly handle a branch whose root is a resolved `UnionAll`? Specifically, does it extract the output attributes from the inner `UnionAll`'s output (not from its own children again)?

Verify by tracing through `outputUnion` with a nested input. If `outputUnion` calls `.output()` on each branch's root plan and the branch root is a `UnionAll`, it gets the `UnionAll`'s already-computed union output, which is correct. No change expected here, but add a targeted unit test to confirm.

### Step 4 — `ResolveUnmapped.java`: Handle nested UnionAll branches

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/rules/ResolveUnmapped.java`

The comment at line 213–214 says "Outermost union's direct branch outputs only; nested unions are rejected downstream by checkNestedUnionAlls". With Step 1 removing that downstream rejection, update this comment and the surrounding logic:

- The current code only patches the outermost `UnionAll`'s direct branch `Project` nodes. For nested `UnionAll`, the inner `UnionAll`'s branches were already patched when the inner `UnionAll` was processed (bottom-up). Confirm this is the case — if `patchFork` is called during the Analyzer's `transformUp` pass, the inner `UnionAll` is patched first. If so, no change to the logic is needed, only the comment.
- Remove the "nested unions are rejected downstream" comment and replace with a note that nested UnionAlls are now supported and processed bottom-up.

### Step 5 — Logical Optimizer Rules: Verify for nested UnionAll

**Files**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/`

#### `PushDownFilterAndLimitIntoUnionAll.java`
This rule pushes Filters and Limits past `UnionAll` branches. With nested `UnionAll`, a branch of the outer `UnionAll` may itself be a `UnionAll`. The rule must not push an operator from the outer level through the inner `UnionAll`'s node boundary incorrectly.

Audit: the rule matches specific branch shapes (`Project > EsRelation`, `Project > Subquery`, direct leaf). If a branch's root is a (nested) `UnionAll`, the rule should not match that branch shape and should leave it alone. Verify this by tracing the branch-shape matching code with a nested-`UnionAll` branch.

#### `PushAggregateThroughUnionAll.java`
Uses `isLeafUnionAll` to check that all children are direct `EsRelation`/`ExternalRelation` leaves. A branch that is itself a `UnionAll` fails `isLeafUnionAll`, so the rule would not fire for nested `UnionAll`. This is the correct behavior — confirm and add a test.

#### `PruneEmptyForkBranches.java`
Prunes empty (all-LocalRelation) branches. Operates node-locally on each `UnionAll`. For nested `UnionAll`, the inner `UnionAll`'s empty branches are pruned first (bottom-up). If all inner branches are pruned, the inner `UnionAll` collapses to a single `LocalRelation`, which may then cause the outer `UnionAll`'s branch containing it to also be pruned. Verify this chain works correctly and does not leave the outer `UnionAll` with a broken branch.

#### Other rules
`PushDownFiltersIntoFork` and `PushDownLimitAndOrderByIntoFork` explicitly check `if (filter.child() instanceof Fork == false || filter.child() instanceof UnionAll)` to skip `UnionAll` — these already skip nested `UnionAll` and require no change.

### Step 6 — Physical Plan: Nested `MergeExec` handling

**Files**:
- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/MergeExec.java`
- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java` (lines 138–151)
- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/Mapper.java` (`mapFork`, lines 273–288)

#### `Mapper.mapFork`
Maps a `Fork`/`UnionAll` to a `MergeExec` by calling `mapInner` on each child branch. For nested `UnionAll`, a branch's plan may itself map to a `MergeExec` (from the inner `UnionAll`). The existing `if (child instanceof FragmentExec) { child = new ExchangeExec(...) }` wrapping only covers the `FragmentExec` case. Determine whether a `MergeExec` child of an outer `MergeExec` also needs wrapping (it may need to be wrapped in an `ExchangeExec` to force the coordinator-data-node boundary). If so, extend the condition:

```java
if (child instanceof FragmentExec || child instanceof MergeExec) {
    child = new ExchangeExec(child.source(), child.output(), child);
}
```

Verify against the existing execution model whether a coordinator-side `MergeExec` can be a direct child of another `MergeExec`, or if the `ExchangeExec` wrapper is required.

#### `PlannerUtils.breakPlanIntoSubPlansAndMainPlan`
This is the most structurally significant change. The current implementation (lines 138–151) uses a single `Holder<List<PhysicalPlan>>` that is **overwritten** each time `transformUp` encounters a `MergeExec`. For nested `UnionAll`, `transformUp` processes inner `MergeExec` nodes first (bottom-up), replacing each with an `ExchangeSourceExec`. The outer `MergeExec` then sees `ExchangeSourceExec` children (already correct). The bug is that the `Holder` is overwritten each time, so only the **last** (outermost) `MergeExec`'s subplans survive.

Fix: replace the `Holder` with an accumulating list:

```java
public static Tuple<List<PhysicalPlan>, PhysicalPlan> breakPlanIntoSubPlansAndMainPlan(PhysicalPlan plan) {
    List<PhysicalPlan> allSubplans = new ArrayList<>();
    PhysicalPlan mainPlan = plan.transformUp(MergeExec.class, me -> {
        for (PhysicalPlan child : me.children()) {
            allSubplans.add(new ExchangeSinkExec(child.source(), child.output(), false, child));
        }
        return new ExchangeSourceExec(me.source(), me.output(), false);
    });
    return new Tuple<>(allSubplans, mainPlan);
}
```

Because `transformUp` processes inner nodes before outer ones, `allSubplans` contains inner fork subplans at lower indices and outer fork subplans at higher indices. Inner subplans must complete before their `ExchangeSourceExec` placeholder in the outer subplan can be consumed.

### Step 7 — `ComputeService`: Execute nested subplans in dependency order

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java`

After Step 6, `breakPlanIntoSubPlansAndMainPlan` returns all subplans from all nesting levels in inner-first order. The `SubPlansExecutor` (lines 808–914) currently starts up to `branchParallelDegree` subplans concurrently. With nested `UnionAll`, an outer subplan's `ExchangeSourceExec` reads from an `ExchangeSinkHandler` that belongs to an inner subplan. If the outer subplan starts before the inner subplan's `ExchangeSinkHandler` is registered, the outer subplan will block waiting for data that hasn't arrived yet — or worse, fail with a "handler not found" error.

Two options:

**Option A — Register all `ExchangeSinkHandler`s upfront** (recommended): Before starting any subplan, register all `ExchangeSinkHandler`s in the `ExchangeSourceHandler`. Then start subplans concurrently. The outer subplan's `ExchangeSourceExec` blocks until the inner subplan's sink produces data, which is the normal backpressure mechanism and already works for single-level.

**Option B — Execute in dependency order**: Execute inner subplans first (wait for completion), then outer subplans. Simple but serial — loses parallelism between unrelated branches.

Implement Option A. Audit the `ExchangeSourceHandler.addRemoteSink` registration path (line 775 context) to confirm it allows pre-registration before the producer has started.

---

## Implementation Order

Dependencies flow in this sequence:

1. **Step 1** (`UnionAll.java` — remove nested UnionAll check): unblocks all tests.
2. **Step 2** (`FieldNameUtils.java` — verify/fix assertion and branch iteration): needed for field-caps resolution to work.
3. **Step 3** (`Analyzer.java` — verify recursive resolution): likely no code change, but add targeted tests.
4. **Step 4** (`ResolveUnmapped.java` — update comment and audit logic): needed for unmapped-field tests to pass.
5. **Step 5** (Optimizer rules — audit): can run in parallel with Steps 2–4.
6. **Step 6** (`PlannerUtils.breakPlanIntoSubPlansAndMainPlan` + `Mapper.mapFork`): requires Steps 1–4 (correct logical plan).
7. **Step 7** (`ComputeService` — subplan dependency ordering): requires Step 6.
8. Tests: unit tests alongside each step; integration tests after Step 7.

---

## Test Plan

### 1. Existing tests to flip from error to success

| Test file | Location | Current expected error | New expected behavior |
|---|---|---|---|
| `LogicalPlanOptimizerTests.java` | line 10312 `testNestedSubqueries` | "Nested subqueries are not supported" | Resolves and optimizes without error |
| `LogicalPlanOptimizerTests.java` | line 10327 `testForkInSubquery` | "FORK inside subquery is not supported" | **KEEP failing** — FORK inside subquery remains unsupported |

### 2. Existing tests to confirm unchanged (FORK restrictions still hold)

Confirm all of the following still produce their existing errors, unchanged:

| Test file | Pattern | Expected error (unchanged) |
|---|---|---|
| `AnalyzerTests.java:3756` | `FORK \| FORK` | "Only a single FORK command is supported…" |
| `AnalyzerTests.java:3761` | FORK nested inside a FORK branch | "Only a single FORK command is supported…" |
| `VerifierTests.java:4180` | FORK after subquery FROM | "FORK after subquery is not supported" |
| `AnalyzerUnmappedTests.java:431` | FORK \| FORK with unmapped DROP | "Only a single FORK command is supported…" |
| `AnalyzerUnmappedTests.java:448` | FORK after subquery (unmapped) | "FORK after subquery is not supported" |
| `LogicalPlanOptimizerTests.java:10327` | FORK inside subquery | "FORK inside subquery is not supported" |

### 3. New Analyzer unit tests (`AnalyzerTests.java` or `AnalyzerUnionAllTests.java`)

- **`testNestedUnionAllResolution`**: Two-level nested subquery pattern — outer `UnionAll` whose one branch contains an inner `UnionAll`. Assert the plan resolves without error and the output schema is correct.

- **`testNestedUnionAllTypeResolution`**: Inner and outer `UnionAll` branches have type conflicts on a shared column. Assert `ResolveUnionTypesInUnionAll` inserts type-coercion Evals at both the inner and outer levels independently.

- **`testNestedUnionAllUnmappedField`**: A field is unmapped at one index within an inner `UnionAll` branch. Assert the null-Eval patch is applied at the inner level (not the outer), and the outer `UnionAll` sees a consistent schema from the inner one.

- **`testViewUnionAllNestedStillRejected`**: A `ViewUnionAll` pattern nested inside a `UnionAll` branch still produces "a pattern that expands to multiple sources…". This ensures the ViewUnionAll arm in `nestedUnionAllFailure` is not accidentally removed.

- **`testForkInsideUnionAllStillRejected`**: A `FORK` inside a `UnionAll` branch still produces "FORK inside subquery is not supported".

### 4. New Logical Optimizer unit tests (`LogicalPlanOptimizerTests.java`)

- **`testNestedUnionAllOptimizationNoError`**: Nested `UnionAll` plan passes through the full optimizer pipeline (including post-optimization verification) without error.

- **`testFilterNotPushedThroughNestedUnionAll`**: A `Filter` above an outer `UnionAll` is pushed into direct leaf branches of the outer `UnionAll`, but is NOT pushed through the outer branch boundary into the inner `UnionAll`'s branches. Assert the optimizer does not create an invalid plan shape.

- **`testAggregateNotPushedThroughNestedUnionAllSubquery`**: `PushAggregateThroughUnionAll` does not fire when any branch of the outer `UnionAll` is itself a `UnionAll` (non-leaf shape). Assert the aggregate stays above the outer `UnionAll`.

- **`testPruneEmptyInnerUnionAllBranch`**: An inner `UnionAll` branch has a `WHERE false`. Assert the inner branch is pruned. If all inner branches are pruned, the inner `UnionAll` collapses to a `LocalRelation`, and the outer `UnionAll`'s branch containing it is also pruned. Assert no crash or malformed plan.

### 5. Physical Planner tests

- **`testBreakPlanNestedMergeExec`** (unit test for `PlannerUtils`): Construct a `MergeExec` whose child branches contain additional `MergeExec` nodes (simulating nested `UnionAll`). Call `breakPlanIntoSubPlansAndMainPlan` directly. Assert:
  - The returned subplans list contains subplans from all levels (inner-level subplans at lower indices).
  - The returned main plan contains `ExchangeSourceExec` in place of every `MergeExec`.
  - No `MergeExec` nodes remain in the main plan.

- **`testNestedUnionAllPhysicalMapping`**: Map a nested-`UnionAll` logical plan end-to-end through the physical planner. Assert the resulting `MergeExec` tree is correctly structured before breaking.

### 6. Integration tests (`ForkIT.java` or a new `UnionAllIT.java`)

- **`testNestedUnionAllExecution`**: Execute a query whose `FROM` pattern creates a two-level nested `UnionAll` (e.g., `FROM (idx1, idx2), (idx3, idx4)` or equivalent subquery). Assert correct row count and field values from all contributing indices.

- **`testNestedUnionAllResultCorrectness`**: Insert distinct documents to three indices. Use a nested `UnionAll` pattern that unions indices 1+2 and then unions that with index 3. Assert all documents appear exactly once in the result.

- **`testNestedUnionAllWithFilter`**: A `WHERE` clause after the nested `UnionAll`. Assert it correctly filters across all nested sources.

- **`testNestedUnionAllWithStats`**: `STATS count = COUNT(*) BY someField` over a nested `UnionAll`. Assert aggregate counts are correct.

- **`testNestedUnionAllWithLimit`**: `LIMIT N` after a nested `UnionAll`. Assert at most N rows are returned.

- **`testNestedUnionAllEmptyInnerBranch`**: One source in the inner `UnionAll` matches no documents. Assert 0 rows from that source, no crash.

- **`testNestedUnionAllMaxDepth`** (optional, for stress): Three levels of nesting. Assert correct execution and result count.

### 7. CSV spec tests (`x-pack/plugin/esql/src/test/resources/csv-spec/`)

Add a new section (or file) `nested-subquery.csv-spec`:

- **`nestedSubqueryBasic`**: Multi-level nested FROM pattern producing rows from all sources. Verify field values and row count.
- **`nestedSubqueryWithFilter`**: Filter applied after nested subquery.
- **`nestedSubqueryWithStats`**: Aggregate after nested subquery.
- **`nestedSubqueryTypeCoercion`**: Type conflict across inner union branches resolved by coercion.

---

## Open Questions

1. **`ExchangeSinkHandler` pre-registration**: Confirm that `ExchangeSourceHandler.addRemoteSink` supports pre-registration (registering the sink before the producer has started) and that the consumer correctly blocks rather than errors when no data has arrived yet.

2. **`Mapper.mapFork` wrapping for nested `MergeExec`**: Confirm whether a `MergeExec` child of an outer `MergeExec` requires an `ExchangeExec` wrapper, or if the coordinator can directly chain `MergeExec` nodes.

3. **Three or more nesting levels**: The `transformUp`-based approach in Steps 3 and 6 is theoretically recursive to any depth. Validate with a three-level nested `UnionAll` integration test to confirm there is no hidden hard-coded depth assumption elsewhere.

---

# Follow-up Plan: Flatten Single-Branch Nested `UnionAll` Nodes

> **Implementation status: DONE.** This logical-optimizer follow-up removes redundant plain `UnionAll` nodes after other operator rules
> reduce them to one surviving branch, while preserving the output identity expected by their parents. Coverage was implemented as
> analyzed/optimized golden plans rather than synthetic rule-unit plans, including a three-level singleton collapse. Correlation is
> deliberately skipped when it would erase a specialized non-`ReferenceAttribute` output type.

## Motivation and observed plan shape

`LogicalPlanOptimizerSubqueryGoldenTests.testNestedSubqueriesWithUnionAllOnTopOfMultipleUnionAllsWithPredicatePushdown` demonstrates the opportunity. Predicate pushdown removes branches that cannot evaluate `emp_no > 10000`, leaving two inner `UnionAll` nodes with one child each. Those nodes are still mapped to merge/exchange execution and therefore remain pipeline breakers even though a one-branch union has no union semantics left.

The optimization is deliberately narrower than general union reassociation:

- Remove a **plain** `UnionAll` only when `children().size() == 1`.
- Do not merge or reassociate multi-branch nested unions.
- Do not change `Fork`; a one-branch `FORK` still supplies FORK semantics such as `_fork`.
- Do not flatten `ViewUnionAll`. It carries a `namedSubqueries` invariant, and existing view compaction/tests intentionally preserve some one-child wrappers. The rule targets parser/dataset plain `UnionAll` nodes.
- Leave zero-child unions alone so the existing verifier reports the malformed state.

## Step 1 — Add `FlattenNestedSubqueries`

**New file**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/FlattenNestedSubqueries.java`

1. Add a final optimizer rule extending `OptimizerRules.OptimizerRule<UnionAll>`.
2. Use `TransformDirection.UP` so inner singleton unions are removed before their containing unions are considered. This makes arbitrary nesting deterministic and lets one application flatten every eligible level.
3. Return the input unchanged for `ViewUnionAll`, zero branches, and two or more branches.
4. For exactly one child, replace the union with either the child or an output-correlation `Project`, as described in Step 2.
5. Add class Javadoc explaining that the rule removes a merge/pipeline boundary after branch-pruning rules have made it redundant; it does not flatten multi-branch unions into one another.

The rule is idempotent: after a singleton union is replaced, that union is absent on the next fixed-point iteration.

## Step 2 — Preserve the `UnionAll` output contract

A direct `return unionAll.children().getFirst()` is only safe when the child already exposes the same ordered output attributes. Normally the analyzed `UnionAll` has its own `ReferenceAttribute` `NameId`s, while its branch has different field/reference IDs. Plans above the union continue to reference the union IDs, so dropping the node without correlation would create missing references.

Use this replacement algorithm:

1. Let `child` be the only branch.
2. If `unionAll.output()` and `child.output()` already agree in order, names, data types, and IDs, return `child` directly.
3. Otherwise, index the child output by name. Resolved `UnionAll` output names are unique, but build the map defensively and keep the original union if a name is missing, duplicated, or has an incompatible data type.
4. Iterate `unionAll.output()` in its original order and build `Project` expressions:
   - Reuse the matching child attribute when it already has the target output identity.
   - Otherwise create an `Alias` whose child is the branch attribute and whose name, `NameId`, and synthetic flag come from the corresponding union output attribute.
   - Keep the union when a changed target is a specialized non-`ReferenceAttribute` output. `Alias.toAttribute()` always creates a
     `ReferenceAttribute`, so correlating an `ExternalMetadataAttribute` this way would erase its virtual-column marker and could enable
     invalid predicate pushdown or field pruning.
5. Return `new Project(unionAll.source(), child, projections)`.

This produces the values from the surviving branch while retaining the exact column order and IDs consumed by parent `Project`, `Filter`, `Limit`, and outer `UnionAll` nodes. The next operator-batch iteration can combine the correlation project with an adjacent branch project where safe.

## Step 3 — Register the rule last in the operators batch

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/LogicalPlanOptimizer.java`

1. Import `FlattenNestedSubqueries`.
2. Add `new FlattenNestedSubqueries()` as the final rule in `operators()`, immediately after `PruneEmptyForkBranches`.

The ordering matters:

- Constant folding, predicate pushdown, and `PruneEmptyForkBranches` first determine which branches survive.
- `FlattenNestedSubqueries` then removes any newly singleton plain unions.
- The operators batch runs to a fixed point, so the following iteration gives earlier rules such as `CombineProjections`, filter pushdown, and column pruning access to the newly exposed child plan.

Because `LocalLogicalPlanOptimizer` reuses `operators()`, verify that the rule is a harmless no-op on ordinary local fragments, where coordinator-only `UnionAll` nodes should already be absent.

## Step 4 — Keep related comments accurate

**Files**:

- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Fork.java`
- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnionAll.java`
- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/PruneEmptyForkBranches.java`

Keep `pruneEmptyBranches` as a pruning primitive; do not move singleton-collapse behavior into `Fork` or `UnionAll`. Update only comments/Javadocs that describe what happens after a single survivor remains:

- `Fork` and `UnionAll` pruning preserve the wrapper themselves.
- `FlattenNestedSubqueries` performs the later logical-optimizer collapse for plain `UnionAll`.
- `ViewUnionAll` and bare `Fork` remain wrapped.

This separation lets analyzer/view code retain its structural metadata while the optimizer removes only execution-redundant subquery unions.

## Step 5 — Add optimizer golden coverage

**File**: `x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/optimizer/LogicalPlanOptimizerSubqueryGoldenTests.java`

Use analyzed query plans rather than hand-built rule inputs so the tests exercise the output identities and branch-alignment projections
that the analyzer actually produces:

1. **Single surviving branch**: push a predicate through a two-branch union so one branch is pruned. Assert through the golden plan that
   the singleton union disappears and a correlation `Project` retains the union output IDs.
2. **Multiple nested singleton unions**: construct several nested union levels whose non-matching branches are pruned by the same outer
   predicate. Assert that every singleton union is removed bottom-up and the resulting plan remains reference-consistent.
3. **Multi-branch unions remain**: retain the existing nested-subquery golden cases in which two or more branches survive.

Malformed synthetic plans, zero-child unions, and metadata-bearing `ViewUnionAll` wrappers cannot be produced by this resolved-query
golden harness. The rule therefore keeps defensive no-op guards for those shapes, while the existing Fork/View analyzer and optimizer
suites continue to cover their externally visible behavior.

## Step 6 — Update the nested-subquery golden test

**Files**:

- `x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/optimizer/LogicalPlanOptimizerSubqueryGoldenTests.java`
- `x-pack/plugin/esql/src/test/resources/org/elasticsearch/xpack/esql/optimizer/golden_tests/LogicalPlanOptimizerSubqueryGoldenTests/testNestedSubqueriesWithUnionAllOnTopOfMultipleUnionAllsWithPredicatePushdown/logical_optimization.expected`

1. Remove the TODO above `testNestedSubqueriesWithUnionAllOnTopOfMultipleUnionAllsWithPredicatePushdown`.
2. Regenerate only this test's logical-optimization expectation.
3. Confirm that:
   - The outer three-branch `UnionAll` remains.
   - Both inner one-branch `UnionAll` nodes disappear.
   - Correlation `Project`s preserve the inner union outputs consumed by the surrounding subquery/outer branch.
   - The pushed predicates remain on the surviving `employees` branches.
   - `analysis.expected` remains unchanged because this is an optimizer-only rule.

## Step 7 — Update existing singleton-union expectations

The new rule changes full-optimizer tests that currently inspect the intermediate result of branch pruning and expect a one-child `UnionAll` to survive.

Audit and update at least:

- `PruneEmptyForkBranchesTests.testOneEmptySubquery` — expect the correlated surviving branch rather than a singleton union.
- The singleton cases in `PushDownFilterAndLimitIntoUnionAllTests`, including reference-attribute, full-text, KNN, ROW-source, and TS-source branch-pruning scenarios. Preserve their assertions about the pushed filter and surviving source, but navigate through the correlation `Project` instead of through `unionAll.children().getFirst()`.
- Any golden output or logical/physical optimizer test found by the targeted suite that contains a plain one-child `UnionAll` after optimization.

Do not change tests that apply `PruneEmptyForkBranches` directly and assert the pruning primitive's contract, and do not change one-child `Fork` or `ViewUnionAll` expectations.

## Verification plan

1. Format the Java changes:

   ```bash
   ./gradlew :x-pack:plugin:esql:spotlessJavaApply
   ./gradlew :x-pack:plugin:esql:spotlessJavaCheck
   ```

2. Run the nested-subquery golden suite and affected pruning/pushdown suites:

   ```bash
   ./gradlew :x-pack:plugin:esql:test --tests 'org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerSubqueryGoldenTests*'
   ./gradlew :x-pack:plugin:esql:test --tests org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneEmptyForkBranchesTests
   ./gradlew :x-pack:plugin:esql:test --tests org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownFilterAndLimitIntoUnionAllTests
   ```

3. Regenerate the intended golden cases, inspect their diffs, then rerun the full golden class without overwrite:

   ```bash
   ./gradlew :x-pack:plugin:esql:test --tests 'org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerSubqueryGoldenTests.testSingleBranchUnionAllIsFlattened*' --tests 'org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerSubqueryGoldenTests.testMultipleNestedSingleBranchUnionAllsAreFlattened*' -Dgolden.overwrite
   ./gradlew :x-pack:plugin:esql:test --tests 'org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerSubqueryGoldenTests.testNestedSubqueriesWithUnionAllOnTopOfMultipleUnionAllsWithPredicatePushdown*' -Dgolden.overwrite
   ./gradlew :x-pack:plugin:esql:test --tests 'org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerSubqueryGoldenTests*'
   ```

4. Run the broader logical optimizer regression suite to find other valid singleton-union expectation changes:

   ```bash
   ./gradlew :x-pack:plugin:esql:test --tests 'org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerTests'
   ```

5. Finish with `git diff --check` and inspect that existing `analysis.expected` files and unrelated user-owned changes were not rewritten.

No new ES|QL capability or transport version is needed: this is a plan-shape optimization under the existing nested-subquery capability and does not alter wire serialization or query syntax.

---

# Follow-up Plan: Harden Nested `UnionAll` Execution

> **Implementation status: PLANNED.** This follow-up addresses the execution-lifecycle, concurrency, topology, profiling, and coverage
> findings from the nested-subquery implementation review. It does not broaden the supported logical syntax. Documentation is out of
> scope for this follow-up.

## Design decisions

1. Build the complete nested-subquery execution topology once, before execution, instead of repeatedly rediscovering and splitting
   `MergeExec` nodes from inside `ComputeService` callbacks.
2. `branch_parallel_degree` will be a query-wide ceiling on concurrently executing **leaf producer subplans**. Coordinator merge
   segments that wait for nested producers will not hold a permit; counting them would deadlock a nested query when the degree is one.
3. A single coordinator execution segment may contain at most one topmost `MergeExec`. That is the invariant supported by the current
   `ComputeContext`, which supplies one exchange source to every `ExchangeSourceExec` in the segment. Detect violations rather than
   silently feeding independent merge points from the same source.
4. Every merge node in the execution topology owns its exchange source, child sinks, empty-sink keepalive, listener references, session
   registration, and profile identity. This makes success, failure, cancellation, and early-finish cleanup follow one lifecycle.
5. The root exchange source will be registered under the root query session ID so async STOP can find it. The main compute driver may
   continue to use its child session ID for its own execution identity.
6. Top-level profile descriptions remain unchanged. Nested descriptions gain their full branch path so profiles from different levels
   cannot collide.

## Step 1 — Build an immutable nested execution topology

**Files**:

- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/PlannerUtils.java`
- A new immutable execution-topology type in the ES|QL planner/plugin package
- `x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/planner/PlannerUtilsTests.java`

1. Replace the flat `breakPlanIntoSubPlansAndMainPlan` result with a sealed, immutable tree whose nodes are conceptually:
   - `Leaf(plan)`: a producer plan with no topmost `MergeExec`;
   - `Merge(segmentPlan, children)`: a coordinator segment whose topmost merge has been replaced by an `ExchangeSourceExec`, plus the
     recursively planned producer branches that feed it.
2. Build this tree recursively in `PlannerUtils`. For each merge branch, first add the required `ExchangeSinkExec`, then build that
   branch's child topology. This preserves the current exchange boundaries without making execution rediscover physical-plan shape.
3. In each segment, replace exactly one topmost `MergeExec` and skip its descendants while scanning that segment. Nested merges remain in
   the collected child branches and become child `Merge` nodes.
4. If a second independent topmost merge is found in one segment, throw an internal planning exception containing the segment shape.
   The current `ComputeContext` has only one exchange-source supplier, so accepting that plan would silently mix streams.
5. Assign or derive a stable branch path for each tree node while constructing/traversing the topology. The same identity will drive
   session names and profiling descriptions later.
6. Remove runtime calls that repeatedly invoke `breakPlanIntoSubPlansAndMainPlan`; the complete topology must be validated before any
   exchange source is registered or branch starts.

Extend `PlannerUtilsTests` with leaf-only, one-level, two-level, three-level, and multiple nested-sibling shapes; assert the segment plans,
exchange sink/source boundaries, output attributes, and stable traversal order. Add the two-independent-topmost-merges failure case. Run
the physical planner, logical/physical optimizer, join, FORK, view, and nested-subquery suites to confirm supported plans satisfy the
invariant. If a supported query violates it, replace the invariant with ID-aware local exchange groups rather than sharing one unkeyed
source: each merge point must carry an exchange key that `LocalExecutionPlanner` resolves to its own handler.

## Step 2 — Replace recursive `SubPlansExecutor` discovery with a topology runner

**Files**:

- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java`
- A new package-private `SubPlanExecutionRunner` and focused unit test in `org.elasticsearch.xpack.esql.plugin`
- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/QueryPragmas.java`

1. Have `ComputeService.execute` build the execution topology once. Execute a root `Leaf` through the existing `executePlan` path; hand
   a root `Merge` to `SubPlanExecutionRunner`. Remove the callback-time recursive splitting from `SubPlansExecutor` and
   `executeSubPlanWithNestedSubPlans`.
2. Give the runner one non-blocking, query-wide scheduler sized from `branchParallelDegree()`. It schedules topology nodes rather than
   allowing each nesting level to create an independent concurrency window.
3. A `Leaf` holds one scheduler slot from immediately before `executePlan` until its response or failure. A `Merge` starts its local
   coordinator segment, enqueues its child nodes, and releases the scheduler slot immediately; it must not occupy a producer permit
   while waiting for descendants. This guarantees progress when the degree is one.
4. Each `Merge` runtime owns a dedicated `ExchangeSourceHandler`, an empty-sink keepalive, one child sink per branch, and a node-local
   completion listener covering its coordinator segment and descendants. It reports completion to its parent sink only when the entire
   subtree has reached a terminal state.
5. Give every scheduled item a release-once token. Release it on response, failure, cancellation, or synchronous startup exception
   before dispatching more work.
6. Make queued-task termination explicit: query cancellation or a terminal runner failure drains the queue and invokes each item's
   failure callback so pre-acquired listener references and exchange keepalives cannot remain unresolved.
7. Dispatch outside the scheduler lock through the existing search executor, so synchronous callbacks cannot recurse while scheduler
   state is being mutated.
8. Update `BRANCH_PARALLEL_DEGREE` Javadoc to define the query-wide leaf-producer semantics.

Add deterministic runner/scheduler unit coverage for degrees one and greater, maximum observed leaf concurrency, nested progress,
queued ordering, synchronous startup failure, cancellation/failure draining, and double-release protection. Add an end-to-end
degree-one three-level query to prove the runner cannot deadlock.

## Step 3 — Make exchange and session cleanup node-owned

**Files**:

- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java`
- The new `SubPlanExecutionRunner`
- Relevant async-stop/internal-cluster tests under `x-pack/plugin/esql/src/internalClusterTest/java/.../action/`

1. Centralize every merge node's exchange registration/removal, empty-sink reference, child sinks, compute-listener references, and
   cancellation hook in its runner-owned runtime object. Use one release-once terminal path for success, failure, STOP, early finish,
   and synchronous setup exceptions.
2. Register the root merge source in `ExchangeService` under the root `sessionId`, not `mainSessionId`. Keep `mainSessionId` in the main
   `ComputeContext` as its driver/session identity.
3. Register nested merge sources under stable unique child session IDs derived from the topology path. Closing the root must cascade to
   its child sinks and runner tasks, after which every nested node removes its own registration.
4. Remove each source with the same key used to register it. Completion must not depend on the inactive-sink reaper.
5. Prefer testing `ExchangeService.finishSessionEarly` through the public async-stop flow. Add a narrowly scoped package-private test
   accessor only if existing APIs cannot observe post-completion cleanup.

Add tests proving async STOP finds an active nested-subquery root, returns buffered results, marks the response partial, and terminates
all descendants. Also assert that normal success, innermost failure, cancellation, early `LIMIT`, and synchronous startup failure leave
no root or nested handler registered.

## Step 4 — Make nested profile identities unambiguous

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java`

1. Give each execution-topology node a profile path derived from its parent and branch index; use the same path in its runner runtime.
2. Preserve existing top-level names such as `subplan-0.final` and `subplan-1.final`.
3. Use hierarchical names for nested work, for example `subplan-1.subplan-0.final` and
   `subplan-1.subplan-0.merge`, rather than restarting numbering at every level.
4. Add a nested profiling integration test that asserts descriptions are unique, every merge segment is represented, and leaf/data-node
   profiles are still accumulated into the final response.

## Step 5 — Close nested execution coverage gaps

**Files**:

- `x-pack/plugin/esql/src/internalClusterTest/java/org/elasticsearch/xpack/esql/action/SubqueryIT.java`
- `x-pack/plugin/esql/src/internalClusterTest/java/org/elasticsearch/xpack/esql/action/SubqueryFailureIT.java`
- The most appropriate existing async-stop test class
- Existing nested CSV specs where result-oriented coverage is clearer

Add focused scenarios for:

1. Two- and three-level queries at branch degrees one, two, and greater than the number of leaves, with identical results at every
   degree.
2. Failure in the innermost branch while outer siblings are running and while siblings are queued; assert the original failure is
   returned, all listener references complete, and no exchange handler waits for the reaper.
3. `allow_partial_results` with a failing inner shard, checking both returned rows and cluster/execution status propagation through each
   nesting level.
4. An outer early `LIMIT` that stops consuming while nested branches are producing; assert the sink-close cascade terminates all nested
   work promptly.
5. Async STOP/cancellation during nested execution, including the degree-one case.
6. Profiling, as described in Step 4.
7. A repeat/stress run using deterministic seeds to catch completion races around empty sinks, queued branches, and nested listener
   teardown.

Reuse real test clusters and `InternalExchangePlugin`/existing failing-field infrastructure rather than mocks. Keep the successful
three-level CSV and cross-cluster tests as broad end-to-end coverage.

## Recommended implementation order

1. Introduce the immutable execution topology and validate the one-topmost-merge invariant with planner tests.
2. Implement the topology runner and query-wide scheduler with deterministic lifecycle/concurrency unit tests.
3. Move exchange/session ownership into the runner and add root STOP plus registry-cleanup regressions.
4. Add hierarchical profiling and the nested failure/STOP/early-limit integration coverage.

## Verification plan

1. Format and lint:

   ```bash
   ./gradlew :x-pack:plugin:esql:spotlessJavaApply
   ./gradlew :x-pack:plugin:esql:spotlessJavaCheck
   ```

2. Run focused unit suites:

   ```bash
   ./gradlew :x-pack:plugin:esql:test --tests 'org.elasticsearch.xpack.esql.planner.PlannerUtilsTests'
   ./gradlew :x-pack:plugin:esql:test --tests 'org.elasticsearch.xpack.esql.plugin.SubPlanExecutionRunnerTests'
   ```

3. Run nested execution, failure, profile, and async-stop internal-cluster tests using targeted method filters first, then their complete
   containing classes.
4. Rerun the nested-subquery optimizer goldens and the three-level CSV case to confirm the execution hardening does not change logical
   plans or results.
5. Run the broader ES|QL optimizer/planner precommit coverage appropriate to every touched project.
6. Finish with `git diff HEAD --check` and verify that all exchange registry additions have a matching success/failure/STOP removal path.

The execution topology is coordinator-local, so no new ES|QL capability or transport version should be needed. If Step 1 discovers that
ID-aware exchange groups must instead be carried in a serialized physical plan, stop and follow the repository's transport-version
workflow before changing the wire shape.
