```yaml {applies_to}
serverless: preview
stack: preview 9.4.0
```

A subquery is a complete ES|QL query wrapped in parentheses that can be used
in place of an index pattern in the [`FROM`](/reference/query-languages/esql/commands/from.md) command.
Each subquery is executed independently, and the results are combined using
UNION ALL semantics.

## Syntax

```esql
FROM index_pattern, ( FROM index_pattern | <processing_commands> ), ...
```

A subquery starts with a `FROM` source command followed by zero or more piped
processing commands, all enclosed in parentheses. Multiple subqueries and regular
index patterns can be combined in a single `FROM` clause, separated by commas.

## Description

Subqueries enable you to combine results from multiple independently processed
data sources within a single query. Each subquery runs its own pipeline of
processing commands (such as `WHERE`, `EVAL`, `STATS`, or `SORT`) and the
results are unioned together with results from other index patterns or subqueries
in the `FROM` clause.

Fields that exist in one source but not another are filled with `null` values.

Subqueries support but not limit to the following processing commands:
- Filtering with `WHERE`
- Field transformations with `EVAL`
- Aggregations with `STATS`
- Sorting with `SORT` and limiting with `LIMIT`
- Enrichment with `ENRICH` or `LOOKUP JOIN`
- Pattern extraction with `GROK` or `DISSECT`
- The `METADATA` directive on either the subquery or the outer `FROM`

::::{note}
The `FROM` clause can also consist entirely of subqueries, with no regular index
pattern required.
::::

## Examples

The following examples show how to use subqueries within the `FROM` command.

### Combine data from multiple indices

Use a subquery alongside a regular index pattern to combine results from
different indices:

```esql
FROM
    employees,
    (FROM sample_data)
| WHERE ( emp_no >= 10091 AND emp_no < 10094)  OR emp_no IS NULL
| SORT emp_no, client_ip
| KEEP  emp_no, languages, client_ip
```

| emp_no:integer | languages:integer | client_ip:ip |
| --- | --- | --- |
| 10091 | 3 | null |
| 10092 | 1 | null |
| 10093 | 3 | null |
| null | null | 172.21.0.5 |
| null | null | 172.21.2.113 |
| null | null | 172.21.2.162 |
| null | null | 172.21.3.15 |
| null | null | 172.21.3.15 |
| null | null | 172.21.3.15 |
| null | null | 172.21.3.15 |

Rows from `employees` have `null` for `client_ip`, while rows from `sample_data`
have `null` for `emp_no` and `languages`, because each index has different fields.

### Use only subqueries (no main index pattern)

You can use one or more subqueries without specifying a regular index pattern:

```esql
FROM (FROM employees)
| WHERE emp_no >= 10091 AND emp_no < 10094
| SORT emp_no
| KEEP  emp_no, languages
```

| emp_no:integer | languages:integer |
| --- | --- |
| 10091 | 3 |
| 10092 | 1 |
| 10093 | 3 |

### Filter data inside a subquery

Apply a `WHERE` clause inside the subquery to pre-filter data before combining:

```esql
FROM
    employees,
    (FROM sample_data metadata _index
     | WHERE client_ip == "172.21.3.15")
    metadata _index
| WHERE ( emp_no >= 10091 AND emp_no < 10094)  OR emp_no IS NULL
| EVAL _index = MV_LAST(SPLIT(_index, ":"))
| SORT emp_no
| KEEP _index,  emp_no, languages, client_ip
```

| _index:keyword | emp_no:integer | languages:integer | client_ip:ip |
| --- | --- | --- | --- |
| employees | 10091 | 3 | null |
| employees | 10092 | 1 | null |
| employees | 10093 | 3 | null |
| sample_data | null | null | 172.21.3.15 |
| sample_data | null | null | 172.21.3.15 |
| sample_data | null | null | 172.21.3.15 |
| sample_data | null | null | 172.21.3.15 |

### Aggregate data inside a subquery

Use `STATS` inside a subquery to aggregate data before combining with other sources:

```esql
FROM
    employees,
    (FROM sample_data metadata _index
     | STATS cnt = count(*) by _index, client_ip)
    metadata _index
| WHERE ( emp_no >= 10091 AND emp_no < 10094)  OR emp_no IS NULL
| EVAL _index = MV_LAST(SPLIT(_index, ":"))
| SORT _index, emp_no, client_ip
| KEEP _index, emp_no, languages, cnt, client_ip
```

| _index:keyword | emp_no:integer | languages:integer | cnt:long | client_ip:ip |
| --- | --- | --- | --- | --- |
| employees | 10091 | 3 | null | null |
| employees | 10092 | 1 | null | null |
| employees | 10093 | 3 | null | null |
| sample_data | null | null | 1 | 172.21.0.5 |
| sample_data | null | null | 1 | 172.21.2.113 |
| sample_data | null | null | 1 | 172.21.2.162 |
| sample_data | null | null | 4 | 172.21.3.15 |

### Combine multiple subqueries

Multiple subqueries can be combined in a single `FROM` clause:

```esql
FROM
    employees,
    (FROM sample_data metadata _index
     | STATS cnt = count(*) by _index, client_ip),
    (FROM sample_data_str metadata _index
     | STATS cnt = count(*) by _index, client_ip)
    metadata _index
| EVAL client_ip = client_ip::ip, _index = MV_LAST(SPLIT(_index, ":"))
| WHERE client_ip == "172.21.3.15" AND cnt >0
| SORT _index, emp_no, client_ip
| KEEP _index, emp_no, languages, cnt, client_ip
```

| _index:keyword | emp_no:integer | languages:integer | cnt:long | client_ip:ip |
| --- | --- | --- | --- | --- |
| sample_data | null | null | 4 | 172.21.3.15 |
| sample_data_str | null | null | 4 | 172.21.3.15 |

### Use LOOKUP JOIN inside a subquery

Enrich subquery results with a lookup join before combining:

```esql
FROM
    employees,
    (FROM sample_data
     | EVAL client_ip = client_ip::keyword
     | LOOKUP JOIN clientips_lookup ON client_ip)
| WHERE ( emp_no >= 10091 AND emp_no < 10094)  OR emp_no IS NULL
| SORT emp_no, client_ip
| KEEP emp_no, languages, client_ip, env
```

| emp_no:integer | languages:integer | client_ip:keyword | env:keyword |
| --- | --- | --- | --- |
| 10091 | 3 | null | null |
| 10092 | 1 | null | null |
| 10093 | 3 | null | null |
| null | null | 172.21.0.5 | Development |
| null | null | 172.21.2.113 | QA |
| null | null | 172.21.2.162 | QA |
| null | null | 172.21.3.15 | Production |
| null | null | 172.21.3.15 | Production |
| null | null | 172.21.3.15 | Production |
| null | null | 172.21.3.15 | Production |

### Sort and limit inside a subquery

Use `SORT` and `LIMIT` inside a subquery to return only top results:

```esql
FROM
    employees,
    (FROM sample_data
     | STATS cnt = count(*) by client_ip
     | SORT cnt DESC
     | LIMIT 1 )
| WHERE ( emp_no >= 10091 AND emp_no < 10094)  OR emp_no IS NULL
| SORT emp_no, client_ip
| KEEP emp_no, languages, cnt, client_ip
```

| emp_no:integer | languages:integer | cnt:long | client_ip:ip |
| --- | --- | --- | --- |
| 10091 | 3 | null | null |
| 10092 | 1 | null | null |
| 10093 | 3 | null | null |
| null | null | 4 | 172.21.3.15 |
