# Rule Configuration

Rules can be configured with the `.sqlfluff` config files.

Common rule configurations can be set in the `[sqlfluff:rules]` section.

For example:

```ini
[sqlfluff:rules]
allow_scalar = True
single_table_references = consistent
unquoted_identifiers_policy = all
```

Rule specific configurations are set in rule specific subsections.

For example, enforce that keywords are upper case by configuring the rule
[CP01](../reference/rules/capitalisation#cp01):

```ini
[sqlfluff:rules:capitalisation.keywords]
# Keywords
capitalisation_policy = upper
```

All possible options for rule sections are documented in [Rules Reference](../reference/rules/index).

For an overview of the most common rule configurations that you may want to
tweak, see [Default Configuration](../configuration/defaults) (and use [Rules Reference](../reference/rules/index) to find the
available alternatives).


## Enabling and Disabling Rules

The decision as to which rules are applied to a given file is applied on a file
by file basis, by the effective configuration for that file. There are two
configuration values which you can use to set this:

* `rules`, which explicitly *enables* the specified rules. If this
  parameter is unset or empty for a file, this implies "no selection" and
  so "all rules" is taken to be the meaning.
* `exclude_rules`, which explicitly *disables* the specified rules.
  This parameter is applied *after* the `rules` parameter so can be
  used to *subtract* from the otherwise enabled set.

Each of these two configuration values accept a comma separated list of
*references*. Each of those references can be:

* a rule *code* e.g. `LT01`
* a rule *name* e.g. `layout.indent`
* a rule *alias*, which is often a deprecated *code* e.g. `L003`
* a rule *group* e.g. `layout` or `capitalisation`

These different references can be mixed within a given expression, which
results in a very powerful syntax for selecting exactly which rules are
active for a given file.

::: tip Note
It's worth mentioning here that the application of `rules` and
`exclude_rules`, with *groups*, *aliases* and *names*, in projects
with potentially multiple nested configuration files defining different
rules for different areas of a project can get very confusing very fast.
While this flexibility is intended for users to take advantage of, we do
have some recommendations about how to do this is a way that remains
manageable.

When considering configuration inheritance, each of `rules` and
`exclude_rules` will totally overwrite any values in parent config
files if they are set in a child file. While the subtraction operation
between both of them is calculated *"per file"*, there is no combination
operation between two definitions of `rules` (just one overwrites
the other).

The effect of this is that we recommend one of two approaches:

1. Simply only use `rules`. This has the upshot of each area of
    your project being very explicit in which rules are enabled. When
    that changes for part of your project you just reset the whole list
    of applicable rules for that part of the project.
2. Set a single `rules` value in your master project config file
    and then only use `exclude_rules` in sub-configuration files
    to *turn off* specific rules for parts of the project where those
    rules are inappropriate. This keeps the simplicity of only having
    one value which is inherited, but allows slightly easier and simpler
    rollout of new rules because we manage by exception.
:::

For example, to disable the rules [LT08](../reference/rules/layout#lt08)
and [RF02](../reference/rules/references#rf02):

```ini
[sqlfluff]
exclude_rules = LT08, RF02
```

To enable individual rules, configure `rules`, respectively.

For example, to enable [RF02](../reference/rules/references#rf02):

```ini
[sqlfluff]
rules = RF02
```

Use rule groups to enable or disable multiple rules together. For example,
`rules = layout` enables layout rules. The configuration below enables the
rules in the `core` group.

```ini
[sqlfluff]
rules = core
```

More information about 'core rules' can be found in the [Rules Reference](../reference/rules/).

Additionally, some rules have a special `force_enable` configuration
option, which allows to enable the given rule even for dialects where it is
disabled by default. The rules that support this can be found in the
[Rules Reference](../reference/rules/).

The default values can be seen in [Default Configuration](../configuration/defaults).

See [Ignoring Errors](../configuration/ignoring) for more information on how to turn ignore particular
rules for specific lines, sections or files.

## Downgrading rules to warnings

To keep displaying violations for specific rules, but not have those
issues lead to a failed run, rules can be downgraded to *warnings*.
Rules set as *warnings* won't cause a file to fail, but will still
be shown in the CLI to warn users of their presence.

The configuration of this behaves very like `exclude_rules`
above:

```ini
[sqlfluff]
warnings = LT01, LT04
```

With this configuration, files with no other issues (other than
those set to warn) will pass. If there are still other issues, then
the file will still fail, but will show both warnings and failures.

```bash
== [test.sql] PASS
L:   2 | P:   9 | LT01 | WARNING: Missing whitespace before +
== [test2.sql] FAIL
L:   2 | P:   8 | CP02 | Unquoted identifiers must be consistently upper case.
L:   2 | P:  11 | LT01 | WARNING: Missing whitespace before +
```

This is particularly useful as a transitional tool when considering
the introduction on new rules on a project where you might want to
make users aware of issues without blocking their workflow (yet).

You can use either rule code or rule name for this setting.

## Layout & Spacing Configuration

The `[sqlfluff:layout]` section of the config controls the
treatment of spacing and line breaks across all rules. To understand
more about this section, see the section of the docs dedicated to
layout: [Layout Configuration](../configuration/layout).

## Row-Pick Determinism Catalog

The row-pick determinism rules,
[`ST13`](../reference/rules/structure#st13) and
[`AM10`](../reference/rules/ambiguous#am10), need table metadata to prove that a
`ROW_NUMBER()` pick has a unique winner. Because SQLFluff has no database
connection, that metadata comes from a user-supplied, read-only *catalog*.

```ini
[sqlfluff:rules:structure.deterministic_row_pick]
catalog_path = catalog.yml
```

The catalog is YAML (or JSON), and declares tables, natural keys and
relationships:

```yaml
tables:
  site_catalog:
    primary_key: site_id
  event:
    primary_key: [site_id, event_time, record_no]
    nullable: [event_time]
  receipt:
    primary_key: receipt_no
    columns:
      record_no:
        nullable: false

relationships:
  - name: catalog_per_site
    left: site_catalog
    right: event
    left_columns: [site_id]
    right_columns: [site_id]
    cardinality: one_to_many
  - name: receipts_per_record
    left: receipt
    right: event
    left_columns: [record_no]
    right_columns: [record_no]
    cardinality: many_to_one
```

`primary_key`, `natural_key`, `business_key`, `unique_key` and `keys` are
accepted for key declarations. `nullable`, `nullable_columns` and
`nullable_keys` declare key columns which may be null. A relationship's
`cardinality` can be written as `one_to_many`, `many_to_one`, `one_to_one` or
`many_to_many`; `left_max_one`/`right_max_one` and `unique_side` are
equivalent spellings.

The analysis reports three outcomes:

* **deterministic** when the partition, filters and ordering cover a declared
  unique key, so every row in a partition has a distinct rank;
* **not deterministic** when two rows can share a rank, for example because a
  join can return more than one row per key, an ordering column is nullable, or
  the ordering stops short of a declared key; and
* **insufficient evidence** when the catalog does not declare the relevant
  key or table. This is reported only when
  `report_insufficient_evidence = True`, and never treated as a proof.

An outer `DISTINCT` does not make a row pick deterministic: the window has
already assigned the tied rows the same rank before the outer query can remove
duplicates. A filter which pins part of a key (for example
`WHERE event_seq = 7`) does contribute to the proof, because it removes the
unfixed key column from the uniqueness check.

Supply the catalog inline with `catalog` instead of `catalog_path` if it is
small or generated, for example:

```ini
[sqlfluff:rules:ambiguous.row_pick_determinism]
catalog = |
  tables:
    event:
      primary_key: [site_id, event_time, record_no]
```


The same analysis is available programmatically:

```python
import sqlfluff

results = sqlfluff.analyze_row_picks(sql, catalog="catalog.yml")
for result in results:
    print(result.status, result.order_by, result.message)
```
