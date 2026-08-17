# ADR 0003 — Config-driven report definitions

## Plain language

The business meaning of a report (what it filters, what grain it publishes, how close SAS must be) lives in a YAML file that a non-developer can open. The Python still does the maths, but we do not hide portfolio filters inside undocumented `if` statements.

## Context

SAS shops often encode “this report is listed equity only” in a macro or a `WHERE` that only one person remembers. Databricks migrations then rediscover the filter during recon. We want one obvious place per report: `conf/reports/R01.yaml` … `R09.yaml`.

## Decision

Each report has a YAML file with:

- identity (`report_id`, name, regulation)
- filters (instrument types, loan purposes)
- output grain and value columns
- reconciliation keys and tolerances

Python modules stay dumb and uniform: read gold, filter, aggregate, write. Tolerances used by `recon/compare.py` come from YAML, not from hardcoded constants.

## Consequences

- Changing “R01 is equity” to include a new instrument type is a YAML edit plus a test rerun — if gold already has the asset class.
- Changing PCAF itself is **not** a YAML edit. New maths belongs in `gold/`. Reports must not fork attribution formulas.
- A steering group can read `conf/reports/` without opening PySpark.
- Risk: YAML and code can drift (YAML says `equity`, code filters `listed_equity`). Reviewers should check both. The catalog in [03_report_catalog.md](../03_report_catalog.md) is the human-readable check.

## Alternatives rejected

- **Plugin registry / decorators** — violates the project rule that a junior engineer reads a file top to bottom.
- **One giant `reports.yaml`** — harder to diff in git per report.
- **Tolerances only in the recon notebook** — they would not travel with the report definition to Databricks.
