# ADR 0001 — Medallion architecture (bronze / silver / gold)

## Plain language

We keep raw files, cleaned files, and calculated files in three separate places so a disputed number can be traced: “this is what SAS sent, this is what we accepted, this is what we published.” Mixing those three in one table is how migrations become un-auditable.

## Context

The SAS job today tends to land extracts, clean them, and produce the report in one program. That is fast to run and hard to explain six months later. Databricks (and this laptop demo) need a layout that maps to Unity Catalog schemas and to the way control functions already think: archive, operations, finance.

## Decision

Use the medallion pattern:

- **Bronze** — source columns unchanged + `_ingested_at`, `_source_file`, `_batch_id`.
- **Silver** — types, trim, dedup, rejects with reasons. Entity resolution lives here.
- **Gold** — PCAF, quality scores, WACI, taxonomy. Reports only slice gold.

Rejected rows are written to `silver._rejects`. They are never deleted without a reason column.

## Consequences

- More tables than a single SAS `WORK` library. That is intentional.
- A report developer who “just needs a CSV” must not read bronze. If they do, they re-introduce dirty ISINs and duplicates.
- Re-running gold does not require re-ingesting SAS files.
- Cost: we store three copies. For this volume it is negligible; for production it is still cheaper than an untraceable number.

## Alternatives rejected

- **One “ESG mart” table** — cannot tell archive from calculation; Unity Catalog grants become all-or-nothing.
- **Notebook-only, no layers** — cannot test PCAF without clicking through; cannot deploy the same code locally and on a cluster.
