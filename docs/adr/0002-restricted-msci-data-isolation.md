# ADR 0002 — Restricted MSCI data isolation

## Plain language

The emissions file is licensed. It must not sit in the same open folder as portfolios and holdings. Locally we use a separate directory named `esg_restricted`. On Databricks that becomes a Unity Catalog schema with its own grants. Same idea, different lock.

## Context

Banks typically receive MSCI (or equivalent) under a contract that limits who may see entity-level emissions and ratings. The SAS warehouse often already isolates that library. A lakehouse migration that dumps `msci_esg` into `bronze` next to `holdings.csv` is a licence and audit failure even if the PCAF maths is perfect.

## Decision

- Ingest `msci_esg` into schema key `esg_restricted`, not `bronze`.
- Clean it in place (`msci_esg_silver`), still restricted.
- Join it in gold for attribution and scores.
- Do not create an unrestricted copy “for convenience”.
- Pipeline identity needs access; general gold consumers may see derived financed emissions, not necessarily raw vendor fields.

Configured in `conf/config.yaml` (`schemas.esg_restricted`) and `conf/sources.yaml` (`restricted: true`, `bronze_schema: esg_restricted`).

## Consequences

- Jobs fail closed if the principal cannot read `esg_restricted` ([05_runbook.md](../05_runbook.md) failure 8).
- Analysts debugging R01 without a grant cannot “just select * from msci”. That is the point.
- Gold currently carries some vendor columns (scopes, fossil flag, rating) on `financed_emissions` so reports can score and flag. A production licence review may require dropping or hashing those columns on the open gold table and keeping them only in restricted.

## Alternatives rejected

- **Put MSCI in bronze and rely on folder permissions** — local demo has no real ACL; the schema split is the portable control.
- **Hash issuer names in bronze and keep emissions in gold** — still redistributes licensed metrics.
- **Call the vendor API from each report** — slow, not reproducible, worse for recon.
