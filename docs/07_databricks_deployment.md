# Databricks deployment

The Python in `src/xpankki_esg/` is written so that moving to Azure Databricks is a **configuration and platform** exercise, not a rewrite of PCAF. This page says exactly what to change, what the bank must provide, and what this laptop demo deliberately does not implement.

## For business readers

On the laptop, reports appear as files in `data/output/`. In the bank they should appear as tables a control function can query, with licensed MSCI data only visible to people and jobs that are allowed to see it. Scheduling, access control, secrets and CI/CD are Azure/Databricks work. They are out of scope here so the demo stays runnable without cloud credentials.

## What you change

| Topic | This demo | Databricks |
|---|---|---|
| Environment switch | `conf/config.yaml` → `environment: local` | `environment: databricks` or env `XPANKKI_ENV=databricks` |
| Table names | Folder `data/lakehouse/silver/holdings` | `esg_prod.silver.holdings` |
| Restricted MSCI | Folder `data/lakehouse/esg_restricted/` | Catalog schema `esg_prod.esg_restricted` with **separate grants** |
| Landing files | `data/landing/*.csv` | Volume e.g. `/Volumes/esg_prod/landing/sas_extracts` |
| Report CSV | `data/output/R0N.csv` | Volume `/Volumes/esg_prod/output` plus gold tables |
| Spark session | Built in `spark_session.py` with Delta extension | **Reuse** the cluster session (`DATABRICKS_RUNTIME_VERSION` is set) |
| As-of date | `--as-of 2025-12-31` | Same CLI flag on the job task |

`config.py` already implements the name switch. Do not scatter `if databricks` through report code.

Suggested Unity Catalog layout (names are placeholders — replace with the bank’s catalog policy):

```text
esg_prod
├── bronze          # raw extracts + audit columns
├── silver          # cleaned + _rejects + issuer_entity_map
├── gold            # financed emissions, intensities, report tables
├── esg_restricted  # msci_esg and msci_esg_silver only
├── landing         # Volume for SAS CSVs
├── baselines       # Volume for frozen SAS exports
└── output          # Volume for CSV packs
```

## Cluster

- Runtime in the same family as **Spark 3.5** (this repo pins `pyspark==3.5.3` and `delta-spark==3.2.1`). Databricks 14.x / 15.x class is the intended match; confirm the exact DBR with the platform team.
- Job cluster is enough; no all-purpose cluster is required to *run* the pipeline.
- Photon is optional. Do not write Photon-only functions; stay on the DataFrame API used here.
- Single-node is fine for this synthetic volume. Production extracts need sizing from data engineering after the first real load.

Libraries on the cluster: `PyYAML` (and the project source). `pyspark` and Delta are already on DBR — **do not** `pip install pyspark` on a Databricks cluster.

## Job orchestration

Intended Workflows job (not checked in as a JSON job file — see “not implemented”):

1. **Task generate** — only in the demo. In production, omit; SAS already dropped files.
2. **Task bronze** — `python -m xpankki_esg.pipeline run --layer bronze --as-of {{as_of}}`
3. **Task silver** — depends on bronze (`--layer silver`)
4. **Task gold** — depends on silver
5. **Task reports** — depends on gold (or `--layer all` as one task for v1)
6. **Task recon** — depends on reports; fail the job on unexpected FAIL

Schedule: month-end + N operational days, aligned with the SAS DW extract SLA.

Parameter `as_of` should be a job parameter, not a hardcoded notebook widget, so Finance can rerun 31 March without editing code.

## Secrets and identity

This demo has **no secrets**. Production will.

| Secret | Used for | Store |
|---|---|---|
| MSCI (or other vendor) API / file credentials | If extracts stop being SAS-DW CSVs | Databricks secret scope; never in git |
| Storage / volume access | Usually UC + Azure AD, not a key in code | Workspace identity / managed identity |
| Git token for CI | Deploying this repo to the workspace | Azure DevOps / GitHub Actions secret |

The job should run as a **service principal** that can read landing + restricted schema and write bronze/silver/gold. Human analysts get `SELECT` on gold, not on `esg_restricted`, unless they are in the licensed user group.

## Access control on the restricted schema

This is the control a vendor audit will ask about.

- `esg_restricted`: `USAGE` on catalog, `USAGE` + `SELECT` on schema only for (a) the pipeline principal, (b) named ESG data stewards.
- Do not grant `ALL PRIVILEGES` to analysts “so they can debug”. Debug with sampled, approved extracts.
- Gold **may** contain financed emissions (derived). It must **not** become a dump of raw vendor fields if the licence forbids redistribution. Today gold keeps some MSCI columns on `financed_emissions` for scoring; a production design should project only what the licence allows (see ADR 0002).

Unity Catalog row filters / column masks are **not** implemented here.

## CI/CD

Not implemented in this repository (no GitHub Action, no Databricks asset bundle).

A reasonable v1, once the bank’s platform is chosen:

1. On pull request: `make test` on a runner with JDK 17 (same as local).
2. On merge to `main`: `databricks bundle deploy` or copy `src/` + `conf/` to a workspace repo.
3. Do not run `make generate` in CI against production catalogs.
4. Integration test: optional job in a `esg_dev` catalog with the synthetic generator.

`databricks.yml` / DABs are omitted so this project stays runnable with four pip packages and no cloud login.

## Notebooks

`notebooks/00_run_full_pipeline.py` and `01_explore_gold_layer.py` are thin wrappers. The source of truth is the CLI. On Databricks you may `%run` or call the same `python -m xpankki_esg.pipeline` from a notebook task. Do not fork logic into notebooks.

## What is not implemented here, and why

| Missing piece | Why it is absent |
|---|---|
| Azure account, Unity Catalog, actual grants | Demo must run offline |
| Real MSCI licence feed | Replaced by synthetic `msci_esg.csv` |
| Real SAS exports as baselines | Generator mutates pipeline output; documented in [06_reconciliation.md](06_reconciliation.md) |
| ADF / Autoloader streaming | Extracts are batch CSVs, like the SAS DW drop |
| Databricks Workflows JSON / Asset Bundles | Would require a workspace; Makefile is the local analogue |
| Secret scopes, Azure Key Vault | No credentials in the demo |
| Photon, Delta Live Tables, CDC | Would hide the SAS-to-Spark teaching path |
| Full PCAF 1a–5 option set, SFDR templates, taxonomy DNSH | Marked `SIMPLIFIED` so nobody mistakes the demo for a disclosure engine |
| Automated tests in CI | Tests exist under `tests/` but are not wired to GitHub Actions yet |

When those items are added, keep them **outside** the report modules. The rule is still: configuration and platform change; R01’s business question does not.
