# Staging Mode for Unity Catalog Governance

## Why this exists

The default operation of the accelerator writes a Delta `_delta_log` whose
AddActions reference parquet files at **absolute** `s3a://...` paths. That
works correctly when the source bucket is registered as a Unity Catalog
external location. But in the typical Guidewire CDA SaaS deployment, the
source bucket is owned by Guidewire's AWS account and the customer holds
only a cross-account read-only IAM grant. UC has no governance reach over
that bucket.

Without staging:
- Reads of the resulting Delta table follow the absolute paths using
  whatever IAM identity the engine has, **bypassing UC**.
- On serverless compute, those reads fail closed (no IAM fallback).
- On classic compute with a broad cluster instance profile, the reads
  silently succeed without UC audit, lineage, or policy enforcement.

Staging mode closes the gap by **copying source parquets into a customer-
owned bucket** that *can* be a UC external location. The Delta log is then
written with paths relative to the staged target root, and reads stay
inside UC.

## When to use it

| Deployment | Staging needed? |
|---|---|
| Guidewire CDA SaaS (Guidewire-owned source bucket) | Yes |
| Self-hosted CDA (customer owns source bucket) | No -- pass `target_storage=UCStorage(...)` without `staging_mode=True` |
| No Unity Catalog | No -- omit `target_storage` entirely; legacy behavior is preserved |

## Setup

1. **Create the customer-owned target bucket** that will receive staged
   parquets. Suggested layout:

   ```
   s3://customer-uc-bucket/staged-cda/
   ```

2. **Register the bucket prefix as a UC external location** in your
   workspace, with a UC storage credential whose IAM role has
   `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, and
   `s3:ListBucket` on the prefix.

3. **Pre-create the UC tables** under `<catalog>.<schema>` -- one per CDA
   table you want to land. The accelerator looks up the UC `table_id` for
   each table at startup; tables that don't exist will fail loudly with
   an actionable error.

   ```sql
   CREATE CATALOG IF NOT EXISTS main;
   CREATE SCHEMA IF NOT EXISTS main.guidewire_cda;
   CREATE TABLE main.guidewire_cda.cc_claim
       LOCATION 's3://customer-uc-bucket/staged-cda/cc_claim/';
   -- ... one per table
   ```

4. **Configure the accelerator** via env vars (or pass directly to
   `Processor`):

   ```bash
   export AWS_MANIFEST_LOCATION=s3://guidewire-bucket/customer-id
   export AWS_SOURCE_REGION=us-east-1
   export AWS_SOURCE_ACCESS_KEY_ID=...      # Guidewire's cross-account read grant
   export AWS_SOURCE_SECRET_ACCESS_KEY=...

   export DELTA_TARGET_CLOUD=aws
   export AWS_TARGET_S3_BUCKET=customer-uc-bucket
   export AWS_TARGET_S3_PREFIX=staged-cda

   # Staging-mode opt-in
   export STAGING_MODE=true
   export UC_CATALOG_NAME=main
   export UC_SCHEMA_NAME=guidewire_cda
   export UC_REGION=us-east-1
   ```

   ```bash
   python main.py
   ```

## Configuration reference

| Env var | Required | Purpose |
|---|---|---|
| `STAGING_MODE` | yes (set `true`) | Enables staging |
| `UC_CATALOG_NAME` | yes | UC catalog containing the target tables |
| `UC_SCHEMA_NAME` | yes | UC schema (database) within the catalog |
| `UC_REGION` | no | AWS region for vended creds; falls back to `AWS_REGION` |
| `DATABRICKS_HOST` | yes | Where to vend UC credentials from |
| `DATABRICKS_TOKEN` | yes | Auth for the WorkspaceClient |

The Databricks SDK's default auth chain applies, so any of the standard
mechanisms (host+token, OAuth, profile) work in place of explicit env vars.

## What staging does, step by step

For each table in the manifest:

1. Resolve the UC `table_id` for `<catalog>.<schema>.<table_name>`.
2. Vend short-lived AWS credentials via the SDK's
   `temporary_table_credentials` API. These are scoped to the UC table's
   storage credential -- not your account-wide IAM.
3. List source parquets for each new timestamp folder (driven by the
   existing watermark mechanism -- no re-staging of already-processed
   timestamps).
4. For each parquet:
   - `head_object` on the target. If a file of the same size already
     exists, skip the copy.
   - Otherwise stream the bytes from source to target via PyArrow's S3
     filesystem.
5. Write the AddAction with a **path relative to the table root** -- e.g.
   `<schema_hash>/<timestamp>/part-0.parquet`. This is required for UC
   governance to track the file.
6. Commit one Delta transaction per timestamp folder, advancing the
   watermark. A failed copy aborts the transaction *before* the watermark
   advances; restart resumes cleanly.

## Limitations (v1)

- **AWS source -> AWS UC target only.** Cross-cloud staging (Azure CDA ->
  AWS UC, etc.) raises `NotImplementedError` in `StagingExecutor`.
- **Sequential within-table copy.** Cross-table parallelism is preserved
  via Ray; within a single table, files are staged serially. Adequate for
  typical CDA volumes; can be parallelized in a follow-on.
- **Idempotency uses size match.** If a previous run failed mid-stream
  and left a partial parquet at the target whose byte count happens to
  match the source, the next run will skip it. The window is small
  (single file's copy duration) and the typical recovery is to delete
  any stale targets manually before re-running. A future hardening pass
  could use a temp-key + atomic-rename pattern.
- **Pre-create UC tables.** The accelerator does not create UC tables;
  it only resolves their `table_id` for credential vending. Customers
  must register tables with appropriate `LOCATION` clauses ahead of
  the first run.

## Programmatic API (without env vars)

For customers wiring the accelerator into a custom workflow:

```python
from guidewire import Processor, UCStorage, Batch

processor = Processor(
    target_cloud="aws",
    table_names=("cc_claim", "cc_exposure", "cc_check"),
    parallel=False,
    staging_mode=True,
    uc_catalog="main",
    uc_schema="guidewire_cda",
    uc_region="us-east-1",
)
processor.run()
```

Or per-batch, with explicit `UCStorage`:

```python
uc = UCStorage(uc_table_id="<resolved-table-id>", region="us-east-1")
batch = Batch(
    table_name="cc_claim",
    manifest=manifest,
    target_cloud="aws",
    storage_or_s3_name="customer-uc-bucket",
    storage_container=None,
    target_storage=uc,
    staging_mode=True,
)
batch.process_batch()
```

## Backward compatibility

Staging mode is fully opt-in. With no `staging_mode` and no
`target_storage` passed, every code path in this module is unreachable;
the accelerator emits absolute `s3a://...` AddAction paths exactly as
the upstream did. See `testing/tests/test_target_cloud_config.py` and
`testing/tests/test_staging_unit.py` for explicit regression-guard
tests covering the legacy paths.
