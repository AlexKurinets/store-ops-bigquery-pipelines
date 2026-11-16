# Runbook

Commands are shown for the local stack (`docker compose run --rm pipeline ...`). In production the same commands run
as Cloud Run jobs: `gcloud run jobs execute storeops-<source> --args=...`.

## Every morning

Cloud Scheduler starts one job per source at the cron in its contract (`contracts/*.yaml`, summarized in
[freshness.md](freshness.md)). Each job has one automatic task retry. dbt and the checks run at 07:15 and 09:15 CT.
Alerts go to Slack, and every attempt is in `ops.pipeline_runs`.

What to look at first: the dashboard's Pipeline health page, or

```
storeops check freshness
```

## A run failed

1. Read the alert and the run's `error` in `ops.pipeline_runs` (the dashboard's "Runs that needed a person" table).
2. If the source was down (HTTP 5xx after five attempts, a connection error), wait until it answers and rerun:

   ```
   storeops run pos --date 2026-09-23 --trigger manual_rerun
   ```

   A rerun is always safe: the load replaces that business date's partition, it never appends to it.
3. If several days are missing, backfill. It runs one date at a time and stops at the first failure:

   ```
   storeops backfill pos --from 2026-09-20 --to 2026-09-23
   ```

4. After loading, rebuild and recheck: `storeops transform`, then `storeops check reconciliation`.

## Source-specific notes

**pos** (Cloud POS, OAuth2). A 401 mid-run is handled (the token is refreshed and the same page re-read). Repeated
401s mean the client secret was rotated on the POS side: update `pos-client-secret` in Secret Manager; the next run
picks it up, no deploy needed. Each run reloads three business days, so a terminal that was offline for a day is
picked up automatically. Longer than that needs a backfill.

**timekeeping** (OData). Identity fields are never requested (`$select` is built from the contract). If the vendor
renames a field, the run fails on a 400 naming the field; update the contract. The job runs at 06:15 CT, after this system's
own 05:00-local sales import has run in both time zones, so the reconciliation can cover the latest day. If an import
is late, the reconciliation stops at the last date whose import is loaded.

**distributor** (SFTP). A file that doesn't match the manifest's SHA-256 or row count is rejected and alerted; the
distributor resends it as `_v2`, and the next run (or a rerun for that date) loads it. A missing manifest fails the
run: the drop hasn't arrived. The SFTP host key is pinned (`sftp-host-key-pub`); if the distributor changes servers,
confirm the new fingerprint with them by phone before updating it.

**audits** (REST, API key). Comments are masked before landing. If the post-load scan quarantines a partition
(`pii_detected`), a new free-text field has appeared or a pattern slipped past masking: look at the scan row in
`ops.pii_scans`, fix the contract, and rerun the date. The quarantined partition is already deleted.

**budgets** (workbooks in a bucket). Finance re-uploads to correct a workbook; the new object generation is loaded
and becomes the month's budget version 2. A store name that doesn't map shows up in `conformed.unmapped_keys` and
fails the `assert_every_budget_row_maps` dbt test: add the alias to `transform/seeds/store_xref.csv`.

## The reconciliation is below 99%

Open the dashboard's Key reconciliation page. Each mismatched store-day has a reason:

- `pos_changed_after_labor_import`: POS tickets arrived, or were voided, after the timekeeping system's 05:00 import.
  The POS figure is the correct one; nothing to fix.
- `missing_in_labor_system`: the timekeeping system has no imported sales for that store and day. Ask the HR
  systems owner to rerun the import; the next timekeeping run picks it up.
- `missing_in_pos`: POS has no sales for a day the timekeeping system does. Check for an unmapped POS store number
  in `conformed.unmapped_keys`.
- `unexplained`: compare the two systems' figures by hand. Leave it marked unexplained until someone has.

Several stores missing on the same day usually means an unmapped or renumbered code: add a crosswalk row with
`valid_from` and `valid_to` rather than editing the old one, so history stays on one `store_key`.

## Adding a source

1. Write `contracts/<source>.yaml`: every field with a class. Anything personal is `drop`, `hash`, `mask` or
   `restrict`; `test_every_personal_field_is_never_kept_as_is` enforces it.
2. Add a connector in `src/storeops/connectors/` that returns raw records per contract table, and register it in
   `connectors/__init__.py`.
3. Add a staged model, the conformed fact, and crosswalk rows for any new store or item codes.
4. Run `storeops docs` to regenerate `docs/freshness.md`, then the tests.
5. Terraform creates the Cloud Run job and Scheduler trigger from the new contract file on the next apply.

## Rotating a secret

Add a new version in Secret Manager. Jobs read the latest version when they start. Disable the old version once a
run has succeeded with the new one.
