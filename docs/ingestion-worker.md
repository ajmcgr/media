# Apify to Supabase Ingestion Worker

## How it works

This worker pulls dataset items from one or more Apify datasets, normalizes them into `journalist` and `creators` records, dedupes them locally, and then writes them to Supabase in batches.

It is designed to bypass Bubble, Make, and the Supabase SQL editor for bulk ingestion.

## Required secrets and env vars

The worker reads:

- `APIFY_TOKEN`
- `APIFY_DATASET_IDS`
- `APIFY_TASK_IDS` optional comma-separated Apify saved task ids
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `MAX_RECORDS_PER_RUN` default `1000`
- `BATCH_SIZE` default `250`
- `DRY_RUN` default `true`

## How to add dataset IDs

Set `APIFY_DATASET_IDS` as a comma-separated list of dataset ids.
Set `APIFY_TASK_IDS` as a comma-separated list of saved task ids when you want the worker
to automatically ingest the latest successful dataset produced by those tasks.

Example:

```bash
APIFY_DATASET_IDS=abc123,def456,ghi789
APIFY_TASK_IDS=R6TlqXbiPu3Rz5ySe
```

## How to run a dry run locally

Export secrets in your shell, then run:

```bash
npm install
npm run ingest:dev
```

Or build first and run the compiled version:

```bash
npm install
npm run build
npm run ingest
```

To stay in dry-run mode:

```bash
export DRY_RUN=true
```

## How to run live locally

Set:

```bash
export DRY_RUN=false
```

Then run:

```bash
npm run ingest:dev
```

## How to run in GitHub Actions

Open the `Ingest Apify To Supabase` workflow and use `Run workflow`.

For the first manual test:

- keep `dry_run=true`
- keep `max_records_per_run=1000`
- keep `batch_size=250`

For a live run:

- set `dry_run=false`

Scheduled runs default to `DRY_RUN=false`, so once secrets are configured and the workflow file is on `main`, the cron job will run live every 15 minutes unless you disable the schedule.

## How to disable the schedule

Edit `.github/workflows/ingest-apify.yml` and remove or comment out:

```yaml
schedule:
  - cron: "*/15 * * * *"
```

Then commit and push the workflow change.

## Expected logs

Each run prints:

- dataset id
- records pulled
- records normalized
- malformed records
- duplicates skipped
- journalist rows attempted
- creator rows attempted
- rows written
- errors
- runtime seconds

Dry-run mode also prints up to 5 sample normalized records.

## Rollback instructions

This worker does not make schema changes.

To roll back operationally:

1. Set `DRY_RUN=true` for manual runs.
2. Disable the GitHub Actions schedule.
3. Revert the worker commit in Git.
4. If a live run inserted bad rows, clean those rows with your normal data-remediation process outside this worker.

## Notes and assumptions

- The worker assumes the `journalist` and `creators` tables already exist.
- It assumes the mapped columns are valid table columns.
- It assumes unique constraints already exist for key upsert columns if you want database-level upsert behavior on those keys.
- Composite fallback dedupe (`name + outlet`, `name + website`) is handled in application logic before insert.
