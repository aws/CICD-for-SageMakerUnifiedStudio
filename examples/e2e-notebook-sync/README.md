# E2E Notebook Sync Example

End-to-end test for the SMUS CI/CD notebook export and sync feature.

## Overview

This example tests the full notebook lifecycle:
1. **Setup** — Creates 4 notebooks in the DEV environment with varying configurations
2. **Bundle** — Exports only 3 of those notebooks (filtered by `notebook_ids`)
3. **Deploy** — Syncs the exported notebooks to TEST and PROD environments

> **Note:** Native SMUS notebook sync only supports the **bundle-deploy** mode
> (`bundle` from source → `deploy` to target). Direct deploy without a bundle is
> not supported for notebooks because the bundle export captures source notebook
> IDs, metadata, and parameters needed for reliable create/update tracking across
> environments.

Two variants are included:
- **IAM domain** (`manifest.yaml`) — uses tag-based domain resolution
- **IdC domain** (`manifest-idc.yaml`) — uses name-based domain resolution

## Notebooks Created by Setup

| Name | Parameters | Environment Config | In Filter |
|------|-----------|-------------------|-----------|
| `data-prep-etl` | 4 params (input_path, output_path, refresh_mode, partition_keys) | sagemaker-distribution-v2 + pip packages | Yes |
| `feature-engineering` | 3 params (feature_group, lookback_days, target_column) | None | Yes |
| `model-training` | None | sagemaker-distribution-v2 + pip packages | Yes |
| `exploratory-analysis` | None | None | **No** (excluded) |

## Prerequisites

1. Ensure DEV, TEST, and PROD environments are provisioned with the appropriate domains and projects.
2. Set environment variables:
   - `AWS_ACCOUNT_ID`
   - `DEV_DOMAIN_REGION`
   - `TEST_DOMAIN_REGION`
   - `PROD_DOMAIN_REGION`

## Setup (One-time, Manual)

```bash
# IAM domain:
python examples/e2e-notebook-sync/setup_notebooks.py \
    --manifest examples/e2e-notebook-sync/manifest.yaml \
    --stage dev

# IdC domain:
python examples/e2e-notebook-sync/setup_notebooks.py \
    --manifest examples/e2e-notebook-sync/manifest-idc.yaml \
    --stage dev-idc
```

After running, copy the printed notebook IDs into the respective manifest's
`content.notebooks.notebook_ids` list.

## Running the Workflow

The GitHub workflow (`.github/workflows/e2e-notebook-sync.yml`) triggers on
pushes to `main` that modify files under `examples/e2e-notebook-sync/` or `src/`.

It runs both the IAM and IdC variants in parallel using the reusable
`smus-bundle-deploy.yml` workflow.

## What It Tests

- Notebook export with `notebook_ids` filtering (3 of 4 notebooks)
- Fail-fast ID validation (invalid IDs abort before any export)
- Bundle packaging of `.ipynb` files + manifest
- Notebook sync to target projects (create and update paths)
- Metadata tracking via `smus-cicd-source-notebook-id`
- Parameter and environment configuration preservation across sync
- Idempotency (re-running sync updates existing notebooks)
