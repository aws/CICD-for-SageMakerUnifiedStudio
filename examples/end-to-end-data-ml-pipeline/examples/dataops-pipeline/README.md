# DataOps Pipeline — Bank Marketing Data Processing

End-to-end data operations pipeline using SMUS CLI: ingest raw bank marketing data, run Glue ETL jobs for cleansing and transformation, execute data quality checks, and register the output as a queryable Athena table.

## Architecture

```text
ingest_bank_data (Glue)
  → transform_aggregate (Glue)
    → data_quality_checks (Glue)
      → create_athena_database (Athena)
        → create_athena_table (Athena)
          → refresh_athena_table (Athena)
```

**Output:** `bank_mktg_dev.campaign_results` table in the Glue Catalog, ready for ML training.

## Prerequisites

```bash
pip install aws-smus-cicd-cli
export DEV_DOMAIN_REGION=<your-region>      # required
export DEV_PROJECT_NAME=<your-dev-project>  # optional (default: e2e-data-ml-ops-dev)
```

Account ID and domain are resolved at runtime, and the rest have defaults — see the [parent e2e example README](../../README.md#prerequisites) for the full variable list and CI/CD setup.

## Deploy and Run

```bash
# Validate connectivity
aws-smus-cicd-cli describe --manifest manifest.yaml --targets dev --connect

# Deploy code and workflows to S3 + MWAA
aws-smus-cicd-cli deploy --manifest manifest.yaml --targets dev

# Trigger the pipeline
aws-smus-cicd-cli run --manifest manifest.yaml --targets dev --workflow data_pipeline

# Monitor execution
aws-smus-cicd-cli monitor --manifest manifest.yaml --targets dev --live
```

## Project Structure

```text
dataops-pipeline/
├── manifest.yaml                    # Deployment manifest
├── workflows/
│   └── data_pipeline.yaml           # Airflow workflow definition
├── src/glue-jobs/
│   ├── glue_ingest_raw_data.py      # Reads CSV, writes Parquet to staging
│   ├── glue_transform_aggregate.py  # Adds age_group, contacted_before columns
│   └── glue_data_quality_checks.py  # Validates null rates, row counts
└── data/
    └── bank-mktg-sample.csv         # Sample dataset (~41K rows)
```

## How It Works

1. **Ingest** — reads raw CSV from S3, writes to staging as Parquet
2. **Transform** — aggregates and engineers features (age groups, contact history)
3. **Quality Checks** — validates schema, null rates, and value distributions
4. **Athena** — creates database/table and refreshes partitions for SQL access

The workflow YAML uses `{proj.connection.default.s3_shared.s3Uri}` placeholders that resolve to the project's S3 bucket at deploy time.

## Troubleshooting

**Workflow not found:** Ensure `workflowName` in the manifest matches the top-level key in `workflows/data_pipeline.yaml`.

**Glue job failures:** Check the Glue console or CloudWatch logs. Common issues:

- Missing IAM/Lake Formation permissions on the Glue Catalog database
- Invalid S3 paths (check that deploy uploaded files correctly)

**Athena table creation fails:** The execution role needs Lake Formation `Describe` and `CreateTable` permissions on the target database.
