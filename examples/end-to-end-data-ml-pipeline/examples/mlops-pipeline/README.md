# MLOps Pipeline — Bank Marketing Classification

End-to-end ML pipeline that trains an XGBoost binary classifier with SageMaker, registers it in the Model Registry, and deploys it to a real-time endpoint on approval.

**Prerequisite:** The [DataOps pipeline](../dataops-pipeline/) must run first to create the `bank_mktg_<stage>.campaign_results` table.

## Architecture

Two workflows make up the MLOps flow:

**`training_pipeline`** — trains and registers a model version:

```text
preprocess (GlueJobOperator)
  → train_model (SageMakerTrainingOperator)
    → evaluate_model (SageMakerProcessingOperator)
      → register_model (SageMakerRegisterModelVersionOperator → PendingManualApproval)
```

**`deploy_pipeline`** — deploys the approved model to an endpoint (event-driven):

```text
deploy_model (SageMakerProcessingOperator → bank-mktg-prediction-<stage> endpoint)
```

| Step | Operator | Description |
| ---- | -------- | ----------- |
| `preprocess` | GlueJobOperator | Feature engineering from the DataOps Athena table; train/val/test split |
| `train_model` | SageMakerTrainingOperator | XGBoost training via `train_xgboost.py` (script mode) with MLflow tracking |
| `evaluate_model` | SageMakerProcessingOperator | Batch evaluation on the held-out test set (`evaluate_model.py`) |
| `register_model` | SageMakerRegisterModelVersionOperator | Registers a version in `bank-mktg-prediction-models` as `PendingManualApproval` |
| `deploy_model` | SageMakerProcessingOperator | Creates/updates the prediction endpoint (`deploy_model.py`) |

Model **approval** drives deployment: approving a version fires EventBridge → Lambda → GitHub Actions → `deploy_pipeline`. See the [parent README](../../README.md#deploy-trigger-behavior) for details.

## Prerequisites

```bash
pip install aws-smus-cicd-cli
export DEV_DOMAIN_REGION=<your-region>      # required
export DEV_PROJECT_NAME=<your-dev-project>  # optional (default: e2e-data-ml-ops-dev)
```

An MLflow tracking server must exist (the manifest bootstrap creates the MLflow connection; `MLFLOW_TRACKING_SERVER_NAME` defaults to `smus-integration-mlflow-us-east-1`). Account ID and domain are resolved at runtime — see the [parent e2e example README](../../README.md#prerequisites) for the full variable list and CI/CD setup.

## Deploy and Run

```bash
# Validate connectivity
aws-smus-cicd-cli describe --manifest manifest.yaml --targets dev --connect

# Deploy code and workflows to S3 + MWAA
aws-smus-cicd-cli deploy --manifest manifest.yaml --targets dev

# Run the training pipeline
aws-smus-cicd-cli run --manifest manifest.yaml --targets dev --workflow training_pipeline

# Monitor execution
aws-smus-cicd-cli monitor --manifest manifest.yaml --targets dev --live
```

> The `train_model` task downloads `train_model_sourcedir.tar.gz` from the project's shared S3 location. The CLI packages this automatically during `deploy` from the manifest's `train_model_sourcedir` storage item (`compression: tar.gz`, bundling `train_xgboost.py` + `requirements.txt`) — no separate build step is needed. Each script-mode SageMaker job gets its own per-job bundle named after the task that consumes it.

## Project Structure

```text
mlops-pipeline/
├── manifest.yaml                        # Deployment manifest (dev/test/prod stages)
├── workflows/
│   ├── training_pipeline.yaml           # preprocess → train → evaluate → register
│   └── deploy_pipeline.yaml             # deploy_model → endpoint (event-driven)
└── src/
    ├── feature_engineering.py           # Glue preprocess job
    ├── train_xgboost.py                 # XGBoost training script (SageMaker script mode)
    ├── evaluate_model.py                # Batch evaluation processing script
    ├── deploy_model.py                  # Endpoint deploy processing script
    ├── requirements.txt                 # Training container deps (scikit-learn pinned to container version)
    └── notebooks/
        ├── evaluate_model.ipynb         # Interactive evaluation
        └── validate_mlops.ipynb         # Post-pipeline validation
```

## Resource Naming

Stage-prefixed, timestamped names keep runs isolated across stages and reruns:

```text
Training job     : bank-mktg-xgb-<timestamp>
Model package    : bank-mktg-prediction-models (registry group, versioned)
Endpoint         : bank-mktg-prediction-<stage>   (e.g. bank-mktg-prediction-dev)
```

## Validation

After the pipeline completes, use `src/notebooks/validate_mlops.ipynb` to verify the training job status, the registered model version, endpoint health (`InService`), and a sample inference.

## Troubleshooting

**Training fails importing scikit-learn (`KeyError: '__reduce_cython__'`):** `requirements.txt` pins `scikit-learn` to the container's preinstalled version to avoid a numpy/Cython ABI mismatch — don't bump it past what the `sagemaker-xgboost` container ships.

**Endpoint already exists:** the deploy step updates the existing endpoint in place when one with the same name exists.

**No promote after approval:** the promote workflow runs from the default branch via `repository_dispatch`, and the repo must have **Issues enabled** for the approval gates. See the [parent README](../../README.md#cicd).
