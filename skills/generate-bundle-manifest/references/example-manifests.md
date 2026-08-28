# Example Manifests

Complete working examples for common deployment patterns.

## 1. DataOps Pipeline (ETL + Glue + Athena)

Full data processing pipeline with ingestion, transformation, quality checks, and catalog refresh.

```yaml
applicationName: BankMktgDataOps

content:
  storage:
    - name: src
      include:
        - "src/"
      exclude:
        - .ipynb_checkpoints/
        - __pycache__/
        - "*.pyc"
        - .libs.json
    - name: workflows
      include:
        - "workflows/"
      exclude:
        - .ipynb_checkpoints/
        - __pycache__/
        - "*.pyc"
        - .libs.json
    - name: data
      include:
        - "data/"
      exclude:
        - .ipynb_checkpoints/
        - __pycache__/
        - "*.pyc"
        - .libs.json
  workflows:
    - workflowName: data_pipeline
      connectionName: default.workflow_serverless

stages:
  dev:
    stage: DEV
    domain:
      tags:
        purpose: ${DOMAIN_TAG_PURPOSE:smus-cicd-testing}
      region: ${DEV_DOMAIN_REGION}
    project:
      name: ${DEV_PROJECT_NAME:dataops-dev}
    environment_variables:
      ATHENA_DB_NAME: bank_mktg_dev
    bootstrap:
      actions:
        - type: datazone.create_environment
          environmentConfigurationName: "Lakehouse Database"
        - type: workflow.create
          workflowName: data_pipeline
    deployment_configuration:
      storage:
        - name: src
          connectionName: default.s3_shared
          targetDirectory: bank-mktg/data_pipeline/src
        - name: workflows
          connectionName: default.s3_shared
          targetDirectory: bank-mktg/data_pipeline/workflows
        - name: data
          connectionName: default.s3_shared
          targetDirectory: bank-mktg/data_pipeline/data

  test:
    stage: TEST
    domain:
      tags:
        purpose: ${DOMAIN_TAG_PURPOSE:smus-cicd-testing}
      region: ${TEST_DOMAIN_REGION:us-east-1}
    project:
      name: ${TEST_PROJECT_NAME:dataops-test}
    environment_variables:
      ATHENA_DB_NAME: bank_mktg_test
    bootstrap:
      actions:
        - type: datazone.create_environment
          environmentConfigurationName: "Lakehouse Database"
        - type: workflow.create
          workflowName: data_pipeline
        - type: workflow.run
          workflowName: data_pipeline
          trailLogs: true
    deployment_configuration:
      storage:
        - name: src
          connectionName: default.s3_shared
          targetDirectory: bank-mktg/data_pipeline/src
        - name: workflows
          connectionName: default.s3_shared
          targetDirectory: bank-mktg/data_pipeline/workflows
        - name: data
          connectionName: default.s3_shared
          targetDirectory: bank-mktg/data_pipeline/data

  prod:
    stage: PROD
    domain:
      tags:
        purpose: ${DOMAIN_TAG_PURPOSE:smus-cicd-production}
      region: ${PROD_DOMAIN_REGION:us-east-1}
    project:
      name: ${PROD_PROJECT_NAME:dataops-prod}
    environment_variables:
      ATHENA_DB_NAME: bank_mktg_prod
    bootstrap:
      actions:
        - type: datazone.create_environment
          environmentConfigurationName: "Lakehouse Database"
        - type: workflow.create
          workflowName: data_pipeline
    deployment_configuration:
      storage:
        - name: src
          connectionName: default.s3_shared
          targetDirectory: bank-mktg/data_pipeline/src
        - name: workflows
          connectionName: default.s3_shared
          targetDirectory: bank-mktg/data_pipeline/workflows
        - name: data
          connectionName: default.s3_shared
          targetDirectory: bank-mktg/data_pipeline/data
```

### Matching Orchestration Workflow (`workflows/data_pipeline.yaml`)

```yaml
data_pipeline:
  dag_id: data_pipeline
  schedule: "0 5 * * *"
  description: "Data ingestion, transformation, and quality pipeline"
  default_args:
    owner: data-engineering
    retries: 0

  tasks:
    ingest_raw_data:
      operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
      job_name: raw-data-ingest
      iam_role_name: "{proj.iam_role_name}"
      s3_bucket: "{proj.connection.default.s3_shared.bucket_name}"
      region_name: "{domain.region}"
      update_config: true
      script_location: "{proj.connection.default.s3_shared.s3Uri}bank-mktg/data_pipeline/src/glue-jobs/ingest.py"
      create_job_kwargs:
        GlueVersion: "{proj.connection.default.spark.glueVersion}"
        NumberOfWorkers: 2
        WorkerType: "{proj.connection.default.spark.workerType}"

    transform_data:
      operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
      job_name: data-transform
      iam_role_name: "{proj.iam_role_name}"
      s3_bucket: "{proj.connection.default.s3_shared.bucket_name}"
      region_name: "{domain.region}"
      update_config: true
      script_location: "{proj.connection.default.s3_shared.s3Uri}bank-mktg/data_pipeline/src/glue-jobs/transform.py"
      create_job_kwargs:
        GlueVersion: "{proj.connection.default.spark.glueVersion}"
        NumberOfWorkers: 4
        WorkerType: "{proj.connection.default.spark.workerType}"
      dependencies: [ingest_raw_data]

    quality_checks:
      operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
      job_name: data-quality
      iam_role_name: "{proj.iam_role_name}"
      s3_bucket: "{proj.connection.default.s3_shared.bucket_name}"
      region_name: "{domain.region}"
      update_config: true
      script_location: "{proj.connection.default.s3_shared.s3Uri}bank-mktg/data_pipeline/src/glue-jobs/quality_checks.py"
      create_job_kwargs:
        GlueVersion: "{proj.connection.default.spark.glueVersion}"
        NumberOfWorkers: 2
        WorkerType: "{proj.connection.default.spark.workerType}"
      dependencies: [transform_data]

    create_catalog_table:
      operator: airflow.providers.amazon.aws.operators.athena.AthenaOperator
      aws_conn_id: aws_default
      database: "{env.ATHENA_DB_NAME}"
      query: |
        CREATE EXTERNAL TABLE IF NOT EXISTS results (
          id INT, name STRING, value DOUBLE
        )
        STORED AS PARQUET
        LOCATION '{proj.connection.default.s3_shared.s3Uri}bank-mktg/data_pipeline/output/'
      output_location: "{proj.connection.default.s3_shared.s3Uri}bank-mktg/data_pipeline/athena_results/"
      dependencies: [quality_checks]
```

## 2. MLOps Pipeline (Training + Deployment)

ML model training with SageMaker operators and MLflow tracking.

```yaml
applicationName: MLOpsPipeline

content:
  storage:
    - name: src
      include: ["src/"]
      exclude: [".ipynb_checkpoints/", "__pycache__/", "*.pyc", ".libs.json"]
    - name: workflows
      include: ["workflows/"]
      exclude: [".ipynb_checkpoints/", "__pycache__/", "*.pyc", ".libs.json"]
  workflows:
    - workflowName: training_pipeline
      connectionName: default.workflow_serverless
    - workflowName: deploy_pipeline
      connectionName: default.workflow_serverless

stages:
  dev:
    stage: DEV
    domain:
      tags:
        purpose: ${DOMAIN_TAG_PURPOSE:smus-cicd-testing}
      region: ${DEV_DOMAIN_REGION}
    project:
      name: ${DEV_PROJECT_NAME:mlops-dev}
    environment_variables:
      ENDPOINT_NAME: prediction-endpoint-dev
      MLFLOW_TRACKING_SERVER_NAME: "${MLFLOW_TRACKING_SERVER_NAME}"
    bootstrap:
      actions:
        - type: datazone.create_environment
          environmentConfigurationName: "OnDemand Workflows"
        - type: datazone.create_connection
          name: mlflow-tracking
          connection_type: MLFLOW
          properties:
            trackingServerArn: "arn:aws:sagemaker:${DEV_DOMAIN_REGION}:${AWS_ACCOUNT_ID}:mlflow-tracking-server/${MLFLOW_TRACKING_SERVER_NAME}"
            trackingServerName: "${MLFLOW_TRACKING_SERVER_NAME}"
        - type: workflow.create
          workflowName: training_pipeline
        - type: workflow.create
          workflowName: deploy_pipeline
    deployment_configuration:
      storage:
        - name: src
          connectionName: default.s3_shared
          targetDirectory: mlops/src
        - name: workflows
          connectionName: default.s3_shared
          targetDirectory: mlops/workflows

  test:
    stage: TEST
    domain:
      tags:
        purpose: ${DOMAIN_TAG_PURPOSE:smus-cicd-testing}
      region: ${TEST_DOMAIN_REGION:us-east-1}
    project:
      name: ${TEST_PROJECT_NAME:mlops-test}
    environment_variables:
      ENDPOINT_NAME: prediction-endpoint-test
      MLFLOW_TRACKING_SERVER_NAME: "${MLFLOW_TRACKING_SERVER_NAME}"
    bootstrap:
      actions:
        - type: datazone.create_environment
          environmentConfigurationName: "OnDemand Workflows"
        - type: datazone.create_connection
          name: mlflow-tracking
          connection_type: MLFLOW
          properties:
            trackingServerArn: "arn:aws:sagemaker:${TEST_DOMAIN_REGION:us-east-1}:${AWS_ACCOUNT_ID}:mlflow-tracking-server/${MLFLOW_TRACKING_SERVER_NAME}"
            trackingServerName: "${MLFLOW_TRACKING_SERVER_NAME}"
        - type: workflow.create
          workflowName: deploy_pipeline
    deployment_configuration:
      storage:
        - name: src
          connectionName: default.s3_shared
          targetDirectory: mlops/src
        - name: workflows
          connectionName: default.s3_shared
          targetDirectory: mlops/workflows

  prod:
    stage: PROD
    domain:
      tags:
        purpose: ${DOMAIN_TAG_PURPOSE:smus-cicd-production}
      region: ${PROD_DOMAIN_REGION:us-east-1}
    project:
      name: ${PROD_PROJECT_NAME:mlops-prod}
    environment_variables:
      ENDPOINT_NAME: prediction-endpoint-prod
      MLFLOW_TRACKING_SERVER_NAME: "${MLFLOW_TRACKING_SERVER_NAME}"
    bootstrap:
      actions:
        - type: datazone.create_environment
          environmentConfigurationName: "OnDemand Workflows"
        - type: datazone.create_connection
          name: mlflow-tracking
          connection_type: MLFLOW
          properties:
            trackingServerArn: "arn:aws:sagemaker:${PROD_DOMAIN_REGION:us-east-1}:${AWS_ACCOUNT_ID}:mlflow-tracking-server/${MLFLOW_TRACKING_SERVER_NAME}"
            trackingServerName: "${MLFLOW_TRACKING_SERVER_NAME}"
        - type: workflow.create
          workflowName: deploy_pipeline
    deployment_configuration:
      storage:
        - name: src
          connectionName: default.s3_shared
          targetDirectory: mlops/src
        - name: workflows
          connectionName: default.s3_shared
          targetDirectory: mlops/workflows
```

## 3. Analytics Dashboard (QuickSight + ETL)

ETL pipeline feeding a QuickSight dashboard with cross-stage promotion.

```yaml
applicationName: SalesDashboard

content:
  storage:
    - name: etl-code
      connectionName: default.s3_shared
      include: ["*.py"]
      exclude: [".ipynb_checkpoints/", "__pycache__/", "*.pyc", ".libs.json"]
    - name: workflows
      connectionName: default.s3_shared
      include: ["*.yaml"]
      exclude: [".ipynb_checkpoints/", "__pycache__/", "*.pyc", ".libs.json", "manifest.yaml"]
  quicksight:
    - name: SalesDashboard
      type: dashboard
  workflows:
    - workflowName: data_processing_dag
      connectionName: default.workflow_serverless

stages:
  dev:
    stage: DEV
    domain:
      tags:
        purpose: smus-cicd-testing
      region: ${DEV_DOMAIN_REGION:us-east-1}
    project:
      name: dev-analytics
    bootstrap:
      actions:
        - type: workflow.create
          workflowName: data_processing_dag
    deployment_configuration:
      storage:
        - name: etl-code
          connectionName: default.s3_shared
          targetDirectory: etl/bundle
        - name: workflows
          connectionName: default.s3_shared
          targetDirectory: etl/bundle/workflows

  test:
    stage: TEST
    domain:
      tags:
        purpose: smus-cicd-testing
      region: ${TEST_DOMAIN_REGION:us-east-1}
    project:
      name: test-analytics
    environment_variables:
      S3_PREFIX: test
    bootstrap:
      actions:
        - type: workflow.create
          workflowName: data_processing_dag
        - type: workflow.run
          workflowName: data_processing_dag
          trailLogs: true
        - type: quicksight.refresh_dataset
          refreshScope: IMPORTED
          ingestionType: FULL_REFRESH
          wait: true
    deployment_configuration:
      storage:
        - name: etl-code
          connectionName: default.s3_shared
          targetDirectory: etl/bundle
        - name: workflows
          connectionName: default.s3_shared
          targetDirectory: etl/bundle/workflows
      quicksight:
        assets:
          - name: SalesDashboard
            owners:
              - arn:aws:quicksight:${TEST_DOMAIN_REGION}:${AWS_ACCOUNT_ID}:user/default/Admin/*
            viewers:
              - arn:aws:quicksight:${TEST_DOMAIN_REGION}:${AWS_ACCOUNT_ID}:user/default/Admin/*

  prod:
    stage: PROD
    domain:
      tags:
        purpose: smus-cicd-production
      region: ${PROD_DOMAIN_REGION}
    project:
      name: prod-analytics
    environment_variables:
      S3_PREFIX: prod
    bootstrap:
      actions:
        - type: workflow.create
          workflowName: data_processing_dag
        - type: workflow.run
          workflowName: data_processing_dag
          wait: true
        - type: quicksight.refresh_dataset
          refreshScope: IMPORTED
          ingestionType: FULL_REFRESH
          wait: true
          timeout: 600
    deployment_configuration:
      storage:
        - name: etl-code
          connectionName: default.s3_shared
          targetDirectory: etl/bundle
        - name: workflows
          connectionName: default.s3_shared
          targetDirectory: etl/bundle/workflows
      quicksight:
        assets:
          - name: SalesDashboard
            owners:
              - arn:aws:quicksight:${PROD_DOMAIN_REGION}:${AWS_ACCOUNT_ID}:user/default/Admin/*

tests:
  folder: ./tests
```

## 4. Minimal Manifest (Notebook Promotion)

Simple promotion of notebooks and scripts without workflows.

```yaml
applicationName: AnalyticsNotebooks

content:
  storage:
    - name: code
      connectionName: default.s3_shared
      include: ["*"]
      exclude:
        - .ipynb_checkpoints/
        - __pycache__/
        - "*.pyc"
        - .libs.json

stages:
  dev:
    stage: DEV
    domain:
      region: ${DEV_DOMAIN_REGION:us-east-1}
    project:
      name: dev-analytics
    deployment_configuration:
      storage:
        - name: code
          connectionName: default.s3_shared
          targetDirectory: src

  test:
    stage: TEST
    domain:
      region: ${TEST_DOMAIN_REGION:us-east-1}
    project:
      name: test-analytics
    deployment_configuration:
      storage:
        - name: code
          connectionName: default.s3_shared
          targetDirectory: src

  prod:
    stage: PROD
    domain:
      region: ${PROD_DOMAIN_REGION:us-east-1}
    project:
      name: prod-analytics
    deployment_configuration:
      storage:
        - name: code
          connectionName: default.s3_shared
          targetDirectory: src
```
