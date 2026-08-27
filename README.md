# SMUS CI/CD Pipeline CLI

[![en](https://img.shields.io/badge/lang-en-brightgreen.svg?style=for-the-badge)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/langs/pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/langs/fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/langs/it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/langs/ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/langs/zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/langs/he/README.md)

> **[IAM + IdC Domains]** This CLI supports both IAM-based and IAM Identity Center (IdC)-based SMUS domains. For IdC domains, additional setup (VPC networking, Lake Formation permissions, inline IAM policies) may be required — see the setup scripts in each example directory.

**Automate deployment of data applications across SageMaker Unified Studio environments**

Deploy Airflow DAGs, Jupyter notebooks, and ML workflows from development to production with confidence. Built for data scientists, data engineers, ML engineers, and GenAI app developers working with DevOps teams.

**Works with your deployment strategy:** Whether you use git branches (branch-based), versioned artifacts (bundle-based), git tags (tag-based), or direct deployment - this CLI supports your workflow. Define your application once, deploy it your way.

---

## Why SMUS CI/CD CLI?

✅ **AWS Abstraction Layer** - CLI encapsulates all AWS analytics, ML, and SMUS complexity - DevOps teams never call AWS APIs directly  
✅ **Separation of Concerns** - Data teams define WHAT to deploy (manifest.yaml), DevOps teams define HOW and WHEN (CI/CD workflows)  
✅ **Generic CI/CD Workflows** - Same workflow works for Glue, SageMaker, Bedrock, QuickSight, or any AWS service combination  
✅ **Deploy with Confidence** - Pre-deployment dry-run validation and automated testing before production  
✅ **Multi-Environment Management** - Test → Prod with environment-specific configuration  
✅ **Infrastructure as Code** - Version-controlled application manifests and reproducible deployments  
✅ **Event-Driven Workflows** - Trigger workflows automatically via EventBridge on deployment  

---

## Quick Start

**Install:**
```bash
pip install aws-smus-cicd-cli
```

**Deploy your first application:**
```bash
# Validate configuration
aws-smus-cicd-cli describe --manifest manifest.yaml --connect

# Create deployment bundle (optional)
aws-smus-cicd-cli bundle --manifest manifest.yaml

# Preview deployment (dry run)
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml --dry-run

# Deploy to test environment
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml

# Run validation tests
aws-smus-cicd-cli test --manifest manifest.yaml --targets test

# Clean up when done
aws-smus-cicd-cli destroy --manifest manifest.yaml --targets test --force
```

**See it in action:** [Live GitHub Actions Example](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/runs/24535194879)

---

## Who Is This For?

### 👨‍💻 Data Teams (Data Scientists, Data Engineers, GenAI App Developers)
**You focus on:** Your application - what to deploy, where to deploy, and how it runs  
**You define:** Application manifest (`manifest.yaml`) with your code, workflows, and configurations  
**You don't need to know:** CI/CD pipelines, GitHub Actions, deployment automation  

→ **[Quick Start Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/quickstart.md)** - Deploy your first application in 10 minutes  

**Includes examples for:**
- Data Engineering (Glue, Notebooks, Athena)
- ML Workflows (SageMaker, Notebooks)
- GenAI Applications (Bedrock, Notebooks)

### 🔧 DevOps Teams
**You focus on:** CI/CD best practices, security, compliance, and deployment automation  
**You define:** Workflow templates that enforce testing, approvals, and promotion policies  
**You don't need to know:** Application-specific details, AWS services used, DataZone APIs, SMUS project structures, or business logic  

→ **[Admin Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/admin-quickstart.md)** - Configure infrastructure and pipelines in 15 minutes  
→ **[GitHub Workflow Templates](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/git-templates/)** - Generic, reusable workflow templates for automated deployment

**The CLI is your abstraction layer:** You just call `aws-smus-cicd-cli deploy` - the CLI handles all AWS service interactions (DataZone, Glue, Athena, SageMaker, MWAA, S3, IAM, etc.). Your workflows stay simple and generic.

---

## What Can You Deploy?

**📊 Analytics & BI**
- Glue ETL jobs and crawlers
- Athena queries
- QuickSight dashboards
- EMR jobs (future)
- Redshift queries (future)

**🤖 Machine Learning**
- SageMaker training jobs
- ML models and endpoints
- MLflow experiments
- Feature Store (future)
- Batch transforms (future)

**🧠 Generative AI**
- Bedrock agents
- Knowledge bases
- Foundation model configurations (future)

**📓 Code & Workflows**
- Jupyter notebooks
- SageMaker Unified Studio notebooks
- Python scripts
- Airflow DAGs (MWAA and Amazon MWAA Serverless)
- Lambda functions (future)

**💾 Data & Storage**
- S3 data files
- Git repositories
- DataZone catalog resources (Glossaries, GlossaryTerms, FormTypes, AssetTypes, Assets, Data Products, Metadata Forms)

---

## Supported AWS Services

Deploy workflows using these AWS services through Airflow YAML syntax:

### 🎯 Analytics & Data
**Amazon Athena** • **AWS Glue** • **Amazon EMR** • **Amazon Redshift** • **Amazon QuickSight** • **Lake Formation**

### 🤖 Machine Learning  
**SageMaker Training** • **SageMaker Pipelines** • **Feature Store** • **Model Registry** • **Batch Transform**

### 🧠 Generative AI
**Amazon Bedrock** • **Bedrock Agents** • **Bedrock Knowledge Bases** • **Guardrails**

### 📊 Additional Services
S3 • Lambda • Step Functions • DynamoDB • RDS • SNS/SQS • Batch

**See complete list:** [Airflow AWS Operators Reference](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md)

---

## Core Concepts

### Separation of Concerns: The Key Design Principle

**The Problem:** Traditional deployment approaches force DevOps teams to learn AWS analytics services (Glue, Athena, DataZone, SageMaker, MWAA, etc.) and understand SMUS project structures, or force data teams to become CI/CD experts.

**The Solution:** SMUS CI/CD CLI is the abstraction layer that encapsulates all AWS and SMUS complexity.

**Example workflow:**

```
1. DevOps Team                 2. Data Team                    3. SMUS CI/CD CLI (The Abstraction)
   ↓                               ↓                              ↓
Defines the PROCESS            Defines the CONTENT            Workflow calls:
- Test on merge                - Glue jobs                    aws-smus-cicd-cli deploy --manifest manifest.yaml
- Approval for prod            - SageMaker training             ↓
- Security scans               - Athena queries               CLI handles ALL AWS complexity:
- Notification rules           - File structure               - DataZone APIs
                                                              - Glue/Athena/SageMaker APIs
Defines INFRASTRUCTURE                                        - MWAA deployment
- Account & region                                            - S3 management
- IAM roles                                                   - IAM configuration
- Resources                                                   - Infrastructure provisioning

Works for ANY app!
No ML/Analytics/GenAI
service knowledge needed!
```

**DevOps teams focus on:**
- CI/CD best practices (testing, approvals, notifications)
- Security and compliance gates
- Deployment orchestration
- Monitoring and alerting

**SMUS CI/CD CLI handles ALL AWS complexity:**
- DataZone domain and project management
- AWS Glue, Athena, SageMaker, MWAA APIs
- S3 storage and artifact management
- IAM roles and permissions
- Connection configurations
- Catalog asset subscriptions
- Workflow deployment to Airflow
- Infrastructure provisioning
- Testing and validation

**Data teams focus on:**
- Application code and workflows
- Which AWS services to use (Glue, Athena, SageMaker, etc.)
- Environment configurations
- Business logic

**Result:** 
- **DevOps teams never call AWS APIs directly** - they just call `aws-smus-cicd-cli deploy`
- **CI/CD workflows are generic** - same workflow works for Glue apps, SageMaker apps, or Bedrock apps
- Data teams never touch CI/CD configs
- Both teams work independently using their expertise

---

### Application Manifest
A declarative YAML file (`manifest.yaml`) that defines your data application:
- **Application details** - Name, version, description
- **Content** - Code from git repositories, data/models from storage, QuickSight dashboards
- **Workflows** - Airflow DAGs for orchestration and automation
- **Stages** - Where to deploy (dev, test, prod environments)
- **Configuration** - Environment-specific settings, connections, and bootstrap actions

**Created and owned by data teams.** Defines **what** to deploy and **where**. No CI/CD knowledge required.

### Application
Your data/analytics workload being deployed:
- Airflow DAGs and Python scripts
- Jupyter notebooks and data files
- ML models and training code
- ETL pipelines and transformations
- GenAI agents and MCP servers
- Foundation model configurations

### Stage
A deployment environment (dev, test, prod) mapped to a SageMaker Unified Studio project:
- Domain and region configuration
- Project name and settings
- Resource connections (S3, Airflow, Athena, Glue)
- Environment-specific parameters
- Optional branch mapping for git-based deployments

### Stage-to-Project Mapping

Each application stage deploys to a dedicated SageMaker Unified Studio (SMUS) project. A project can host a single application or multiple applications depending on your architecture and CI/CD methodology. Stage projects are independent entities with their own governance:

- **Ownership & Access:** Each stage project has its own set of owners and contributors, which may differ from the development project. Production projects typically have restricted access compared to development environments.
- **Multi-Domain & Multi-Region:** Stage projects can belong to different SMUS domains, AWS accounts, and regions. For example, your dev stage might deploy to a development domain in us-east-1, while prod deploys to a production domain in eu-west-1.
- **Flexible Architecture:** Organizations can choose between dedicated projects per application (isolation) or shared projects hosting multiple applications (consolidation), based on security, compliance, and operational requirements.

This separation enables true environment isolation with independent access controls, compliance boundaries, and regional data residency requirements.

### Workflow
Orchestration logic that executes your application. Workflows serve two purposes:

**1. Deployment-time:** Create required AWS resources during deployment
- Provision infrastructure (S3 buckets, databases, IAM roles)
- Configure connections and permissions
- Set up monitoring and logging

**2. Runtime:** Execute ongoing data and ML pipelines
- Scheduled execution (daily, hourly, etc.)
- Event-driven triggers (S3 uploads, API calls)
- Data processing and transformations
- Model training and inference

Workflows are defined as Airflow DAGs (Directed Acyclic Graphs) in YAML format. Supports [MWAA (Managed Workflows for Apache Airflow)](https://aws.amazon.com/managed-workflows-for-apache-airflow/) and [Amazon MWAA Serverless](https://aws.amazon.com/blogs/big-data/introducing-amazon-mwaa-serverless/) ([User Guide](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html)).

### CI/CD Automation
GitHub Actions workflows (or other CI/CD systems) that automate deployment:
- **Created and owned by DevOps teams**
- Defines **how** and **when** to deploy
- Runs tests and quality gates
- Manages promotion across targets
- Enforces security and compliance policies
- Example: `.github/workflows/deploy.yml`

**Key insight:** DevOps teams create generic, reusable workflows that work for ANY application. They don't need to know if the app uses Glue, SageMaker, or Bedrock - the CLI handles all AWS service interactions. The workflow just calls `aws-smus-cicd-cli deploy` and the CLI does the rest.

### Deployment Modes

**Bundle-based (Artifact):** Create versioned archive → deploy archive to stages
- Good for: audit trails, rollback capability, compliance
- Command: `aws-smus-cicd-cli bundle` then `aws-smus-cicd-cli deploy --manifest app.tar.gz`

**Direct (Git-based):** Deploy directly from sources without intermediate artifacts
- Good for: simpler workflows, rapid iteration, git as source of truth
- Command: `aws-smus-cicd-cli deploy --manifest manifest.yaml --targets test`

Both modes work with any combination of storage and git content sources.

---

## Example Applications

Real-world examples showing how to deploy different workloads with SMUS CI/CD.

### 📊 Analytics - QuickSight Dashboard
Deploy interactive BI dashboards with automated Glue ETL pipelines for data preparation. Uses QuickSight asset bundles, Athena queries, and GitHub dataset integration with environment-specific configurations.

**AWS Services:** QuickSight • Glue • Athena • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-dashboard-glue-quicksight.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-dashboard-glue-quicksight.yml)

**What happens during deployment:** Application code is deployed to S3, Glue jobs and Airflow workflows are created and executed, QuickSight dashboard/data source/dataset are created, and QuickSight ingestion is initiated to refresh the dashboard with latest data.

<details>
<summary><b>📁 App Structure</b></summary>

```
dashboard-glue-quick/
├── manifest.yaml                      # Deployment configuration
├── covid_etl_workflow.yaml           # Airflow workflow definition
├── glue_setup_covid_db.py            # Glue job: Create database & tables
├── glue_covid_summary_job.py         # Glue job: ETL transformations
├── glue_set_permission_check.py      # Glue job: Permission validation
├── quicksight/
│   └── TotalDeathByCountry.qs        # QuickSight dashboard bundle
└── app_tests/
    └── test_covid_data.py            # Integration tests
```

**Key Files:**
- **Glue Jobs**: Python scripts for database setup, ETL, and validation
- **Workflow**: YAML defining Airflow DAG for orchestration
- **QuickSight Bundle**: Dashboard, datasets, and data sources
- **Tests**: Validate data quality and dashboard functionality

</details>

<details>
<summary><b>View Airflow Workflow</b></summary>

```yaml
workflow_combined:
  dag_id: 'covid_dashboard_glue_quick_pipeline'
  tasks:
    setup_covid_db_task:
      operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
      retries: 0
      job_name: setup-covid-db-job
      script_location: '{proj.connection.default.s3_shared.s3Uri}dashboard-glue-quick/bundle/glue_setup_covid_db.py'
      s3_bucket: '{proj.connection.default.s3_shared.bucket}'
      iam_role_name: '{proj.iam_role_name}'
      region_name: '{domain.region}'
      update_config: true
      script_args:
        '--BUCKET_NAME': '{proj.connection.default.s3_shared.bucket}'
        '--REGION_NAME': '{domain.region}'
      create_job_kwargs:
        GlueVersion: '4.0'
        MaxRetries: 0
        Timeout: 180

    data_summary_task:
      operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
      retries: 0
      job_name: summary-glue-job
      script_location: '{proj.connection.default.s3_shared.s3Uri}dashboard-glue-quick/bundle/glue_covid_summary_job.py'
      s3_bucket: '{proj.connection.default.s3_shared.bucket}'
      iam_role_name: '{proj.iam_role_name}'
      region_name: '{domain.region}'
      update_config: true
      script_args:
        '--DATABASE_NAME': 'covid19_db'
        '--TABLE_NAME': 'us_simplified'
        '--SUMMARY_DATABASE_NAME': 'covid19_summary_db'
        '--S3_DATABASE_PATH': '{proj.connection.default.s3_shared.s3Uri}dashboard-glue-quick/output/databases/covid19_summary_db/'
        '--BUCKET_NAME': '{proj.connection.default.s3_shared.bucket}'
      dependencies: [setup_covid_db_task]
      create_job_kwargs:
        GlueVersion: '4.0'
        MaxRetries: 0
        Timeout: 180

    set_permission_check_task:
      operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
      retries: 0
      job_name: set-permission-check-job
      script_location: '{proj.connection.default.s3_shared.s3Uri}dashboard-glue-quick/bundle/glue_set_permission_check.py'
      s3_bucket: '{proj.connection.default.s3_shared.bucket}'
      iam_role_name: '{proj.iam_role_name}'
      region_name: '{domain.region}'
      update_config: true
      script_args:
        '--BUCKET_NAME': '{proj.connection.default.s3_shared.bucket}'
        '--REGION_NAME': '{domain.region}'
        '--ROLES': '{env.GRANT_TO}'
      dependencies: [data_summary_task]
      create_job_kwargs:
        GlueVersion: '4.0'
        MaxRetries: 0
        Timeout: 180
```

</details>

<details>
<summary><b>View Manifest</b></summary>

```yaml
applicationName: IntegrationTestETLWorkflow

content:
  storage:
  - name: dashboard-glue-quick
    include:
    - "*.py"
  - name: workflows
    include:
    - "*.yaml"
  
  git:
  - repository: covid-19-dataset
    url: https://github.com/datasets/covid-19.git
  
  quicksight:
  - name: TotalDeathByCountry
    type: dashboard
  
  workflows:
  - workflowName: covid_dashboard_glue_quick_pipeline
    connectionName: default.workflow_serverless

stages:
  test:
    stage: TEST
    domain:
      tags:
        purpose: smus-cicd-testing
      region: ${TEST_DOMAIN_REGION}
    project:
      name: test-marketing
      owners:
      - Eng1
      - arn:aws:iam::${AWS_ACCOUNT_ID}:role/GitHubActionsRole-SMUS-CLI-Tests
      - arn:aws:iam::${AWS_ACCOUNT_ID}:role/Admin
    environment_variables:
      S3_PREFIX: test
      AWS_REGION: ${TEST_DOMAIN_REGION}
      GRANT_TO: Admin,service-role/aws-quicksight-service-role-v0
    bootstrap:
      actions:
      - type: workflow.create
        workflowName: covid_dashboard_glue_quick_pipeline
      - type: workflow.run
        workflowName: covid_dashboard_glue_quick_pipeline
        trailLogs: true
      - type: quicksight.refresh_dataset
        refreshScope: IMPORTED
        ingestionType: FULL_REFRESH
        wait: false
    deployment_configuration:
      storage:
      - name: dashboard-glue-quick
        connectionName: default.s3_shared
        targetDirectory: dashboard-glue-quick/bundle
      - name: workflows
        connectionName: default.s3_shared
        targetDirectory: dashboard-glue-quick/bundle/workflows
      git:
      - name: covid-19-dataset
        connectionName: default.s3_shared
        targetDirectory: repos
      quicksight:
        assets:
        - name: TotalDeathByCountry
          owners:
          - arn:aws:quicksight:${TEST_DOMAIN_REGION}:${AWS_ACCOUNT_ID}:user/default/Admin/*
          viewers:
          - arn:aws:quicksight:${TEST_DOMAIN_REGION}:${AWS_ACCOUNT_ID}:user/default/Admin/*
        overrideParameters:
          ResourceIdOverrideConfiguration:
            PrefixForAllResources: deployed-{stage.name}-covid-
```

</details>

**[View Full Example →](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-analytics---quicksight-dashboard)**

---

### 📓 Data Engineering - Notebooks
Deploy Jupyter notebooks with parallel execution orchestration for data analysis and ETL workflows. Demonstrates notebook deployment with MLflow integration for experiment tracking.

**AWS Services:** SageMaker Notebooks • MLflow • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-data-notebooks.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-data-notebooks.yml)

**What happens during deployment:** Notebooks and workflow definitions are uploaded to S3, Airflow DAG is created for parallel notebook execution, MLflow connection is provisioned for experiment tracking, and notebooks are ready to run on-demand or scheduled.

<details>
<summary><b>📁 App Structure</b></summary>

```
data-notebooks/
├── manifest.yaml                                # Deployment configuration
├── notebooks/
│   ├── customer_churn_prediction.ipynb         # Customer churn ML
│   ├── retail_sales_forecasting.ipynb          # Sales forecasting
│   ├── customer_segmentation_analysis.ipynb    # Customer segmentation
│   └── requirements.txt                        # Python dependencies
├── workflows/
│   └── parallel_notebooks_workflow.yaml        # Airflow orchestration
└── app_tests/
    └── test_notebooks_execution.py             # Integration tests
```

**Key Files:**
- **Notebooks**: 3 Jupyter notebooks for ML and analytics workflows
- **Workflow**: Parallel execution orchestration with Airflow
- **Tests**: Validate notebook execution and outputs

</details>

<details>
<summary><b>View Manifest</b></summary>

```yaml
applicationName: IntegrationTestNotebooks

content:
  storage:
    - name: notebooks
      connectionName: default.s3_shared
      include:
        - notebooks/
        - workflows/
  
  workflows:
    - workflowName: parallel_notebooks_execution
      connectionName: default.workflow_serverless

stages:
  test:
    domain:
      region: us-east-1
    project:
      name: test-marketing
      owners:
        - Eng1
        - arn:aws:iam::${AWS_ACCOUNT_ID}:role/GitHubActionsRole-SMUS-CLI-Tests
    environment_variables:
      S3_PREFIX: test
    deployment_configuration:
      storage:
        - name: notebooks
          connectionName: default.s3_shared
          targetDirectory: notebooks/bundle/notebooks
    bootstrap:
      actions:
        - type: datazone.create_connection
          name: mlflow-server
          connection_type: MLFLOW
          properties:
            trackingServerArn: arn:aws:sagemaker:${TEST_DOMAIN_REGION}:${AWS_ACCOUNT_ID}:mlflow-tracking-server/smus-integration-mlflow-use2
            trackingServerName: smus-integration-mlflow-use2
        - type: workflow.create
          workflowName: parallel_notebooks_execution
        - type: workflow.run
          workflowName: parallel_notebooks_execution
          trailLogs: true
```

</details>

<details>
<summary><b>View Airflow Workflow</b></summary>

```yaml
notebooks_workflow:
  dag_id: notebooks_parallel
  tasks:
    nb_churn:
      operator: airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator
      retries: 0
      domain_id: "{domain.id}"
      project_id: "{proj.id}"
      domain_region: "{domain.region}"
      input_config:
        input_path: notebooks/bundle/notebooks/customer_churn_prediction.ipynb
        input_params: {}
      output_config:
        output_formats:
        - NOTEBOOK
      compute:
        instance_type: ml.c5.xlarge
        image_details:
          image_name: sagemaker-distribution-prod
          image_version: '3'
      wait_for_completion: true
    nb_sales:
      operator: airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator
      retries: 0
      domain_id: "{domain.id}"
      project_id: "{proj.id}"
      domain_region: "{domain.region}"
      input_config:
        input_path: notebooks/bundle/notebooks/retail_sales_forecasting.ipynb
        input_params: {}
      output_config:
        output_formats:
        - NOTEBOOK
      compute:
        instance_type: ml.c5.xlarge
        image_details:
          image_name: sagemaker-distribution-prod
          image_version: '3'
      wait_for_completion: true
    nb_segment:
      operator: airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator
      retries: 0
      domain_id: "{domain.id}"
      project_id: "{proj.id}"
      domain_region: "{domain.region}"
      input_config:
        input_path: notebooks/bundle/notebooks/customer_segmentation_analysis.ipynb
        input_params: {}
      output_config:
        output_formats:
        - NOTEBOOK
      compute:
        instance_type: ml.c5.xlarge
        image_details:
          image_name: sagemaker-distribution-prod
          image_version: '3'
      wait_for_completion: true
```

</details>

**[View Full Example →](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-data-engineering---notebooks)**

---

### 🤖 Machine Learning - Training
Train ML models with SageMaker using the [SageMaker SDK](https://sagemaker.readthedocs.io/) and [SageMaker Distribution](https://github.com/aws/sagemaker-distribution/tree/main/src) images. Track experiments with MLflow and automate training pipelines with environment-specific configurations.

**AWS Services:** SageMaker Training • MLflow • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-ml-training.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-ml-training.yml)

**What happens during deployment:** Training code and workflow definitions are uploaded to S3 with compression, Airflow DAG is created for training orchestration, MLflow connection is provisioned for experiment tracking, and SageMaker training jobs are created and executed using SageMaker Distribution images.

<details>
<summary><b>📁 App Structure</b></summary>

```
ml/training/
├── manifest.yaml                      # Deployment configuration
├── code/
│   ├── sagemaker_training_script.py  # Training script
│   └── requirements.txt              # Python dependencies
├── workflows/
│   ├── ml_training_workflow.yaml     # Airflow orchestration
│   └── ml_training_notebook.ipynb    # Training notebook
└── app_tests/
    └── test_model_registration.py    # Integration tests
```

**Key Files:**
- **Training Script**: SageMaker training job implementation
- **Workflow**: Airflow DAG for training orchestration
- **Notebook**: Interactive training workflow
- **Tests**: Validate model registration and training

</details>

<details>
<summary><b>View Manifest</b></summary>

```yaml
applicationName: IntegrationTestMLTraining

content:
  storage:
    - name: training-code
      connectionName: default.s3_shared
      include: [ml/training/code]
    
    - name: training-workflows
      connectionName: default.s3_shared
      include: [ml/training/workflows]
  
  workflows:
    - workflowName: ml_training_workflow
      connectionName: default.workflow_serverless

stages:
  test:
    domain:
      region: us-east-1
    project:
      name: test-ml-training
      owners:
        - Eng1
        - arn:aws:iam::${AWS_ACCOUNT_ID}:role/GitHubActionsRole-SMUS-CLI-Tests
      role:
        arn: arn:aws:iam::${AWS_ACCOUNT_ID}:role/SMUSCICDTestRole
    environment_variables:
      S3_PREFIX: test
    deployment_configuration:
      storage:
        - name: training-code
          connectionName: default.s3_shared
          targetDirectory: ml/bundle/training-code
          compression: gz
        - name: training-workflows
          connectionName: default.s3_shared
          targetDirectory: ml/bundle/training-workflows
    bootstrap:
      actions:
        - type: datazone.create_connection
          name: mlflow-server
          connection_type: MLFLOW
          properties:
            trackingServerArn: arn:aws:sagemaker:${TEST_DOMAIN_REGION}:${AWS_ACCOUNT_ID}:mlflow-tracking-server/smus-integration-mlflow-use2
        - type: workflow.create
          workflowName: ml_training_workflow
        - type: workflow.run
          workflowName: ml_training_workflow
          trailLogs: true
```

</details>

<details>
<summary><b>View Airflow Workflow</b></summary>

```yaml
ml_training_workflow:
  dag_id: "ml_training_workflow"
  tasks:
    ml_training_notebook:
      operator: "airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator"
      retries: 0
      domain_id: "{domain.id}"
      project_id: "{proj.id}"
      domain_region: "{domain.region}"
      input_config:
        input_path: "ml/bundle/training-workflows/ml_training_notebook.ipynb"
        input_params:
          mlflow_tracking_server_arn: "{proj.connection.mlflow-server.trackingServerArn}"
          mlflow_artifact_location: "{proj.connection.default.s3_shared.s3Uri}ml/mlflow-artifacts"
          sklearn_version: "1.2-1"
          python_version: "py3"
          training_instance_type: "ml.m5.large"
          model_name: "realistic-classifier-v1"
      output_config:
        output_formats: 
          ['NOTEBOOK']
      wait_for_completion: True
```

</details>

**[View Full Example →](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---training)**

---

### 🤖 Machine Learning - Deployment
Deploy trained ML models as SageMaker real-time inference endpoints. Uses SageMaker SDK for endpoint configuration and [SageMaker Distribution](https://github.com/aws/sagemaker-distribution/tree/main/src) images for serving.

**AWS Services:** SageMaker Endpoints • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-ml-deployment.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-ml-deployment.yml)

**What happens during deployment:** Model artifacts, deployment code, and workflow definitions are uploaded to S3, Airflow DAG is created for endpoint deployment orchestration, SageMaker endpoint configuration and model are created, and the inference endpoint is deployed and ready to serve predictions.

<details>
<summary><b>📁 App Structure</b></summary>

```
ml/deployment/
├── manifest.yaml                      # Deployment configuration
├── code/
│   └── inference.py                  # Inference handler
├── workflows/
│   ├── ml_deployment_workflow.yaml   # Airflow orchestration
│   └── ml_deployment_notebook.ipynb  # Deployment notebook
└── app_tests/
    └── test_endpoint_deployment.py   # Integration tests
```

**Key Files:**
- **Inference Handler**: Custom inference logic for endpoint
- **Workflow**: Airflow DAG for endpoint deployment
- **Notebook**: Interactive deployment workflow
- **Tests**: Validate endpoint deployment and predictions

</details>

<details>
<summary><b>View Manifest</b></summary>

```yaml
applicationName: IntegrationTestMLDeployment

content:
  storage:
    - name: deployment-code
      connectionName: default.s3_shared
      include: [ml/deployment/code]
    
    - name: deployment-workflows
      connectionName: default.s3_shared
      include: [ml/deployment/workflows]
    
    - name: model-artifacts
      connectionName: default.s3_shared
      include: [ml/output/model-artifacts/latest]
  
  workflows:
    - workflowName: ml_deployment_workflow
      connectionName: default.workflow_serverless

stages:
  test:
    domain:
      region: us-east-1
    project:
      name: test-ml-deployment
      owners:
        - Eng1
        - arn:aws:iam::${AWS_ACCOUNT_ID}:role/GitHubActionsRole-SMUS-CLI-Tests
      role:
        arn: arn:aws:iam::${AWS_ACCOUNT_ID}:role/SMUSCICDTestRole
    environment_variables:
      S3_PREFIX: test
    deployment_configuration:
      storage:
        - name: deployment-code
          connectionName: default.s3_shared
          targetDirectory: ml/bundle/deployment-code
        - name: deployment-workflows
          connectionName: default.s3_shared
          targetDirectory: ml/bundle/deployment-workflows
        - name: model-artifacts
          connectionName: default.s3_shared
          targetDirectory: ml/bundle/model-artifacts
    bootstrap:
      actions:
        - type: workflow.create
          workflowName: ml_deployment_workflow
        - type: workflow.run
          workflowName: ml_deployment_workflow
          trailLogs: true
```

</details>

<details>
<summary><b>View Airflow Workflow</b></summary>

```yaml
ml_deployment_workflow:
  dag_id: "ml_deployment_workflow"
  tasks:
    ml_deployment_notebook:
      operator: "airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator"
      retries: 0
      domain_id: "{domain.id}"
      project_id: "{proj.id}"
      domain_region: "{domain.region}"
      input_config:
        input_path: "ml/bundle/deployment-workflows/ml_deployment_notebook.ipynb"
        input_params:
          model_s3_uri: "{proj.connection.default.s3_shared.s3Uri}ml/output/model-artifacts/latest/output/model.tar.gz"
          sklearn_version: "1.2-1"
          python_version: "py3"
          inference_instance_type: "ml.m5.large"
      output_config:
        output_formats: 
          ['NOTEBOOK']
      wait_for_completion: True
```

</details>

**[View Full Example →](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---deployment)**

---

### 🧠 Generative AI
Deploy GenAI applications with Bedrock agents and knowledge bases. Demonstrates RAG (Retrieval Augmented Generation) workflows with automated agent deployment and testing.

**AWS Services:** Amazon Bedrock • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-genai-workflow.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-genai-workflow.yml)

**What happens during deployment:** Agent configuration and workflow definitions are uploaded to S3, Airflow DAG is created for agent deployment orchestration, Bedrock agents and knowledge bases are configured, and the GenAI application is ready for inference and testing.

<details>
<summary><b>📁 App Structure</b></summary>

```
genai/
├── manifest.yaml                      # Deployment configuration
├── job-code/
│   ├── requirements.txt              # Python dependencies
│   ├── test_agent.yaml               # Agent test configuration
│   ├── lambda_mask_string.py         # Lambda function
│   └── utils/
│       ├── bedrock_agent.py          # Agent management
│       ├── bedrock_agent_helper.py   # Agent utilities
│       └── knowledge_base_helper.py  # Knowledge base utilities
├── workflows/
│   ├── genai_dev_workflow.yaml       # Airflow orchestration
│   └── bedrock_agent_notebook.ipynb  # Agent deployment notebook
└── app_tests/
    └── test_genai_workflow.py        # Integration tests
```

**Key Files:**
- **Agent Code**: Bedrock agent and knowledge base management
- **Workflow**: Airflow DAG for GenAI deployment
- **Notebook**: Interactive agent deployment
- **Tests**: Validate agent functionality

</details>

<details>
<summary><b>View Manifest</b></summary>

```yaml
applicationName: IntegrationTestGenAIWorkflow

content:
  storage:
    - name: agent-code
      connectionName: default.s3_shared
      include: [genai/job-code]
    
    - name: genai-workflows
      connectionName: default.s3_shared
      include: [genai/workflows]
  
  workflows:
    - workflowName: genai_dev_workflow
      connectionName: default.workflow_serverless

stages:
  test:
    domain:
      region: us-east-1
    project:
      name: test-marketing
      owners:
        - Eng1
        - arn:aws:iam::${AWS_ACCOUNT_ID}:role/GitHubActionsRole-SMUS-CLI-Tests
    environment_variables:
      S3_PREFIX: test
    deployment_configuration:
      storage:
        - name: agent-code
          connectionName: default.s3_shared
          targetDirectory: genai/bundle/agent-code
        - name: genai-workflows
          connectionName: default.s3_shared
          targetDirectory: genai/bundle/workflows
```

</details>

<details>
<summary><b>View Airflow Workflow</b></summary>

```yaml
genai_dev_workflow:
  dag_id: "genai_dev_workflow"
  tasks:
    bedrock_agent_notebook:
      operator: "airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator"
      retries: 0
      domain_id: "{domain.id}"
      project_id: "{proj.id}"
      domain_region: "{domain.region}"
      input_config:
        input_path: "genai/bundle/workflows/bedrock_agent_notebook.ipynb"
        input_params:
          agent_name: "calculator_agent"
          agent_llm: "us.anthropic.claude-3-5-sonnet-20241022-v2:0"
          force_recreate: "True"
          kb_name: "mortgage-kb"
      output_config:
        output_formats: 
          ['NOTEBOOK']
      wait_for_completion: True
```

</details>

**[View Full Example →](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-generative-ai)**

---

**[See All Examples with Detailed Walkthroughs →](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md)**

---

### 🔐 IdC Domain Setup

The examples above support both IAM-based and IAM Identity Center (IdC)-based domains. IdC domains require additional one-time setup due to VpcOnly networking and tag-based IAM policies. Each example includes a setup script:

| Example | Setup Script | What It Does |
|---------|-------------|--------------|
| Data Notebooks | [`idc_domain_project_setup.py`](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/examples/analytic-workflow/data-notebooks/idc_domain_project_setup.py) | VPC networking (S3 gateway endpoint, NAT gateway), Lake Formation permissions on `sagemaker_sample_db` |
| ML Training | [`idc_domain_project_setup.py`](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/examples/analytic-workflow/ml/training/idc_domain_project_setup.py) | MLflow tracking server access, CloudWatch Logs permissions |
| ML Deployment | Uses the same project role as ML Training | No additional setup beyond ML Training |

```bash
# Run setup for data-notebooks (IdC domain)
TEST_DOMAIN_REGION=us-east-1 python examples/analytic-workflow/data-notebooks/idc_domain_project_setup.py

# Run setup for ML training (IdC domain)
TEST_DOMAIN_REGION=us-east-1 python examples/analytic-workflow/ml/training/idc_domain_project_setup.py

# Dry run to preview changes
python examples/analytic-workflow/data-notebooks/idc_domain_project_setup.py --dry-run
```

All setup scripts are idempotent and safe to run multiple times. Use `--dry-run` to preview changes before applying.

---

---

<details>
<summary><h2>📋 Feature Checklist</h2></summary>

**Legend:** ✅ Supported | 🔄 Planned | 🔮 Future

### Core Infrastructure
| Feature | Status | Notes |
|---------|--------|-------|
| YAML configuration | ✅ | [Manifest Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest.md) |
| Infrastructure as Code | ✅ | [Deploy Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#deploy) |
| Multi-environment deployment | ✅ | [Stages](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md#stages) |
| CLI tool | ✅ | [CLI Commands](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md) |
| Version control integration | ✅ | [GitHub Actions](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-actions-integration.md) |

### Deployment & Bundling
**Automated Deployment** - Define your application content, workflows, and deployment targets in YAML. Bundle-based (artifact) or direct (git-based) deployment modes. Deploy to test and prod with a single command. Dynamic configuration using `${VAR}` substitution. Track deployments in S3 or git for deployment history.

| Feature | Status | Notes |
|---------|--------|-------|
| Artifact bundling | ✅ | [Bundle Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#bundle) |
| Bundle-based deployment | ✅ | [Deploy Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#deploy) |
| Direct deployment | ✅ | [Deploy Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#deploy) |
| Deployment validation | ✅ | [Describe Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#describe) |
| Dry-run validation | ✅ | [Deploy --dry-run](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#dry-run-mode) |
| Incremental deployment | 🔄 | Upload only changed files |
| Rollback support | 🔮 | Automated rollback |
| Blue-green deployment | 🔮 | Zero-downtime deployments |

### Developer Experience
| Feature | Status | Notes |
|---------|--------|-------|
| Project templates | 🔄 | `aws-smus-cicd-cli init` with templates |
| Manifest initialization | ✅ | [Create Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#create) |
| Interactive setup | 🔄 | Guided configuration prompts |
| Local development | ✅ | [CLI Commands](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md) |
| VS Code extension | 🔮 | IntelliSense and validation |

### Configuration
**Environment Variables & Dynamic Configuration** - Flexible configuration for any environment using variable substitution. Environment-specific settings with validation and connection management.

| Feature | Status | Notes |
|---------|--------|-------|
| Variable substitution | ✅ | [Substitutions Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/substitutions-and-variables.md) |
| Environment-specific config | ✅ | [Stages](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md#stages) |
| Secrets management | 🔮 | AWS Secrets Manager integration |
| Config validation | ✅ | [Manifest Schema](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md) |
| Connection management | ✅ | [Connections Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/connections.md) |

### Resources & Workloads
**Deploy Any AWS Service** - Airflow DAGs, Jupyter notebooks, Glue ETL jobs, Athena queries, SageMaker training and endpoints, QuickSight dashboards, Bedrock agents, Lambda functions, EMR jobs, and Redshift queries.

| Feature | Status | Notes |
|---------|--------|-------|
| Airflow DAGs | ✅ | [Workflows](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md#workflows) |
| Jupyter notebooks | ✅ | [SageMakerNotebookOperator](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-sagemaker) |
| Glue ETL jobs | ✅ | [GlueJobOperator](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#aws-glue) |
| Athena queries | ✅ | [AthenaOperator](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-athena) |
| SageMaker training | ✅ | [SageMakerTrainingOperator](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-sagemaker) |
| SageMaker endpoints | ✅ | [SageMakerEndpointOperator](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-sagemaker) |
| QuickSight dashboards | ✅ | [QuickSight Deployment](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/quicksight-deployment.md) |
| Bedrock agents | ✅ | [BedrockInvokeModelOperator](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-bedrock) |
| Lambda functions | 🔄 | [LambdaInvokeFunctionOperator](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#aws-lambda) |
| EMR jobs | ✅ | [EmrAddStepsOperator](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-emr) |
| Redshift queries | ✅ | [RedshiftDataOperator](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-redshift) |

### Bootstrap Actions
**Automated Workflow Execution & Event-Driven Workflows** - Trigger workflows automatically during deployment with `workflow.run` (use `trailLogs: true` to stream logs and wait for completion). Fetch workflow logs for validation and debugging with `workflow.logs`. Automatically refresh QuickSight dashboards after ETL deployment with `quicksight.refresh_dataset`. Emit custom events for downstream automation and CI/CD orchestration with `eventbridge.put_events`. Provision MLflow and other DataZone connections during deployment. Actions run in order during `aws-smus-cicd-cli deploy` for reliable initialization and validation.

| Feature | Status | Notes |
|---------|--------|-------|
| Workflow execution | ✅ | [workflow.run](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md#workflowrun---trigger-workflow-execution) |
| Log retrieval | ✅ | [workflow.logs](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md#workflowlogs---fetch-workflow-logs) |
| QuickSight refresh | ✅ | [quicksight.refresh_dataset](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md#quicksightrefresh_dataset---trigger-dataset-ingestion) |
| EventBridge events | ✅ | [eventbridge.put_events](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md#customput_events---emit-custom-events) |
| DataZone connections | ✅ | [datazone.create_connection](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md) |
| Sequential execution | ✅ | [Execution Flow](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md#execution-flow) |

### CI/CD Integration
**Pre-built CI/CD Pipeline Workflows** - GitHub Actions, GitLab CI, Azure DevOps, and Jenkins support for automated deployment. Flexible configuration for any CI/CD platform. Trigger deployments from external events with webhook support.

| Feature | Status | Notes |
|---------|--------|-------|
| GitHub Actions | ✅ | [GitHub Actions Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-actions-integration.md) |
| GitLab CI | ✅ | [CLI Commands](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md) |
| Azure DevOps | ✅ | [CLI Commands](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md) |
| Jenkins | ✅ | [CLI Commands](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md) |
| Service principals | ✅ | [GitHub Actions Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-actions-integration.md) |
| OIDC federation | ✅ | [GitHub Actions Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-actions-integration.md) |

### Testing & Validation
**Automated Tests & Quality Gates** - Run validation tests before promoting to production. Block deployments if tests fail. Track execution status and logs. Verify deployment correctness with health checks.

| Feature | Status | Notes |
|---------|--------|-------|
| Unit testing | ✅ | [Test Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#test) |
| Integration testing | ✅ | [Test Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#test) |
| Automated tests | ✅ | [Test Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#test) |
| Quality gates | ✅ | [Test Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#test) |
| Workflow monitoring | ✅ | [Monitor Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#monitor) |

### Monitoring & Observability
| Feature | Status | Notes |
|---------|--------|-------|
| Deployment monitoring | ✅ | [Deploy Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#deploy) |
| Workflow monitoring | ✅ | [Monitor Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#monitor) |
| Custom alerts | ✅ | [Deployment Metrics](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-deployment-metrics.md) |
| Metrics collection | ✅ | [Deployment Metrics](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-deployment-metrics.md) |
| Deployment history | ✅ | [Bundle Command](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md#bundle) |

### AWS Service Integration
| Feature | Status | Notes |
|---------|--------|-------|
| Amazon MWAA | ✅ | [Workflows](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md#workflows) |
| MWAA Serverless | ✅ | [Workflows](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md#workflows) |
| AWS Glue | ✅ | [Airflow Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#aws-glue) |
| Amazon Athena | ✅ | [Airflow Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-athena) |
| SageMaker | ✅ | [Airflow Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-sagemaker) |
| Amazon Bedrock | ✅ | [Airflow Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-bedrock) |
| Amazon QuickSight | ✅ | [QuickSight Deployment](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/quicksight-deployment.md) |
| DataZone | ✅ | [Manifest Schema](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md) |
| EventBridge | ✅ | [Deployment Metrics](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-deployment-metrics.md) |
| Lake Formation | ✅ | [Connections Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/connections.md) |
| Amazon S3 | ✅ | [Storage](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md#storage) |
| AWS Lambda | 🔄 | [Airflow Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#aws-lambda) |
| Amazon EMR | ✅ | [Airflow Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-emr) |
| Amazon Redshift | ✅ | [Airflow Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md#amazon-redshift) |

### Advanced Features
| Feature | Status | Notes |
|---------|--------|-------|
| Multi-region deployment | ✅ | [Stages](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md#stages) |
| Cross-project deployment | ✅ | [Stages](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md#stages) |
| Dependency management | ✅ | [Airflow Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md) |
| Catalog subscriptions | ✅ | [Manifest Schema](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md) |
| Multi-service orchestration | ✅ | [Airflow Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md) |
| Drift detection | 🔮 | Detect configuration drift |
| State management | 🔄 | Comprehensive state tracking |

</details>

---

## Documentation

### Getting Started
- **[Quick Start Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/quickstart.md)** - Deploy your first application (10 min)
- **[Admin Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/admin-quickstart.md)** - Set up infrastructure (15 min)

### Guides
- **[Application Manifest](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest.md)** - Complete YAML configuration reference
- **[CLI Commands](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md)** - All available commands and options
- **[Rollback Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/rollback-guide.md)** - Recover from bad deployments and automate rollback
- **[Bootstrap Actions](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md)** - Automated deployment actions and event-driven workflows
- **[Substitutions & Variables](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/substitutions-and-variables.md)** - Dynamic configuration
- **[Connections Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/connections.md)** - Configure AWS service integrations
- **[GitHub Actions Integration](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-actions-integration.md)** - CI/CD automation setup
- **[GitHub Workflow Application Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-workflow-application-guide.md)** - Application admin guide for direct branch deployment
- **[GitHub Workflow DevOps Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-workflow-devops-guide.md)** - DevOps guide for direct branch deployment
- **[Deployment Metrics](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-deployment-metrics.md)** - Monitoring with EventBridge
- **[Catalog Import/Export Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/catalog-import-export-guide.md)** - Promote DataZone catalog resources across environments
- **[Catalog Import/Export Quick Reference](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/catalog-import-export-quick-reference.md)** - Quick reference for catalog deployment
- **[Notebook Sync (E2E Example)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/examples/e2e-notebook-sync/README.md)** - Export and sync notebooks across environments (bundle-deploy mode)
- **[MCP Configuration](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/mcp-configuration.md)** - MCP server configuration guide
- **[Q CLI Conversation Examples](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/q-cli-conversation-examples.md)** - Example conversations with Q CLI

### Reference
- **[Manifest Schema](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md)** - YAML schema validation and structure
- **[Airflow AWS Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md)** - Custom operator reference
- **[Airflow in SMUS CI/CD Summary](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-smus-cicd-summary.md)** - Overview of Airflow's role in SMUS CI/CD
- **[Architecture](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/architecture.md)** - CLI architecture documentation
- **[Pipeline Architecture Diagram](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-architecture-diagram.md)** - CI/CD pipeline architecture overview

### Examples
- **[Examples Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md)** - Walkthrough of example applications
- **[Data Notebooks](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-data-engineering---notebooks)** - Jupyter notebooks with Airflow
- **[ML Training](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---training)** - SageMaker training with MLflow
- **[ML Deployment](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---deployment)** - SageMaker endpoint deployment
- **[QuickSight Dashboard](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-analytics---quicksight-dashboard)** - BI dashboards with Glue
- **[GenAI Application](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-generative-ai)** - Bedrock agents and knowledge bases

### Development
- **[Developer Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/developer/developer-guide.md)** - Complete development guide with architecture, testing, and workflows
- **[Development Guide](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/development.md)** - Development workflows, testing, and contribution guidelines
- **[PyPI Publishing](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pypi-publishing.md)** - PyPI publishing setup
- **[AI Assistant Context](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/developer/AmazonQ.md)** - Context for AI assistants (Amazon Q, Kiro)
- **[Tests Overview](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/tests/README.md)** - Testing infrastructure

### Support
- **Issues**: [GitHub Issues](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues)
- **Documentation**: [docs/](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/docs/)
- **Examples**: [examples/](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/examples/)

---

## Security Notice

Always install from the official AWS PyPI package or source code.

```bash
# ✅ Correct - Install from official AWS PyPI package
pip install aws-smus-cicd-cli

# ✅ Also correct - Install from official AWS source code
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .
```

---

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/LICENSE) for details.

---

<div align="center">
  <img src="docs/readme-qr-code.png" alt="Scan to view README" width="200"/>
  <p><em>Scan QR code to view this README on GitHub</em></p>
</div>


