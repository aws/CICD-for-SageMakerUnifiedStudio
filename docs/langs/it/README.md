[![en](https://img.shields.io/badge/lang-en-gray.svg)](../../../README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](../pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](../fr/README.md)
[![it](https://img.shields.io/badge/lang-it-brightgreen.svg?style=for-the-badge)](../it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](../ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](../zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](../he/README.md)

← [Back to Main README](../../../README.md)

# CLI della Pipeline CI/CD SMUS

> **[Domini IAM + IdC]** Questa CLI supporta sia domini basati su IAM che domini basati su IAM Identity Center (IdC). Per i domini IdC, potrebbe essere necessaria una configurazione aggiuntiva (rete VPC, permessi Lake Formation, policy IAM inline) — consulta gli script di configurazione in ciascuna directory di esempio.

**Automatizza il deployment di applicazioni dati attraverso gli ambienti SageMaker Unified Studio**

Distribuisci DAG Airflow, notebook Jupyter e workflow ML dallo sviluppo alla produzione con sicurezza. Progettato per data scientist, data engineer, ML engineer e sviluppatori di applicazioni GenAI che lavorano con team DevOps.

**Funziona con la tua strategia di deployment:** Che tu utilizzi branch git (basato su branch), artifact versionati (basato su bundle), tag git (basato su tag) o deployment diretto - questa CLI supporta il tuo flusso di lavoro. Definisci la tua applicazione una volta, distribuiscila a modo tuo.

---

## Perché SMUS CI/CD CLI?

✅ **Livello di Astrazione AWS** - La CLI incapsula tutta la complessità di analytics, ML e SMUS di AWS - I team DevOps non chiamano mai direttamente le API AWS  
✅ **Separazione delle Responsabilità** - I team dati definiscono COSA distribuire (manifest.yaml), i team DevOps definiscono COME e QUANDO (workflow CI/CD)  
✅ **Workflow CI/CD Generici** - Lo stesso workflow funziona per Glue, SageMaker, Bedrock, QuickSight o qualsiasi combinazione di servizi AWS  
✅ **Distribuzione con Sicurezza** - Validazione dry-run pre-distribuzione e test automatizzati prima della produzione  
✅ **Gestione Multi-Ambiente** - Test → Prod con configurazione specifica per ambiente  
✅ **Infrastructure as Code** - Manifest delle applicazioni versionati e distribuzioni riproducibili  
✅ **Workflow Event-Driven** - Attivazione automatica dei workflow tramite EventBridge alla distribuzione  

---

## Guida Rapida

**Installazione:**
```bash
pip install aws-smus-cicd-cli
```

**Distribuisci la tua prima applicazione:**
```bash
# Valida la configurazione
aws-smus-cicd-cli describe --manifest manifest.yaml --connect

# Crea il bundle di distribuzione (opzionale)
aws-smus-cicd-cli bundle --manifest manifest.yaml

# Anteprima della distribuzione (dry run)
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml --dry-run

# Distribuisci nell'ambiente di test
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml

# Esegui i test di validazione
aws-smus-cicd-cli test --manifest manifest.yaml --targets test

# Pulisci quando hai finito
aws-smus-cicd-cli destroy --manifest manifest.yaml --targets test --force
```

**Guarda in azione:** [Esempio Live di GitHub Actions](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/runs/24535194879)

---

## A Chi È Rivolto?

### 👨‍💻 Team di Dati (Data Scientist, Data Engineer, Sviluppatori di Applicazioni GenAI)
**Ti concentri su:** La tua applicazione - cosa distribuire, dove distribuire e come viene eseguita  
**Definisci:** Il manifest dell'applicazione (`manifest.yaml`) con il tuo codice, i workflow e le configurazioni  
**Non hai bisogno di conoscere:** Pipeline CI/CD, GitHub Actions, automazione del deployment  

→ **[Guida Rapida](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/quickstart.md)** - Distribuisci la tua prima applicazione in 10 minuti  

**Include esempi per:**
- Data Engineering (Glue, Notebooks, Athena)
- Workflow ML (SageMaker, Notebooks)
- Applicazioni GenAI (Bedrock, Notebooks)

### 🔧 Team DevOps
**Ti concentri su:** Best practice CI/CD, sicurezza, conformità e automazione del deployment  
**Definisci:** Template di workflow che applicano policy di testing, approvazioni e promozione  
**Non hai bisogno di conoscere:** Dettagli specifici dell'applicazione, servizi AWS utilizzati, API DataZone, strutture dei progetti SMUS o logica di business  

→ **[Guida per Amministratori](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/admin-quickstart.md)** - Configura infrastruttura e pipeline in 15 minuti  
→ **[Template di Workflow GitHub](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/git-templates/)** - Template di workflow generici e riutilizzabili per il deployment automatizzato

**Il CLI è il tuo livello di astrazione:** Devi solo chiamare `aws-smus-cicd-cli deploy` - il CLI gestisce tutte le interazioni con i servizi AWS (DataZone, Glue, Athena, SageMaker, MWAA, S3, IAM, ecc.). I tuoi workflow rimangono semplici e generici.

---

## Cosa Puoi Distribuire?

**📊 Analytics & BI**
- Job ETL e crawler di Glue
- Query di Athena
- Dashboard di QuickSight
- Job EMR (futuro)
- Query di Redshift (futuro)

**🤖 Machine Learning**
- Job di training di SageMaker
- Modelli ML ed endpoint
- Esperimenti MLflow
- Feature Store (futuro)
- Trasformazioni batch (futuro)

**🧠 Generative AI**
- Agenti Bedrock
- Knowledge base
- Configurazioni di modelli fondazionali (futuro)

**📓 Codice & Flussi di Lavoro**
- Notebook Jupyter
- Notebook SageMaker Unified Studio
- Script Python
- DAG Airflow (MWAA e Amazon MWAA Serverless)
- Funzioni Lambda (futuro)

**💾 Dati & Storage**
- File di dati S3
- Repository Git
- Risorse del catalogo DataZone (Glossari, Termini di Glossario, Tipi di Modulo, Tipi di Asset, Asset, Prodotti Dati, Moduli di Metadati)

---

## Servizi AWS Supportati

Distribuisci flussi di lavoro utilizzando questi servizi AWS attraverso la sintassi YAML di Airflow:

### 🎯 Analytics e Dati
**Amazon Athena** • **AWS Glue** • **Amazon EMR** • **Amazon Redshift** • **Amazon QuickSight** • **Lake Formation**

### 🤖 Machine Learning  
**SageMaker Training** • **SageMaker Pipelines** • **Feature Store** • **Model Registry** • **Batch Transform**

### 🧠 AI Generativa
**Amazon Bedrock** • **Bedrock Agents** • **Bedrock Knowledge Bases** • **Guardrails**

### 📊 Servizi Aggiuntivi
S3 • Lambda • Step Functions • DynamoDB • RDS • SNS/SQS • Batch

**Vedi l'elenco completo:** [Riferimento Operatori AWS di Airflow](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md)

---

## Concetti Fondamentali

### Separazione delle Responsabilità: Il Principio di Design Chiave

**Il Problema:** Gli approcci tradizionali al deployment costringono i team DevOps a imparare i servizi di analytics AWS (Glue, Athena, DataZone, SageMaker, MWAA, ecc.) e a comprendere le strutture dei progetti SMUS, oppure costringono i team dati a diventare esperti di CI/CD.

**La Soluzione:** SMUS CI/CD CLI è il livello di astrazione che incapsula tutta la complessità di AWS e SMUS.

**Esempio di workflow:**

```
1. Team DevOps                 2. Team Dati                    3. SMUS CI/CD CLI (L'Astrazione)
   ↓                               ↓                              ↓
Definisce il PROCESSO          Definisce il CONTENUTO         Il workflow chiama:
- Test al merge                - Job Glue                     aws-smus-cicd-cli deploy --manifest manifest.yaml
- Approvazione per prod        - Training SageMaker             ↓
- Scansioni di sicurezza       - Query Athena                 La CLI gestisce TUTTA la complessità AWS:
- Regole di notifica           - Struttura dei file           - API DataZone
                                                              - API Glue/Athena/SageMaker
Definisce l'INFRASTRUTTURA                                    - Deployment MWAA
- Account e region                                            - Gestione S3
- Ruoli IAM                                                   - Configurazione IAM
- Risorse                                                     - Provisioning dell'infrastruttura

Funziona per QUALSIASI app!
Non serve conoscenza dei servizi
ML/Analytics/GenAI!
```

**I team DevOps si concentrano su:**
- Best practice CI/CD (testing, approvazioni, notifiche)
- Gate di sicurezza e conformità
- Orchestrazione del deployment
- Monitoraggio e alerting

**SMUS CI/CD CLI gestisce TUTTA la complessità AWS:**
- Gestione di domini e progetti DataZone
- API AWS Glue, Athena, SageMaker, MWAA
- Gestione dello storage S3 e degli artifact
- Ruoli e permessi IAM
- Configurazioni delle connessioni
- Sottoscrizioni agli asset del catalogo
- Deployment dei workflow su Airflow
- Provisioning dell'infrastruttura
- Testing e validazione

**I team dati si concentrano su:**
- Codice applicativo e workflow
- Quali servizi AWS utilizzare (Glue, Athena, SageMaker, ecc.)
- Configurazioni degli ambienti
- Logica di business

**Risultato:** 
- **I team DevOps non chiamano mai direttamente le API AWS** - chiamano solo `aws-smus-cicd-cli deploy`
- **I workflow CI/CD sono generici** - lo stesso workflow funziona per app Glue, app SageMaker o app Bedrock
- I team dati non toccano mai le configurazioni CI/CD
- Entrambi i team lavorano in modo indipendente utilizzando le proprie competenze

---

### Manifest dell'Applicazione
Un file YAML dichiarativo (`manifest.yaml`) che definisce la tua applicazione dati:
- **Dettagli dell'applicazione** - Nome, versione, descrizione
- **Contenuto** - Codice da repository git, dati/modelli da storage, dashboard QuickSight
- **Workflow** - DAG Airflow per orchestrazione e automazione
- **Stage** - Dove effettuare il deploy (ambienti dev, test, prod)
- **Configurazione** - Impostazioni specifiche per ambiente, connessioni e azioni di bootstrap

**Creato e gestito dai team dati.** Definisce **cosa** deployare e **dove**. Non richiede conoscenza di CI/CD.

### Applicazione
Il tuo carico di lavoro dati/analytics da deployare:
- DAG Airflow e script Python
- Notebook Jupyter e file di dati
- Modelli ML e codice di training
- Pipeline ETL e trasformazioni
- Agenti GenAI e server MCP
- Configurazioni di foundation model

### Stage
Un ambiente di deployment (dev, test, prod) mappato a un progetto SageMaker Unified Studio:
- Configurazione di dominio e region
- Nome e impostazioni del progetto
- Connessioni alle risorse (S3, Airflow, Athena, Glue)
- Parametri specifici per ambiente
- Mappatura opzionale dei branch per deployment basati su git

### Mappatura Stage-to-Project

Ogni stage dell'applicazione viene deployato su un progetto dedicato di SageMaker Unified Studio (SMUS). Un progetto può ospitare una singola applicazione o più applicazioni a seconda dell'architettura e della metodologia CI/CD. I progetti stage sono entità indipendenti con la propria governance:

- **Proprietà e Accesso:** Ogni progetto stage ha il proprio insieme di proprietari e contributori, che possono differire dal progetto di sviluppo. I progetti di produzione hanno tipicamente accesso limitato rispetto agli ambienti di sviluppo.
- **Multi-Dominio e Multi-Region:** I progetti stage possono appartenere a diversi domini SMUS, account AWS e region. Ad esempio, il tuo stage dev potrebbe deployare su un dominio di sviluppo in us-east-1, mentre prod deploya su un dominio di produzione in eu-west-1.
- **Architettura Flessibile:** Le organizzazioni possono scegliere tra progetti dedicati per applicazione (isolamento) o progetti condivisi che ospitano più applicazioni (consolidamento), in base ai requisiti di sicurezza, conformità e operativi.

Questa separazione consente un vero isolamento degli ambienti con controlli di accesso indipendenti, confini di conformità e requisiti di residenza dei dati regionali.

### Workflow
Logica di orchestrazione che esegue la tua applicazione. I workflow servono a due scopi:

**1. Deployment-time:** Creare le risorse AWS necessarie durante il deployment
- Provisioning dell'infrastruttura (bucket S3, database, ruoli IAM)
- Configurare connessioni e permessi
- Configurare monitoraggio e logging

**2. Runtime:** Eseguire pipeline dati e ML continuative
- Esecuzione pianificata (giornaliera, oraria, ecc.)
- Trigger event-driven (upload S3, chiamate API)
- Elaborazione e trasformazioni dei dati
- Training e inferenza dei modelli

I workflow sono definiti come DAG Airflow (Directed Acyclic Graphs) in formato YAML. Supporta [MWAA (Managed Workflows for Apache Airflow)](https://aws.amazon.com/managed-workflows-for-apache-airflow/) e [Amazon MWAA Serverless](https://aws.amazon.com/blogs/big-data/introducing-amazon-mwaa-serverless/) ([Guida Utente](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html)).

### Automazione CI/CD
Workflow GitHub Actions (o altri sistemi CI/CD) che automatizzano il deployment:
- **Creati e gestiti dai team DevOps**
- Definisce **come** e **quando** deployare
- Esegue test e gate di qualità
- Gestisce la promozione tra i target
- Applica policy di sicurezza e conformità
- Esempio: `.github/workflows/deploy.yml`

**Intuizione chiave:** I team DevOps creano workflow generici e riutilizzabili che funzionano per QUALSIASI applicazione. Non hanno bisogno di sapere se l'app usa Glue, SageMaker o Bedrock - la CLI gestisce tutte le interazioni con i servizi AWS. Il workflow chiama semplicemente `aws-smus-cicd-cli deploy` e la CLI fa il resto.

### Modalità di Deployment

**Basato su Bundle (Artifact):** Creare archivio versionato → deployare archivio sugli stage
- Adatto per: audit trail, capacità di rollback, conformità
- Comando: `aws-smus-cicd-cli bundle` poi `aws-smus-cicd-cli deploy --manifest app.tar.gz`

**Diretto (Basato su Git):** Deployare direttamente dalle sorgenti senza artifact intermedi
- Adatto per: workflow più semplici, iterazione rapida, git come fonte di verità
- Comando: `aws-smus-cicd-cli deploy --manifest manifest.yaml --targets test`

Entrambe le modalità funzionano con qualsiasi combinazione di sorgenti di contenuto da storage e git.

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


## Documentazione

### Guida introduttiva
- **[Guida rapida](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/quickstart.md)** - Distribuisci la tua prima applicazione (10 min)
- **[Guida per amministratori](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/admin-quickstart.md)** - Configura l'infrastruttura (15 min)

### Guide
- **[Manifesto dell'applicazione](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest.md)** - Riferimento completo alla configurazione YAML
- **[Comandi CLI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md)** - Tutti i comandi e le opzioni disponibili
- **[Guida al rollback](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/rollback-guide.md)** - Recupera da distribuzioni non riuscite e automatizza il rollback
- **[Azioni di bootstrap](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md)** - Azioni di distribuzione automatizzate e flussi di lavoro basati su eventi
- **[Sostituzioni e variabili](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/substitutions-and-variables.md)** - Configurazione dinamica
- **[Guida alle connessioni](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/connections.md)** - Configura le integrazioni con i servizi AWS
- **[Integrazione con GitHub Actions](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-actions-integration.md)** - Configurazione dell'automazione CI/CD
- **[Guida all'applicazione del workflow GitHub](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-workflow-application-guide.md)** - Guida per amministratori di applicazioni per la distribuzione diretta da branch
- **[Guida DevOps per il workflow GitHub](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-workflow-devops-guide.md)** - Guida DevOps per la distribuzione diretta da branch
- **[Metriche di distribuzione](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-deployment-metrics.md)** - Monitoraggio con EventBridge
- **[Guida all'importazione/esportazione del catalogo](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/catalog-import-export-guide.md)** - Promuovi le risorse del catalogo DataZone tra ambienti
- **[Riferimento rapido per importazione/esportazione del catalogo](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/catalog-import-export-quick-reference.md)** - Riferimento rapido per la distribuzione del catalogo
- **[Sincronizzazione notebook (esempio E2E)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/examples/e2e-notebook-sync/README.md)** - Esporta e sincronizza notebook tra ambienti (modalità bundle-deploy)
- **[Configurazione MCP](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/mcp-configuration.md)** - Guida alla configurazione del server MCP
- **[Esempi di conversazione con Q CLI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/q-cli-conversation-examples.md)** - Esempi di conversazioni con Q CLI

### Riferimenti
- **[Schema del manifesto](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md)** - Validazione e struttura dello schema YAML
- **[Operatori AWS per Airflow](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md)** - Riferimento agli operatori personalizzati
- **[Riepilogo Airflow in SMUS CI/CD](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-smus-cicd-summary.md)** - Panoramica del ruolo di Airflow in SMUS CI/CD
- **[Architettura](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/architecture.md)** - Documentazione dell'architettura CLI
- **[Diagramma dell'architettura della pipeline](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-architecture-diagram.md)** - Panoramica dell'architettura della pipeline CI/CD

### Esempi
- **[Guida agli esempi](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md)** - Procedura dettagliata delle applicazioni di esempio
- **[Notebook per i dati](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-data-engineering---notebooks)** - Notebook Jupyter con Airflow
- **[Addestramento ML](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---training)** - Addestramento SageMaker con MLflow
- **[Distribuzione ML](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---deployment)** - Distribuzione di endpoint SageMaker
- **[Dashboard QuickSight](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-analytics---quicksight-dashboard)** - Dashboard BI con Glue
- **[Applicazione GenAI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-generative-ai)** - Agenti e knowledge base Bedrock

### Sviluppo
- **[Guida per sviluppatori](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/developer/developer-guide.md)** - Guida completa allo sviluppo con architettura, test e flussi di lavoro
- **[Guida allo sviluppo](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/development.md)** - Flussi di lavoro di sviluppo, test e linee guida per i contributi
- **[Pubblicazione su PyPI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pypi-publishing.md)** - Configurazione della pubblicazione su PyPI
- **[Contesto per assistenti AI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/developer/AmazonQ.md)** - Contesto per assistenti AI (Amazon Q, Kiro)
- **[Panoramica dei test](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/tests/README.md)** - Infrastruttura di test

### Supporto
- **Problemi**: [GitHub Issues](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues)
- **Documentazione**: [docs/](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/docs/)
- **Esempi**: [examples/](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/examples/)

---

## Avviso di Sicurezza

Installa sempre dal pacchetto PyPI ufficiale di AWS o dal codice sorgente.

```bash
# ✅ Corretto - Installa dal pacchetto PyPI ufficiale di AWS
pip install aws-smus-cicd-cli

# ✅ Anche corretto - Installa dal codice sorgente ufficiale di AWS
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .
```

---

## Licenza

Questo progetto è concesso in licenza secondo i termini della Apache License, versione 2.0. Consulta [LICENSE](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/LICENSE) per i dettagli.

---

<div align="center">
  <img src="docs/readme-qr-code.png" alt="Scansiona per visualizzare il README" width="200"/>
  <p><em>Scansiona il codice QR per visualizzare questo README su GitHub</em></p>
</div>