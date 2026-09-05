[![en](https://img.shields.io/badge/lang-en-gray.svg)](../../../README.md)
[![pt](https://img.shields.io/badge/lang-pt-brightgreen.svg?style=for-the-badge)](../pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](../fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](../it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](../ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](../zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](../he/README.md)

← [Back to Main README](../../../README.md)

# CLI de Pipeline CI/CD SMUS


> **[Domínios IAM + IdC]** Esta CLI suporta domínios SMUS baseados em IAM e em IAM Identity Center (IdC). Para domínios IdC, configuração adicional (rede VPC, permissões Lake Formation, políticas IAM inline) pode ser necessária — consulte os scripts de configuração em cada diretório de exemplo.

**Automatize a implantação de aplicações de dados em ambientes SageMaker Unified Studio**

Implante DAGs Airflow, notebooks Jupyter e workflows ML de desenvolvimento para produção com confiança. Construído para cientistas de dados, engenheiros de dados, engenheiros ML e desenvolvedores de aplicações GenAI trabalhando com equipes DevOps.

**Funciona com sua estratégia de implantação:** Seja usando branches git (baseado em branch), artefatos versionados (baseado em bundle), tags git (baseado em tag), ou implantação direta - esta CLI suporta seu fluxo de trabalho. Defina sua aplicação uma vez, implante do seu jeito.

---

## Por que SMUS CI/CD CLI?

✅ **Camada de Abstração AWS** - A CLI encapsula toda a complexidade de analytics, ML e SMUS da AWS - Equipes de DevOps nunca chamam APIs da AWS diretamente  
✅ **Separação de Responsabilidades** - Equipes de dados definem O QUE implantar (manifest.yaml), equipes de DevOps definem COMO e QUANDO (workflows de CI/CD)  
✅ **Workflows de CI/CD Genéricos** - O mesmo workflow funciona para Glue, SageMaker, Bedrock, QuickSight ou qualquer combinação de serviços AWS  
✅ **Implante com Confiança** - Validação dry-run pré-implantação e testes automatizados antes da produção  
✅ **Gerenciamento Multi-Ambiente** - Teste → Produção com configuração específica por ambiente  
✅ **Infrastructure as Code** - Manifestos de aplicação versionados e implantações reproduzíveis  
✅ **Workflows Orientados a Eventos** - Acione workflows automaticamente via EventBridge na implantação  

---

## Início Rápido

**Instalar:**
```bash
pip install aws-smus-cicd-cli
```

**Implante sua primeira aplicação:**
```bash
# Validar configuração
aws-smus-cicd-cli describe --manifest manifest.yaml --connect

# Criar pacote de implantação (opcional)
aws-smus-cicd-cli bundle --manifest manifest.yaml

# Visualizar implantação (execução simulada)
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml --dry-run

# Implantar no ambiente de teste
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml

# Executar testes de validação
aws-smus-cicd-cli test --manifest manifest.yaml --targets test

# Limpar quando concluído
aws-smus-cicd-cli destroy --manifest manifest.yaml --targets test --force
```

**Veja em ação:** [Exemplo ao Vivo no GitHub Actions](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/runs/24535194879)

---

## Gerar um Manifesto com IA (Skill)

Não quer escrever `manifest.yaml` manualmente? Este repositório inclui uma [Agent Skill](https://agentskills.io/) que escaneia seu projeto do SageMaker Unified Studio e gera um manifesto de implantação — e um workflow de orquestração quando necessário. Adicione-a a um agente de codificação (Kiro, Amazon Q CLI, Claude Code, ou qualquer agente compatível com [AgentSkills](https://agentskills.io/)), então pergunte *"Gere um manifesto CI/CD SMUS para meu projeto."*

→ **[Gerar um Manifesto com IA](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/generate-manifest-skill.md)** - Instruções de configuração e uso

---

## Para Quem É Isso?

### 👨‍💻 Equipes de Dados (Cientistas de Dados, Engenheiros de Dados, Desenvolvedores de Aplicações GenAI)
**Você foca em:** Sua aplicação - o que implantar, onde implantar e como ela é executada  
**Você define:** Manifesto da aplicação (`manifest.yaml`) com seu código, fluxos de trabalho e configurações  
**Você não precisa saber:** Pipelines de CI/CD, GitHub Actions, automação de implantação  

→ **[Guia de Início Rápido](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/quickstart.md)** - Implante sua primeira aplicação em 10 minutos  

**Inclui exemplos para:**
- Engenharia de Dados (Glue, Notebooks, Athena)
- Fluxos de Trabalho de ML (SageMaker, Notebooks)
- Aplicações GenAI (Bedrock, Notebooks)

### 🔧 Equipes DevOps
**Você foca em:** Melhores práticas de CI/CD, segurança, conformidade e automação de implantação  
**Você define:** Templates de fluxo de trabalho que impõem testes, aprovações e políticas de promoção  
**Você não precisa saber:** Detalhes específicos da aplicação, serviços AWS utilizados, APIs do DataZone, estruturas de projeto SMUS ou lógica de negócio  

→ **[Guia do Administrador](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/admin-quickstart.md)** - Configure infraestrutura e pipelines em 15 minutos  
→ **[Templates de Fluxo de Trabalho do GitHub](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/git-templates/)** - Templates de fluxo de trabalho genéricos e reutilizáveis para implantação automatizada

**O CLI é sua camada de abstração:** Você apenas executa `aws-smus-cicd-cli deploy` - o CLI gerencia todas as interações com os serviços AWS (DataZone, Glue, Athena, SageMaker, MWAA, S3, IAM, etc.). Seus fluxos de trabalho permanecem simples e genéricos.

---

## O Que Você Pode Implantar?

**📊 Analytics & BI**
- Jobs e crawlers do Glue
- Consultas do Athena
- Dashboards do QuickSight
- Jobs do EMR (futuro)
- Consultas do Redshift (futuro)

**🤖 Machine Learning**
- Jobs de treinamento do SageMaker
- Modelos ML e endpoints
- Experimentos do MLflow
- Feature Store (futuro)
- Transformações em lote (futuro)

**🧠 IA Generativa**
- Agentes do Bedrock
- Bases de conhecimento
- Configurações de modelos de fundação (futuro)

**📓 Código & Fluxos de Trabalho**
- Notebooks Jupyter
- Notebooks do SageMaker Unified Studio
- Scripts Python
- DAGs do Airflow (MWAA e Amazon MWAA Serverless)
- Funções Lambda (futuro)

**💾 Dados & Armazenamento**
- Arquivos de dados do S3
- Repositórios Git
- Recursos de catálogo do DataZone (Glossários, Termos de Glossário, Tipos de Formulário, Tipos de Ativos, Ativos, Produtos de Dados, Formulários de Metadados)

---

## Serviços AWS Suportados

Implante fluxos de trabalho usando estes serviços AWS através da sintaxe YAML do Airflow:

### 🎯 Analytics e Dados
**Amazon Athena** • **AWS Glue** • **Amazon EMR** • **Amazon Redshift** • **Amazon QuickSight** • **Lake Formation**

### 🤖 Machine Learning  
**SageMaker Training** • **SageMaker Pipelines** • **Feature Store** • **Model Registry** • **Batch Transform**

### 🧠 IA Generativa
**Amazon Bedrock** • **Bedrock Agents** • **Bedrock Knowledge Bases** • **Guardrails**

### 📊 Serviços Adicionais
S3 • Lambda • Step Functions • DynamoDB • RDS • SNS/SQS • Batch

**Veja a lista completa:** [Airflow AWS Operators Reference](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md)

---

## Conceitos Fundamentais

### Separação de Responsabilidades: O Princípio de Design Chave

**O Problema:** Abordagens tradicionais de implantação forçam equipes de DevOps a aprender serviços de analytics da AWS (Glue, Athena, DataZone, SageMaker, MWAA, etc.) e entender estruturas de projetos SMUS, ou forçam equipes de dados a se tornarem especialistas em CI/CD.

**A Solução:** SMUS CI/CD CLI é a camada de abstração que encapsula toda a complexidade da AWS e do SMUS.

**Exemplo de fluxo de trabalho:**

```
1. Equipe DevOps               2. Equipe de Dados              3. SMUS CI/CD CLI (A Abstração)
   ↓                               ↓                              ↓
Define o PROCESSO              Define o CONTEÚDO              Workflow chama:
- Teste no merge               - Jobs Glue                    aws-smus-cicd-cli deploy --manifest manifest.yaml
- Aprovação para prod          - Treinamento SageMaker          ↓
- Scans de segurança           - Queries Athena               CLI gerencia TODA a complexidade AWS:
- Regras de notificação        - Estrutura de arquivos        - APIs DataZone
                                                              - APIs Glue/Athena/SageMaker
Define INFRAESTRUTURA                                         - Deployment MWAA
- Conta e região                                              - Gerenciamento S3
- Roles IAM                                                   - Configuração IAM
- Recursos                                                    - Provisionamento de infraestrutura

Funciona para QUALQUER app!
Não precisa conhecimento de
serviços ML/Analytics/GenAI!
```

**Equipes DevOps focam em:**
- Melhores práticas de CI/CD (testes, aprovações, notificações)
- Controles de segurança e conformidade
- Orquestração de deployment
- Monitoramento e alertas

**SMUS CI/CD CLI gerencia TODA a complexidade AWS:**
- Gerenciamento de domínio e projeto DataZone
- APIs AWS Glue, Athena, SageMaker, MWAA
- Armazenamento S3 e gerenciamento de artefatos
- Roles e permissões IAM
- Configurações de conexão
- Assinaturas de ativos do catálogo
- Deployment de workflow para Airflow
- Provisionamento de infraestrutura
- Testes e validação

**Equipes de dados focam em:**
- Código da aplicação e workflows
- Quais serviços AWS usar (Glue, Athena, SageMaker, etc.)
- Configurações de ambiente
- Lógica de negócio

**Resultado:** 
- **Equipes DevOps nunca chamam APIs AWS diretamente** - apenas chamam `aws-smus-cicd-cli deploy`
- **Workflows CI/CD são genéricos** - o mesmo workflow funciona para apps Glue, SageMaker ou Bedrock
- Equipes de dados nunca tocam configurações de CI/CD
- Ambas as equipes trabalham independentemente usando sua expertise

---

### Manifesto da Aplicação
Um arquivo YAML declarativo (`manifest.yaml`) que define sua aplicação de dados:
- **Detalhes da aplicação** - Nome, versão, descrição
- **Conteúdo** - Código de repositórios git, dados/modelos de armazenamento, dashboards QuickSight
- **Workflows** - DAGs Airflow para orquestração e automação
- **Stages** - Onde implantar (ambientes dev, test, prod)
- **Configuração** - Configurações específicas de ambiente, conexões e ações de bootstrap

**Criado e mantido pelas equipes de dados.** Define **o que** implantar e **onde**. Não requer conhecimento de CI/CD.

### Aplicação
Sua carga de trabalho de dados/analytics sendo implantada:
- DAGs Airflow e scripts Python
- Notebooks Jupyter e arquivos de dados
- Modelos ML e código de treinamento
- Pipelines ETL e transformações
- Agentes GenAI e servidores MCP
- Configurações de modelos de fundação

### Stage
Um ambiente de deployment (dev, test, prod) mapeado para um projeto SageMaker Unified Studio:
- Configuração de domínio e região
- Nome e configurações do projeto
- Conexões de recursos (S3, Airflow, Athena, Glue)
- Parâmetros específicos de ambiente
- Mapeamento opcional de branch para deployments baseados em git

### Mapeamento Stage-para-Projeto

Cada stage da aplicação é implantado em um projeto dedicado do SageMaker Unified Studio (SMUS). Um projeto pode hospedar uma única aplicação ou múltiplas aplicações dependendo da sua arquitetura e metodologia de CI/CD. Projetos de stage são entidades independentes com sua própria governança:

- **Propriedade e Acesso:** Cada projeto de stage tem seu próprio conjunto de proprietários e colaboradores, que podem diferir do projeto de desenvolvimento. Projetos de produção tipicamente têm acesso restrito comparado aos ambientes de desenvolvimento.
- **Multi-Domínio e Multi-Região:** Projetos de stage podem pertencer a diferentes domínios SMUS, contas AWS e regiões. Por exemplo, seu stage dev pode implantar em um domínio de desenvolvimento em us-east-1, enquanto prod implanta em um domínio de produção em eu-west-1.
- **Arquitetura Flexível:** Organizações podem escolher entre projetos dedicados por aplicação (isolamento) ou projetos compartilhados hospedando múltiplas aplicações (consolidação), baseado em requisitos de segurança, conformidade e operacionais.

Esta separação permite verdadeiro isolamento de ambiente com controles de acesso independentes, limites de conformidade e requisitos de residência regional de dados.

### Workflow
Lógica de orquestração que executa sua aplicação. Workflows servem dois propósitos:

**1. Tempo de deployment:** Criar recursos AWS necessários durante o deployment
- Provisionar infraestrutura (buckets S3, bancos de dados, roles IAM)
- Configurar conexões e permissões
- Configurar monitoramento e logging

**2. Runtime:** Executar pipelines contínuos de dados e ML
- Execução agendada (diária, horária, etc.)
- Triggers orientados a eventos (uploads S3, chamadas API)
- Processamento e transformações de dados
- Treinamento e inferência de modelos

Workflows são definidos como DAGs Airflow (Directed Acyclic Graphs) em formato YAML. Suporta [MWAA (Managed Workflows for Apache Airflow)](https://aws.amazon.com/managed-workflows-for-apache-airflow/) e [Amazon MWAA Serverless](https://aws.amazon.com/blogs/big-data/introducing-amazon-mwaa-serverless/) ([Guia do Usuário](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html)).

### Automação CI/CD
Workflows GitHub Actions (ou outros sistemas CI/CD) que automatizam o deployment:
- **Criado e mantido pelas equipes DevOps**
- Define **como** e **quando** implantar
- Executa testes e controles de qualidade
- Gerencia promoção entre targets
- Aplica políticas de segurança e conformidade
- Exemplo: `.github/workflows/deploy.yml`

**Insight chave:** Equipes DevOps criam workflows genéricos e reutilizáveis que funcionam para QUALQUER aplicação. Eles não precisam saber se o app usa Glue, SageMaker ou Bedrock - o CLI gerencia todas as interações com serviços AWS. O workflow apenas chama `aws-smus-cicd-cli deploy` e o CLI faz o resto.

### Modos de Deployment

**Baseado em Bundle (Artefato):** Criar arquivo versionado → implantar arquivo nos stages
- Bom para: trilhas de auditoria, capacidade de rollback, conformidade
- Comando: `aws-smus-cicd-cli bundle` depois `aws-smus-cicd-cli deploy --manifest app.tar.gz`

**Direto (Baseado em Git):** Implantar diretamente das fontes sem artefatos intermediários
- Bom para: workflows mais simples, iteração rápida, git como fonte da verdade
- Comando: `aws-smus-cicd-cli deploy --manifest manifest.yaml --targets test`

Ambos os modos funcionam com qualquer combinação de fontes de conteúdo de armazenamento e git.

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


## Documentação

### Primeiros Passos
- **[Guia de Início Rápido](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/quickstart.md)** - Implante sua primeira aplicação (10 min)
- **[Guia do Administrador](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/admin-quickstart.md)** - Configure a infraestrutura (15 min)
- **[Gerar um Manifesto com IA](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/generate-manifest-skill.md)** - Habilidade de agente que escaneia seu projeto e gera um manifesto

### Guias
- **[Manifesto da Aplicação](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest.md)** - Referência completa de configuração YAML
- **[Comandos CLI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md)** - Todos os comandos e opções disponíveis
- **[Guia de Rollback](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/rollback-guide.md)** - Recupere-se de implantações ruins e automatize rollback
- **[Ações de Bootstrap](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md)** - Ações de implantação automatizadas e fluxos de trabalho orientados a eventos
- **[Substituições e Variáveis](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/substitutions-and-variables.md)** - Configuração dinâmica
- **[Guia de Conexões](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/connections.md)** - Configure integrações de serviços AWS
- **[Integração com GitHub Actions](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-actions-integration.md)** - Configuração de automação CI/CD
- **[Guia de Aplicação de Workflow GitHub](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-workflow-application-guide.md)** - Guia do administrador de aplicação para implantação direta de branch
- **[Guia DevOps de Workflow GitHub](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-workflow-devops-guide.md)** - Guia DevOps para implantação direta de branch
- **[Métricas de Implantação](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-deployment-metrics.md)** - Monitoramento com EventBridge
- **[Guia de Importação/Exportação de Catálogo](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/catalog-import-export-guide.md)** - Promova recursos de catálogo DataZone entre ambientes
- **[Referência Rápida de Importação/Exportação de Catálogo](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/catalog-import-export-quick-reference.md)** - Referência rápida para implantação de catálogo
- **[Sincronização de Notebook (Exemplo E2E)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/examples/e2e-notebook-sync/README.md)** - Exporte e sincronize notebooks entre ambientes (modo bundle-deploy)
- **[Configuração MCP](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/mcp-configuration.md)** - Guia de configuração do servidor MCP
- **[Exemplos de Conversação Q CLI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/q-cli-conversation-examples.md)** - Exemplos de conversações com Q CLI

### Referência
- **[Schema do Manifesto](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md)** - Validação e estrutura do schema YAML
- **[Operadores AWS do Airflow](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md)** - Referência de operadores personalizados
- **[Resumo do Airflow no CI/CD SMUS](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-smus-cicd-summary.md)** - Visão geral do papel do Airflow no CI/CD SMUS
- **[Arquitetura](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/architecture.md)** - Documentação da arquitetura CLI
- **[Diagrama de Arquitetura do Pipeline](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-architecture-diagram.md)** - Visão geral da arquitetura do pipeline CI/CD

### Exemplos
- **[Guia de Exemplos](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md)** - Passo a passo de aplicações de exemplo
- **[Notebooks de Dados](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-data-engineering---notebooks)** - Notebooks Jupyter com Airflow
- **[Treinamento ML](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---training)** - Treinamento SageMaker com MLflow
- **[Implantação ML](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---deployment)** - Implantação de endpoint SageMaker
- **[Dashboard QuickSight](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-analytics---quicksight-dashboard)** - Dashboards BI com Glue
- **[Aplicação GenAI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-generative-ai)** - Agentes e bases de conhecimento Bedrock

### Desenvolvimento
- **[Guia do Desenvolvedor](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/developer/developer-guide.md)** - Guia completo de desenvolvimento com arquitetura, testes e fluxos de trabalho
- **[Guia de Desenvolvimento](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/development.md)** - Fluxos de trabalho de desenvolvimento, testes e diretrizes de contribuição
- **[Publicação PyPI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pypi-publishing.md)** - Configuração de publicação PyPI
- **[Contexto do Assistente de IA](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/developer/AmazonQ.md)** - Contexto para assistentes de IA (Amazon Q, Kiro)
- **[Visão Geral dos Testes](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/tests/README.md)** - Infraestrutura de testes

### Suporte
- **Issues**: [GitHub Issues](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues)
- **Documentação**: [docs/](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/docs/)
- **Exemplos**: [examples/](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/examples/)

---

## Aviso de Segurança

Sempre instale a partir do pacote PyPI oficial da AWS ou do código-fonte.

```bash
# ✅ Correto - Instalar a partir do pacote PyPI oficial da AWS
pip install aws-smus-cicd-cli

# ✅ Também correto - Instalar a partir do código-fonte oficial da AWS
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .
```

---

## Licença

Este projeto está licenciado sob a Apache License, Versão 2.0. Consulte [LICENSE](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/LICENSE) para mais detalhes.

---

<div align="center">
  <img src="docs/readme-qr-code.png" alt="Escaneie para visualizar o README" width="200"/>
  <p><em>Escaneie o código QR para visualizar este README no GitHub</em></p>
</div>