[![en](https://img.shields.io/badge/lang-en-gray.svg)](../../../README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](../pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](../fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](../it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-brightgreen.svg?style=for-the-badge)](../ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](../zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](../he/README.md)

← [Back to Main README](../../../README.md)

# SMUS CI/CD パイプライン CLI


> **[IAM + IdC ドメイン]** この CLI は IAM ベースと IAM Identity Center (IdC) ベースの両方の SMUS ドメインをサポートしています。IdC ドメインの場合、追加のセットアップ (VPC ネットワーキング、Lake Formation 権限、インライン IAM ポリシー) が必要になる場合があります — 各サンプルディレクトリのセットアップスクリプトを参照してください。

**SageMaker Unified Studio 環境全体でデータアプリケーションのデプロイを自動化**

Airflow DAG、Jupyter ノートブック、ML ワークフローを開発環境から本番環境まで自信を持ってデプロイできます。DevOps チームと協力するデータサイエンティスト、データエンジニア、ML エンジニア、GenAI アプリ開発者向けに構築されています。

**あなたのデプロイ戦略に対応:** git ブランチ (ブランチベース)、バージョン管理されたアーティファクト (バンドルベース)、git タグ (タグベース)、または直接デプロイのいずれを使用する場合でも、この CLI はあなたのワークフローをサポートします。アプリケーションを一度定義すれば、あなたの方法でデプロイできます。

---

## なぜ SMUS CI/CD CLI なのか?

✅ **AWS 抽象化レイヤー** - CLI が AWS の分析、ML、SMUS の複雑さをすべてカプセル化 - DevOps チームが AWS API を直接呼び出すことはありません  
✅ **関心の分離** - データチームは何をデプロイするかを定義 (manifest.yaml)、DevOps チームはどのように、いつデプロイするかを定義 (CI/CD ワークフロー)  
✅ **汎用 CI/CD ワークフロー** - 同じワークフローが Glue、SageMaker、Bedrock、QuickSight、または任意の AWS サービスの組み合わせで機能します  
✅ **自信を持ってデプロイ** - デプロイ前のドライラン検証と本番環境前の自動テスト  
✅ **マルチ環境管理** - テスト → 本番環境への環境固有の設定  
✅ **Infrastructure as Code** - バージョン管理されたアプリケーションマニフェストと再現可能なデプロイメント  
✅ **イベント駆動ワークフロー** - デプロイ時に EventBridge 経由でワークフローを自動的にトリガー  

---

## クイックスタート

**インストール:**
```bash
pip install aws-smus-cicd-cli
```

**最初のアプリケーションをデプロイ:**
```bash
# 設定を検証
aws-smus-cicd-cli describe --manifest manifest.yaml --connect

# デプロイバンドルを作成（オプション）
aws-smus-cicd-cli bundle --manifest manifest.yaml

# デプロイをプレビュー（ドライラン）
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml --dry-run

# テスト環境にデプロイ
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml

# 検証テストを実行
aws-smus-cicd-cli test --manifest manifest.yaml --targets test

# 完了後にクリーンアップ
aws-smus-cicd-cli destroy --manifest manifest.yaml --targets test --force
```

**実際の動作を確認:** [GitHub Actions のライブ例](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/runs/24535194879)

---

## 対象者

### 👨‍💻 データチーム (データサイエンティスト、データエンジニア、GenAI アプリ開発者)
**あなたが注力すること:** アプリケーション - 何をデプロイするか、どこにデプロイするか、どのように実行するか  
**あなたが定義するもの:** コード、ワークフロー、設定を含むアプリケーションマニフェスト (`manifest.yaml`)  
**知る必要がないこと:** CI/CD パイプライン、GitHub Actions、デプロイ自動化  

→ **[クイックスタートガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/quickstart.md)** - 10分で最初のアプリケーションをデプロイ  

**以下の例を含みます:**
- データエンジニアリング (Glue、Notebooks、Athena)
- ML ワークフロー (SageMaker、Notebooks)
- GenAI アプリケーション (Bedrock、Notebooks)

### 🔧 DevOps チーム
**あなたが注力すること:** CI/CD のベストプラクティス、セキュリティ、コンプライアンス、デプロイ自動化  
**あなたが定義するもの:** テスト、承認、プロモーションポリシーを強制するワークフローテンプレート  
**知る必要がないこと:** アプリケーション固有の詳細、使用される AWS サービス、DataZone API、SMUS プロジェクト構造、ビジネスロジック  

→ **[管理者ガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/admin-quickstart.md)** - 15分でインフラストラクチャとパイプラインを設定  
→ **[GitHub ワークフローテンプレート](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/git-templates/)** - 自動デプロイ用の汎用的で再利用可能なワークフローテンプレート

**CLI が抽象化レイヤーです:** `aws-smus-cicd-cli deploy` を呼び出すだけで、CLI がすべての AWS サービスとのやり取り (DataZone、Glue、Athena、SageMaker、MWAA、S3、IAM など) を処理します。ワークフローはシンプルで汎用的なまま維持されます。

---

## デプロイ可能なもの

**📊 分析 & BI**
- Glue ETL ジョブとクローラー
- Athena クエリ
- QuickSight ダッシュボード
- EMR ジョブ (今後対応予定)
- Redshift クエリ (今後対応予定)

**🤖 機械学習**
- SageMaker トレーニングジョブ
- ML モデルとエンドポイント
- MLflow 実験
- Feature Store (今後対応予定)
- バッチ変換 (今後対応予定)

**🧠 生成 AI**
- Bedrock エージェント
- ナレッジベース
- 基盤モデル設定 (今後対応予定)

**📓 コード & ワークフロー**
- Jupyter ノートブック
- SageMaker Unified Studio ノートブック
- Python スクリプト
- Airflow DAG (MWAA および Amazon MWAA Serverless)
- Lambda 関数 (今後対応予定)

**💾 データ & ストレージ**
- S3 データファイル
- Git リポジトリ
- DataZone カタログリソース (Glossaries、GlossaryTerms、FormTypes、AssetTypes、Assets、Data Products、Metadata Forms)

---

## サポートされている AWS サービス

Airflow YAML 構文を使用して、以下の AWS サービスを利用したワークフローをデプロイできます:

### 🎯 分析とデータ
**Amazon Athena** • **AWS Glue** • **Amazon EMR** • **Amazon Redshift** • **Amazon QuickSight** • **Lake Formation**

### 🤖 機械学習
**SageMaker Training** • **SageMaker Pipelines** • **Feature Store** • **Model Registry** • **Batch Transform**

### 🧠 生成 AI
**Amazon Bedrock** • **Bedrock Agents** • **Bedrock Knowledge Bases** • **Guardrails**

### 📊 その他のサービス
S3 • Lambda • Step Functions • DynamoDB • RDS • SNS/SQS • Batch

**完全なリストを参照:** [Airflow AWS Operators Reference](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md)

---

## コアコンセプト

### 関心の分離：重要な設計原則

**問題点：** 従来のデプロイメントアプローチでは、DevOpsチームがAWS分析サービス（Glue、Athena、DataZone、SageMaker、MWAAなど）を学習し、SMUSプロジェクト構造を理解する必要があるか、データチームがCI/CDの専門家になる必要がありました。

**解決策：** SMUS CI/CD CLIは、すべてのAWSとSMUSの複雑さをカプセル化する抽象化レイヤーです。

**ワークフローの例：**

```
1. DevOpsチーム              2. データチーム                3. SMUS CI/CD CLI（抽象化レイヤー）
   ↓                           ↓                              ↓
プロセスを定義               コンテンツを定義               ワークフローの呼び出し：
- マージ時のテスト           - Glueジョブ                   aws-smus-cicd-cli deploy --manifest manifest.yaml
- 本番環境の承認             - SageMakerトレーニング          ↓
- セキュリティスキャン       - Athenaクエリ                 CLIがすべてのAWSの複雑さを処理：
- 通知ルール                 - ファイル構造                 - DataZone API
                                                            - Glue/Athena/SageMaker API
インフラストラクチャを定義                                  - MWAAデプロイメント
- アカウントとリージョン                                    - S3管理
- IAMロール                                                 - IAM設定
- リソース                                                  - インフラストラクチャのプロビジョニング

どのアプリでも動作！
ML/Analytics/GenAI
サービスの知識は不要！
```

**DevOpsチームが注力すること：**
- CI/CDのベストプラクティス（テスト、承認、通知）
- セキュリティとコンプライアンスゲート
- デプロイメントオーケストレーション
- モニタリングとアラート

**SMUS CI/CD CLIがすべてのAWSの複雑さを処理：**
- DataZoneドメインとプロジェクト管理
- AWS Glue、Athena、SageMaker、MWAA API
- S3ストレージとアーティファクト管理
- IAMロールと権限
- 接続設定
- カタログアセットのサブスクリプション
- Airflowへのワークフローデプロイメント
- インフラストラクチャのプロビジョニング
- テストと検証

**データチームが注力すること：**
- アプリケーションコードとワークフロー
- 使用するAWSサービス（Glue、Athena、SageMakerなど）
- 環境設定
- ビジネスロジック

**結果：** 
- **DevOpsチームはAWS APIを直接呼び出さない** - `aws-smus-cicd-cli deploy`を呼び出すだけ
- **CI/CDワークフローは汎用的** - 同じワークフローがGlueアプリ、SageMakerアプリ、Bedrockアプリで動作
- データチームはCI/CD設定に触れない
- 両チームが専門知識を活かして独立して作業

---

### アプリケーションマニフェスト
データアプリケーションを定義する宣言的なYAMLファイル（`manifest.yaml`）：
- **アプリケーション詳細** - 名前、バージョン、説明
- **コンテンツ** - gitリポジトリからのコード、ストレージからのデータ/モデル、QuickSightダッシュボード
- **ワークフロー** - オーケストレーションと自動化のためのAirflow DAG
- **ステージ** - デプロイ先（dev、test、prod環境）
- **設定** - 環境固有の設定、接続、ブートストラップアクション

**データチームが作成し所有します。** **何を**デプロイし、**どこに**デプロイするかを定義します。CI/CDの知識は不要です。

### アプリケーション
デプロイされるデータ/分析ワークロード：
- Airflow DAGとPythonスクリプト
- Jupyterノートブックとデータファイル
- MLモデルとトレーニングコード
- ETLパイプラインと変換処理
- GenAIエージェントとMCPサーバー
- 基盤モデルの設定

### ステージ
SageMaker Unified Studioプロジェクトにマッピングされたデプロイメント環境（dev、test、prod）：
- ドメインとリージョンの設定
- プロジェクト名と設定
- リソース接続（S3、Airflow、Athena、Glue）
- 環境固有のパラメータ
- gitベースのデプロイメント用のオプションのブランチマッピング

### ステージからプロジェクトへのマッピング

各アプリケーションステージは、専用のSageMaker Unified Studio（SMUS）プロジェクトにデプロイされます。プロジェクトは、アーキテクチャとCI/CD手法に応じて、単一のアプリケーションまたは複数のアプリケーションをホストできます。ステージプロジェクトは、独自のガバナンスを持つ独立したエンティティです：

- **所有権とアクセス：** 各ステージプロジェクトには独自の所有者と貢献者がおり、開発プロジェクトとは異なる場合があります。本番プロジェクトは通常、開発環境と比較してアクセスが制限されています。
- **マルチドメインとマルチリージョン：** ステージプロジェクトは、異なるSMUSドメイン、AWSアカウント、リージョンに属することができます。たとえば、devステージはus-east-1の開発ドメインにデプロイし、prodはeu-west-1の本番ドメインにデプロイする場合があります。
- **柔軟なアーキテクチャ：** 組織は、セキュリティ、コンプライアンス、運用要件に基づいて、アプリケーションごとの専用プロジェクト（分離）または複数のアプリケーションをホストする共有プロジェクト（統合）を選択できます。

この分離により、独立したアクセス制御、コンプライアンス境界、リージョナルデータレジデンシー要件を持つ真の環境分離が可能になります。

### ワークフロー
アプリケーションを実行するオーケストレーションロジック。ワークフローには2つの目的があります：

**1. デプロイメント時：** デプロイメント中に必要なAWSリソースを作成
- インフラストラクチャのプロビジョニング（S3バケット、データベース、IAMロール）
- 接続と権限の設定
- モニタリングとロギングのセットアップ

**2. ランタイム：** 継続的なデータとMLパイプラインを実行
- スケジュール実行（日次、時間単位など）
- イベント駆動トリガー（S3アップロード、API呼び出し）
- データ処理と変換
- モデルのトレーニングと推論

ワークフローはYAML形式のAirflow DAG（Directed Acyclic Graph）として定義されます。[MWAA（Managed Workflows for Apache Airflow）](https://aws.amazon.com/managed-workflows-for-apache-airflow/)および[Amazon MWAA Serverless](https://aws.amazon.com/blogs/big-data/introducing-amazon-mwaa-serverless/)（[ユーザーガイド](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html)）をサポートしています。

### CI/CD自動化
デプロイメントを自動化するGitHub Actionsワークフロー（または他のCI/CDシステム）：
- **DevOpsチームが作成し所有**
- **どのように**、**いつ**デプロイするかを定義
- テストと品質ゲートを実行
- ターゲット間のプロモーションを管理
- セキュリティとコンプライアンスポリシーを適用
- 例：`.github/workflows/deploy.yml`

**重要な洞察：** DevOpsチームは、**どの**アプリケーションでも動作する汎用的で再利用可能なワークフローを作成します。アプリがGlue、SageMaker、Bedrockのどれを使用しているかを知る必要はありません - CLIがすべてのAWSサービスとのやり取りを処理します。ワークフローは単に`aws-smus-cicd-cli deploy`を呼び出すだけで、CLIが残りを行います。

### デプロイメントモード

**バンドルベース（アーティファクト）：** バージョン管理されたアーカイブを作成 → ステージにアーカイブをデプロイ
- 適している用途：監査証跡、ロールバック機能、コンプライアンス
- コマンド：`aws-smus-cicd-cli bundle`の後に`aws-smus-cicd-cli deploy --manifest app.tar.gz`

**ダイレクト（Gitベース）：** 中間アーティファクトなしでソースから直接デプロイ
- 適している用途：シンプルなワークフロー、迅速な反復、信頼できる情報源としてのgit
- コマンド：`aws-smus-cicd-cli deploy --manifest manifest.yaml --targets test`

両方のモードは、ストレージとgitコンテンツソースの任意の組み合わせで動作します。

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


## ドキュメント

### はじめに
- **[クイックスタートガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/quickstart.md)** - 最初のアプリケーションをデプロイする (10分)
- **[管理者ガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/admin-quickstart.md)** - インフラストラクチャをセットアップする (15分)

### ガイド
- **[アプリケーションマニフェスト](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest.md)** - 完全な YAML 設定リファレンス
- **[CLI コマンド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md)** - 利用可能なすべてのコマンドとオプション
- **[ロールバックガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/rollback-guide.md)** - 不良デプロイメントからの復旧とロールバックの自動化
- **[ブートストラップアクション](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md)** - 自動デプロイメントアクションとイベント駆動ワークフロー
- **[置換と変数](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/substitutions-and-variables.md)** - 動的な設定
- **[接続ガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/connections.md)** - AWS サービス統合の設定
- **[GitHub Actions 統合](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-actions-integration.md)** - CI/CD 自動化のセットアップ
- **[GitHub ワークフローアプリケーションガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-workflow-application-guide.md)** - 直接ブランチデプロイメントのためのアプリケーション管理者ガイド
- **[GitHub ワークフロー DevOps ガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-workflow-devops-guide.md)** - 直接ブランチデプロイメントのための DevOps ガイド
- **[デプロイメントメトリクス](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-deployment-metrics.md)** - EventBridge によるモニタリング
- **[カタログインポート/エクスポートガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/catalog-import-export-guide.md)** - 環境間で DataZone カタログリソースを昇格させる
- **[カタログインポート/エクスポートクイックリファレンス](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/catalog-import-export-quick-reference.md)** - カタログデプロイメントのクイックリファレンス
- **[ノートブック同期 (E2E 例)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/examples/e2e-notebook-sync/README.md)** - 環境間でノートブックをエクスポートして同期する (bundle-deploy モード)
- **[MCP 設定](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/mcp-configuration.md)** - MCP サーバー設定ガイド
- **[Q CLI 会話例](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/q-cli-conversation-examples.md)** - Q CLI との会話例

### リファレンス
- **[マニフェストスキーマ](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md)** - YAML スキーマの検証と構造
- **[Airflow AWS オペレーター](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md)** - カスタムオペレーターリファレンス
- **[Airflow in SMUS CI/CD 概要](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-smus-cicd-summary.md)** - SMUS CI/CD における Airflow の役割の概要
- **[アーキテクチャ](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/architecture.md)** - CLI アーキテクチャドキュメント
- **[パイプラインアーキテクチャ図](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-architecture-diagram.md)** - CI/CD パイプラインアーキテクチャの概要

### 例
- **[例ガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md)** - サンプルアプリケーションのウォークスルー
- **[データノートブック](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-data-engineering---notebooks)** - Airflow を使用した Jupyter ノートブック
- **[ML トレーニング](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---training)** - MLflow を使用した SageMaker トレーニング
- **[ML デプロイメント](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---deployment)** - SageMaker エンドポイントのデプロイメント
- **[QuickSight ダッシュボード](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-analytics---quicksight-dashboard)** - Glue を使用した BI ダッシュボード
- **[GenAI アプリケーション](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-generative-ai)** - Bedrock エージェントとナレッジベース

### 開発
- **[開発者ガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/developer/developer-guide.md)** - アーキテクチャ、テスト、ワークフローを含む完全な開発ガイド
- **[開発ガイド](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/development.md)** - 開発ワークフロー、テスト、コントリビューションガイドライン
- **[PyPI 公開](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pypi-publishing.md)** - PyPI 公開のセットアップ
- **[AI アシスタントコンテキスト](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/developer/AmazonQ.md)** - AI アシスタント (Amazon Q、Kiro) のためのコンテキスト
- **[テスト概要](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/tests/README.md)** - テストインフラストラクチャ

### サポート
- **Issues**: [GitHub Issues](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues)
- **ドキュメント**: [docs/](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/docs/)
- **例**: [examples/](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/examples/)

---

## セキュリティに関する注意事項

必ず公式の AWS PyPI パッケージまたはソースコードからインストールしてください。

```bash
# ✅ 正しい - 公式 AWS PyPI パッケージからインストール
pip install aws-smus-cicd-cli

# ✅ こちらも正しい - 公式 AWS ソースコードからインストール
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .
```

---

## ライセンス

このプロジェクトは Apache License, Version 2.0 の下でライセンスされています。詳細は [LICENSE](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/LICENSE) を参照してください。

---

<div align="center">
  <img src="docs/readme-qr-code.png" alt="Scan to view README" width="200"/>
  <p><em>QR コードをスキャンして GitHub で README を表示</em></p>
</div>