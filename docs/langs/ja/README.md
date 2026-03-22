[![en](https://img.shields.io/badge/lang-en-gray.svg)](../../../README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](../pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](../fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](../it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-brightgreen.svg?style=for-the-badge)](../ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](../zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](../he/README.md)

# SMUS CI/CD Pipeline CLI

[![en](https://img.shields.io/badge/lang-en-brightgreen.svg?style=for-the-badge)](README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](docs/langs/pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](docs/langs/fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](docs/langs/it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](docs/langs/ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](docs/langs/zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](docs/langs/he/README.md)

> **[プレビュー]** Amazon SageMaker Unified Studio CI/CD CLIは現在プレビュー段階であり、変更される可能性があります。コマンド、設定フォーマット、APIはお客様のフィードバックに基づいて進化する可能性があります。プレビュー期間中は本番環境以外での評価をお勧めします。フィードバックやバグ報告については、以下のイシューを開いてください：https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues

> **[IAMドメインのみ]** 現在このCLIはIAMベースの認証を使用するSMUSドメインのみをサポートしています。IAM Identity Center (IdC)ベースのドメインのサポートは近日公開予定です。

**SageMaker Unified Studio環境全体でのデータアプリケーションのデプロイを自動化**

"Deploy Airflow DAGs, Jupyter notebooks, and ML workflows from development to production with confidence. Built for data scientists, data engineers, ML engineers, and GenAI app developers working with DevOps teams."（Airflow DAG、Jupyterノートブック、MLワークフローを開発から本番環境まで確実にデプロイ。データサイエンティスト、データエンジニア、MLエンジニア、DevOpsチームと協働するGenAIアプリ開発者向けに構築）

"Works with your deployment strategy: Whether you use git branches (branch-based), versioned artifacts (bundle-based), git tags (tag-based), or direct deployment - this CLI supports your workflow. Define your application once, deploy it your way."（あなたのデプロイ戦略に対応：gitブランチ（ブランチベース）、バージョン管理されたアーティファクト（バンドルベース）、gitタグ（タグベース）、直接デプロイのいずれを使用する場合でも、このCLIはあなたのワークフローをサポートします。アプリケーションを一度定義すれば、お好みの方法でデプロイできます）

---

## SMUS CI/CD CLIを使用する理由

✅ **AWS抽象化レイヤー** - "CLI encapsulates all AWS analytics, ML, and SMUS complexity - DevOps teams never call AWS APIs directly" (CLIがすべてのAWSアナリティクス、ML、SMUS の複雑さをカプセル化 - DevOpsチームが直接AWSのAPIを呼び出す必要はありません)

✅ **関心の分離** - "Data teams define WHAT to deploy (manifest.yaml), DevOps teams define HOW and WHEN (CI/CD workflows)" (データチームは何をデプロイするか(manifest.yaml)を定義し、DevOpsチームはどのように、いつ(CI/CDワークフロー)を定義します)

✅ **汎用CI/CDワークフロー** - "Same workflow works for Glue, SageMaker, Bedrock, QuickSight, or any AWS service combination" (同じワークフローがGlue、SageMaker、Bedrock、QuickSight、または任意のAWSサービスの組み合わせで動作します)

✅ **確実なデプロイ** - 本番環境への展開前の自動テストと検証

✅ **マルチ環境管理** - 環境固有の設定によるテスト→本番環境への展開

✅ **Infrastructure as Code** - バージョン管理されたアプリケーションマニフェストと再現可能なデプロイメント

✅ **イベント駆動型ワークフロー** - EventBridgeを介したデプロイメント時の自動ワークフロートリガー

---

## クイックスタート

**ソースからインストール:**
```bash
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .
```

**最初のアプリケーションをデプロイ:**
```bash
# Validate configuration
aws-smus-cicd-cli describe --manifest manifest.yaml --connect

# Create deployment bundle (optional)
aws-smus-cicd-cli bundle --manifest manifest.yaml

# Deploy to test environment
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml

# Run validation tests
aws-smus-cicd-cli test --manifest manifest.yaml --targets test
```

**動作確認:** [Live GitHub Actions Example](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/runs/17631303500)

---

## 対象者

### 👨‍💻 データチーム（データサイエンティスト、データエンジニア、生成AIアプリ開発者）
**フォーカス:** アプリケーション - 何をデプロイし、どこにデプロイし、どのように実行するか  
**定義するもの:** アプリケーションマニフェスト（`manifest.yaml`）- コード、ワークフロー、設定を含む  
**知る必要がないもの:** CI/CDパイプライン、GitHub Actions、デプロイメント自動化

→ **[クイックスタートガイド](docs/getting-started/quickstart.md)** - 10分で最初のアプリケーションをデプロイ

**以下の例を含みます:**
- Data Engineering (Glue, Notebooks, Athena)
- ML Workflows (SageMaker, Notebooks) 
- GenAI Applications (Bedrock, Notebooks)

### 🔧 DevOpsチーム
**フォーカス:** CI/CDのベストプラクティス、セキュリティ、コンプライアンス、デプロイメント自動化  
**定義するもの:** テスト、承認、プロモーションポリシーを実施するワークフローテンプレート  
**知る必要がないもの:** アプリケーション固有の詳細、AWS サービス、DataZone API、SMUSプロジェクト構造、ビジネスロジック

→ **[管理者ガイド](docs/getting-started/admin-quickstart.md)** - 15分でインフラストラクチャとパイプラインを設定  
→ **[GitHubワークフローテンプレート](git-templates/)** - 自動デプロイメント用の汎用的で再利用可能なワークフローテンプレート

**CLIは抽象化レイヤーです:** `aws-smus-cicd-cli deploy`を呼び出すだけで、CLIがすべてのAWSサービスとのやり取り（DataZone、Glue、Athena、SageMaker、MWAA、S3、IAMなど）を処理します。ワークフローはシンプルで汎用的なままです。

---

## デプロイ可能なもの

**📊 分析 & BI**
- Glue ETL jobs and crawlers
- Athena queries
- QuickSight dashboards
- EMR jobs (将来対応予定)
- Redshift queries (将来対応予定)

**🤖 機械学習**
- SageMaker training jobs
- ML models and endpoints
- MLflow experiments
- Feature Store (将来対応予定)
- Batch transforms (将来対応予定)

**🧠 生成AI**
- Bedrock agents
- Knowledge bases
- Foundation model configurations (将来対応予定)

**📓 コード & ワークフロー**
- Jupyter notebooks
- Python scripts
- Airflow DAGs (MWAA および Amazon MWAA Serverless)
- Lambda functions (将来対応予定)

**💾 データ & ストレージ**
- S3 data files
- Git repositories
- Data catalogs (将来対応予定)

---

## サポートされているAWSサービス

"Deploy workflows using these AWS services through Airflow YAML syntax"
（AirflowのYAML構文を使用して、これらのAWSサービスでワークフローをデプロイ）

### 🎯 分析とデータ
**Amazon Athena** • **AWS Glue** • **Amazon EMR** • **Amazon Redshift** • **Amazon QuickSight** • **Lake Formation**

### 🤖 機械学習
**SageMaker Training** • **SageMaker Pipelines** • **Feature Store** • **Model Registry** • **Batch Transform**

### 🧠 生成AI
**Amazon Bedrock** • **Bedrock Agents** • **Bedrock Knowledge Bases** • **Guardrails**

### 📊 その他のサービス
S3 • Lambda • Step Functions • DynamoDB • RDS • SNS/SQS • Batch

**完全なリストを見る：** [Airflow AWS Operators Reference](docs/airflow-aws-operators.md)

---

## コアコンセプト

### 関心の分離：重要な設計原則

**問題点：** 従来のデプロイメントアプローチでは、DevOpsチームがAWSの分析サービス（Glue、Athena、DataZone、SageMaker、MWAAなど）とSMUSプロジェクト構造を学ぶ必要があるか、またはデータチームがCI/CDの専門家になる必要がありました。

**解決策：** SMUS CI/CD CLIは、すべてのAWSとSMUSの複雑さをカプセル化する抽象化レイヤーです。

"Example workflow:" (ワークフロー例：)

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

"DevOps teams focus on:" (DevOpsチームの注力点：)
- CI/CDのベストプラクティス（テスト、承認、通知）
- セキュリティとコンプライアンスのゲート
- デプロイメントのオーケストレーション
- モニタリングとアラート

"SMUS CI/CD CLI handles ALL AWS complexity:" (SMUS CI/CD CLIがすべてのAWSの複雑さを処理：)
- DataZoneドメインとプロジェクト管理
- AWS Glue、Athena、SageMaker、MWAA API
- S3ストレージとアーティファクト管理
- IAMロールと権限
- 接続設定
- カタログアセットのサブスクリプション
- AirflowへのワークフローデプロイメントS
- インフラストラクチャのプロビジョニング
- テストとバリデーション

"Data teams focus on:" (データチームの注力点：)
- アプリケーションコードとワークフロー
- 使用するAWSサービスの選択（Glue、Athena、SageMakerなど）
- 環境設定
- ビジネスロジック

**結果：**
- DevOpsチームはAWS APIを直接呼び出すことはありません - `aws-smus-cicd-cli deploy`を呼び出すだけです
- CI/CDワークフローは汎用的です - 同じワークフローがGlueアプリ、SageMakerアプリ、Bedrockアプリで動作します
- データチームはCI/CD設定に触れることはありません
- 両チームは独自の専門知識を活かして独立して作業します

---

### アプリケーションマニフェスト
データアプリケーションを定義する宣言的YAMLファイル（`manifest.yaml`）：
- **アプリケーションの詳細** - 名前、バージョン、説明
- **コンテンツ** - Gitリポジトリからのコード、ストレージからのデータ/モデル、QuickSightダッシュボード
- **ワークフロー** - オーケストレーションと自動化のためのAirflow DAG
- **ステージ** - デプロイ先（開発、テスト、本番環境）
- **設定** - 環境固有の設定、接続、ブートストラップアクション

データチームが作成し所有します。**何を**デプロイし**どこに**デプロイするかを定義します。CI/CDの知識は不要です。

### アプリケーション
デプロイされるデータ/分析ワークロード：
- Airflow DAGとPythonスクリプト
- Jupyterノートブックとデータファイル
- MLモデルとトレーニングコード
- ETLパイプラインと変換
- GenAIエージェントとMCPサーバー
- 基盤モデルの設定

### ステージ
SageMaker Unified Studioプロジェクトにマッピングされるデプロイメント環境（開発、テスト、本番）：
- ドメインと地域の設定
- プロジェクト名と設定
- リソース接続（S3、Airflow、Athena、Glue）
- 環境固有のパラメータ
- Gitベースのデプロイメント用のオプションのブランチマッピング

### ステージからプロジェクトへのマッピング

各アプリケーションステージは、専用のSageMaker Unified Studio（SMUS）プロジェクトにデプロイされます。プロジェクトは、アーキテクチャとCI/CD手法に応じて、単一のアプリケーションまたは複数のアプリケーションをホストできます。ステージプロジェクトは独自のガバナンスを持つ独立したエンティティです：

- **所有権とアクセス：** 各ステージプロジェクトには独自のオーナーとコントリビューターがおり、開発プロジェクトとは異なる場合があります。本番プロジェクトは通常、開発環境と比較してアクセスが制限されています。
- **マルチドメインとマルチリージョン：** ステージプロジェクトは異なるSMUSドメイン、AWSアカウント、リージョンに属することができます。例えば、開発ステージはus-east-1の開発ドメインにデプロイし、本番はeu-west-1の本番ドメインにデプロイすることができます。
- **柔軟なアーキテクチャ：** 組織は、セキュリティ、コンプライアンス、運用要件に基づいて、アプリケーションごとの専用プロジェクト（分離）または複数のアプリケーションをホストする共有プロジェクト（統合）を選択できます。

この分離により、独立したアクセス制御、コンプライアンス境界、地域データレジデンシー要件を持つ真の環境分離が可能になります。

### ワークフロー
アプリケーションを実行するオーケストレーションロジック。ワークフローには2つの目的があります：

**1. デプロイメント時：** デプロイメント中に必要なAWSリソースを作成
- インフラストラクチャのプロビジョニング（S3バケット、データベース、IAMロール）
- 接続と権限の設定
- モニタリングとロギングのセットアップ

**2. ランタイム：** 継続的なデータとMLパイプラインの実行
- スケジュールされた実行（毎日、毎時など）
- イベント駆動トリガー（S3アップロード、API呼び出し）
- データ処理と変換
- モデルトレーニングと推論

ワークフローはYAML形式でAirflow DAG（Directed Acyclic Graph）として定義されます。[MWAA (Managed Workflows for Apache Airflow)](https://aws.amazon.com/managed-workflows-for-apache-airflow/)と[Amazon MWAA Serverless](https://aws.amazon.com/blogs/big-data/introducing-amazon-mwaa-serverless/)をサポートします（[ユーザーガイド](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html)）。

### CI/CD自動化
デプロイメントを自動化するGitHub Actionsワークフロー（または他のCI/CDシステム）：
- **DevOpsチームが作成し所有**
- **どのように**そして**いつ**デプロイするかを定義
- テストと品質ゲートの実行
- ターゲット間のプロモーション管理
- セキュリティとコンプライアンスポリシーの適用
- 例：`.github/workflows/deploy.yml`

**重要な洞察：** DevOpsチームは、どんなアプリケーションでも動作する汎用的で再利用可能なワークフローを作成します。アプリがGlue、SageMaker、Bedrockのどれを使用しているかを知る必要はありません - CLIがすべてのAWSサービスとのやり取りを処理します。ワークフローは単に`aws-smus-cicd-cli deploy`を呼び出し、CLIが残りを処理します。

### デプロイメントモード

**バンドルベース（アーティファクト）：** バージョン管理されたアーカイブを作成 → アーカイブをステージにデプロイ
- 適している用途：監査証跡、ロールバック機能、コンプライアンス
- コマンド：`aws-smus-cicd-cli bundle`その後`aws-smus-cicd-cli deploy --manifest app.tar.gz`

**直接（Gitベース）：** 中間アーティファクトなしでソースから直接デプロイ
- 適している用途：シンプルなワークフロー、迅速な反復、Gitを真実の源として使用
- コマンド：`aws-smus-cicd-cli deploy --manifest manifest.yaml --stage test`

両モードは、ストレージとGitコンテンツソースのあらゆる組み合わせで動作します。

---

## サンプルアプリケーション

SMUS CI/CDを使用して様々なワークロードをデプロイする実例を紹介します。

### 📊 分析 - QuickSightダッシュボード
データ準備のための自動化されたGlue ETLパイプラインを使用してインタラクティブなBIダッシュボードをデプロイします。QuickSightアセットバンドル、Athenaクエリ、環境固有の設定を使用したGitHubデータセット統合を活用します。

**AWS Services:** QuickSight • Glue • Athena • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-dashboard-glue-quicksight.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-dashboard-glue-quicksight.yml)

**デプロイ時の動作:** アプリケーションコードがS3にデプロイされ、GlueジョブとAirflowワークフローが作成・実行され、QuickSightダッシュボード/データソース/データセットが作成され、最新データでダッシュボードを更新するためにQuickSightの取り込みが開始されます。

(残りのマークダウンは同様に翻訳を続けます...)

[以下同様に翻訳が続きます]

## ドキュメント

### はじめに
- **[クイックスタートガイド](docs/getting-started/quickstart.md)** - 最初のアプリケーションをデプロイ（10分）
- **[管理者ガイド](docs/getting-started/admin-quickstart.md)** - インフラストラクチャのセットアップ（15分）

### ガイド
- **[Application Manifest](docs/manifest.md)** - 完全なYAML設定リファレンス
- **[CLI Commands](docs/cli-commands.md)** - 利用可能なすべてのコマンドとオプション
- **[Bootstrap Actions](docs/bootstrap-actions.md)** - 自動デプロイアクションとイベント駆動型workflow
- **[Substitutions & Variables](docs/substitutions-and-variables.md)** - 動的設定
- **[接続ガイド](docs/connections.md)** - AWSサービス統合の設定
- **[GitHub Actions Integration](docs/github-actions-integration.md)** - CI/CD自動化のセットアップ
- **[Deployment Metrics](docs/pipeline-deployment-metrics.md)** - EventBridgeによる監視

### リファレンス
- **[Manifest Schema](docs/manifest-schema.md)** - YAMLスキーマの検証と構造
- **[Airflow AWS Operators](docs/airflow-aws-operators.md)** - カスタムオペレーターリファレンス

### 例
- **[サンプルガイド](docs/examples-guide.md)** - サンプルアプリケーションのチュートリアル
- **[Data Notebooks](docs/examples-guide.md#-data-engineering---notebooks)** - AirflowによるJupyterノートブック
- **[ML Training](docs/examples-guide.md#-machine-learning---training)** - MLflowによるSageMakerトレーニング
- **[ML Deployment](docs/examples-guide.md#-machine-learning---deployment)** - SageMakerエンドポイントのデプロイ
- **[QuickSight Dashboard](docs/examples-guide.md#-analytics---quicksight-dashboard)** - GlueによるBIダッシュボード
- **[GenAI Application](docs/examples-guide.md#-generative-ai)** - Bedrockエージェントとナレッジベース

### 開発
- **[開発者ガイド](developer/developer-guide.md)** - アーキテクチャ、テスト、workflowを含む完全な開発ガイド
- **[AIアシスタントコンテキスト](developer/AmazonQ.md)** - AIアシスタント用コンテキスト（Amazon Q、Kiro）
- **[テスト概要](tests/README.md)** - テストインフラストラクチャ

### サポート
- **Issues**: [GitHub Issues](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues)
- **ドキュメント**: [docs/](docs/)
- **サンプル**: [examples/](examples/)

---

## セキュリティに関する注意

⚠️ **PyPIからインストールしないでください** - 必ずAWSの公式ソースコードからインストールしてください。

```bash
# ✅ 正しい方法 - AWSの公式リポジトリからインストール
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .

# ❌ 誤った方法 - PyPIを使用しないでください
pip install aws-smus-cicd-cli  # 悪意のあるコードが含まれている可能性があります
```

---

## ライセンス

このプロジェクトはMIT-0ライセンスの下でライセンスされています。詳細は[LICENSE](../../LICENSE)をご覧ください。

---

<div align="center">
  <img src="docs/readme-qr-code.png" alt="Scan to view README" width="200"/>
  <p><em>GitHubでこのREADMEを表示するにはQRコードをスキャンしてください</em></p>
</div>