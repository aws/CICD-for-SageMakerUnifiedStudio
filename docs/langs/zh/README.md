[![en](https://img.shields.io/badge/lang-en-gray.svg)](../../../README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](../pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](../fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](../it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](../ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-brightgreen.svg?style=for-the-badge)](../zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](../he/README.md)

# SMUS CI/CD Pipeline CLI

[![en](https://img.shields.io/badge/lang-en-brightgreen.svg?style=for-the-badge)](README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](docs/langs/pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](docs/langs/fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](docs/langs/it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](docs/langs/ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](docs/langs/zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](docs/langs/he/README.md)

> **[预览版]** Amazon SageMaker Unified Studio CI/CD CLI 目前处于预览阶段，可能会发生变化。命令、配置格式和API可能会根据客户反馈进行调整。我们建议在预览期间在非生产环境中评估此工具。如需反馈和报告错误，请在此处提交问题 https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues

> **[仅支持IAM域]** 此CLI目前仅支持使用基于IAM认证的SMUS域。即将支持基于IAM Identity Center (IdC)的域。

**跨SageMaker Unified Studio环境自动部署数据应用程序**

"Deploy Airflow DAGs, Jupyter notebooks, and ML workflows from development to production with confidence. Built for data scientists, data engineers, ML engineers, and GenAI app developers working with DevOps teams." (从开发环境到生产环境可靠地部署Airflow DAG、Jupyter笔记本和ML工作流。专为与DevOps团队合作的数据科学家、数据工程师、ML工程师和GenAI应用开发人员打造。)

"Works with your deployment strategy: Whether you use git branches (branch-based), versioned artifacts (bundle-based), git tags (tag-based), or direct deployment - this CLI supports your workflow. Define your application once, deploy it your way." (适用于您的部署策略：无论您使用git分支（基于分支）、版本化制品（基于bundle）、git标签（基于标签）还是直接部署 - 此CLI都支持您的工作流程。只需定义一次应用程序，就可以按照您的方式进行部署。)

---

## 为什么选择 SMUS CI/CD CLI？

✅ **AWS 抽象层** - CLI 封装了所有 AWS analytics、ML 和 SMUS 的复杂性 - DevOps 团队无需直接调用 AWS API

✅ **关注点分离** - 数据团队定义需要部署什么（manifest.yaml），DevOps 团队定义如何以及何时部署（CI/CD workflows）

"Deploy to Glue, SageMaker, Bedrock, QuickSight, or any AWS service combination using the same workflow" (使用相同的 workflow 部署到 Glue、SageMaker、Bedrock、QuickSight 或任何 AWS 服务组合)

✅ **部署更有信心** - 生产环境部署前进行自动化测试和验证

✅ **多环境管理** - 通过环境特定配置从测试环境部署到生产环境

✅ **基础设施即代码** - 版本控制的应用程序 manifest 和可重现的部署

✅ **事件驱动的 workflow** - 通过 EventBridge 在部署时自动触发 workflow

---

## 快速入门

**从源代码安装:**
```bash
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .
```

**部署你的第一个应用程序:**
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

**查看实际运行效果:** [Live GitHub Actions Example](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/runs/17631303500)

---

## 适用对象

### 👨‍💻 数据团队（数据科学家、数据工程师、生成式 AI 应用开发者）
**您专注于:** 您的应用 - 部署什么、部署到哪里以及如何运行  
**您定义:** 包含代码、workflow 和配置的应用程序 manifest (`manifest.yaml`)  
"You don't need to know: CI/CD pipelines, GitHub Actions, deployment automation" (您无需了解: CI/CD pipeline、GitHub Actions、部署自动化)

→ **[快速入门指南](docs/getting-started/quickstart.md)** - 10分钟内部署您的第一个应用  

**包含以下示例:**
"Data Engineering (Glue, Notebooks, Athena)" (数据工程)  
"ML Workflows (SageMaker, Notebooks)" (机器学习工作流)  
"GenAI Applications (Bedrock, Notebooks)" (生成式 AI 应用)

### 🔧 DevOps 团队
**您专注于:** CI/CD 最佳实践、安全性、合规性和部署自动化  
**您定义:** 强制执行测试、审批和晋升策略的 workflow 模板  
"You don't need to know: Application-specific details, AWS services used, DataZone APIs, SMUS project structures, or business logic" (您无需了解：应用程序具体细节、使用的 AWS 服务、DataZone API、SMUS 项目结构或业务逻辑)

→ **[管理员指南](docs/getting-started/admin-quickstart.md)** - 15分钟内配置基础设施和 pipeline  
→ **[GitHub Workflow 模板](git-templates/)** - 用于自动部署的通用、可重用 workflow 模板

"The CLI is your abstraction layer: You just call `aws-smus-cicd-cli deploy` - the CLI handles all AWS service interactions (DataZone, Glue, Athena, SageMaker, MWAA, S3, IAM, etc.). Your workflows stay simple and generic." (CLI 是您的抽象层：您只需调用 `aws-smus-cicd-cli deploy` - CLI 会处理所有 AWS 服务交互。您的工作流保持简单和通用。)

---

## 您可以部署什么？

**📊 分析和商业智能**
- Glue ETL jobs and crawlers
- Athena queries
- QuickSight dashboards
- EMR jobs (未来支持)
- Redshift queries (未来支持)

**🤖 机器学习**
- SageMaker training jobs
- ML models and endpoints
- MLflow experiments
- Feature Store (未来支持)
- Batch transforms (未来支持)

**🧠 生成式 AI**
- Bedrock agents
- Knowledge bases
- Foundation model configurations (未来支持)

**📓 代码和工作流**
- Jupyter notebooks
- Python scripts
- Airflow DAGs (MWAA and Amazon MWAA Serverless)
- Lambda functions (未来支持)

**💾 数据和存储**
- S3 data files
- Git repositories
- Data catalogs (未来支持)

---

## 支持的 AWS 服务

通过 Airflow YAML 语法部署工作流，支持以下 AWS 服务：

### 🎯 分析和数据
**Amazon Athena** • **AWS Glue** • **Amazon EMR** • **Amazon Redshift** • **Amazon QuickSight** • **Lake Formation**

### 🤖 机器学习
**SageMaker Training** • **SageMaker Pipelines** • **Feature Store** • **Model Registry** • **Batch Transform**

### 🧠 生成式 AI
**Amazon Bedrock** • **Bedrock Agents** • **Bedrock Knowledge Bases** • **Guardrails**

### 📊 其他服务
S3 • Lambda • Step Functions • DynamoDB • RDS • SNS/SQS • Batch

**查看完整列表：** [Airflow AWS Operators Reference](docs/airflow-aws-operators.md)

---

## 核心概念

### 关注点分离：关键设计原则

**问题：**传统部署方法迫使 DevOps 团队学习 AWS 分析服务（Glue, Athena, DataZone, SageMaker, MWAA 等）并理解 SMUS 项目结构，或迫使数据团队成为 CI/CD 专家。

**解决方案：**SMUS CI/CD CLI 是封装所有 AWS 和 SMUS 复杂性的抽象层。

**Example workflow:** (工作流示例：)

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

**DevOps 团队专注于：**
- CI/CD 最佳实践（测试、审批、通知）
- 安全和合规检查
- 部署编排
- 监控和告警

**SMUS CI/CD CLI handles ALL AWS complexity:** (SMUS CI/CD CLI 处理所有 AWS 复杂性：)
- DataZone domain and project management
- AWS Glue, Athena, SageMaker, MWAA APIs
- S3 storage and artifact management
- IAM roles and permissions
- Connection configurations
- Catalog asset subscriptions
- Workflow deployment to Airflow
- Infrastructure provisioning
- Testing and validation

**数据团队专注于：**
- 应用代码和工作流
- 选择使用哪些 AWS 服务（Glue、Athena、SageMaker 等）
- 环境配置
- 业务逻辑

**结果：**
- **DevOps 团队从不直接调用 AWS API** - 他们只需调用 `aws-smus-cicd-cli deploy`
- **CI/CD workflows are generic** - same workflow works for Glue apps, SageMaker apps, or Bedrock apps (CI/CD 工作流是通用的 - 同样的工作流适用于 Glue 应用、SageMaker 应用或 Bedrock 应用)
- 数据团队从不接触 CI/CD 配置
- 两个团队都能独立运用各自的专长工作

---

### 应用程序清单
一个声明式 YAML 文件（`manifest.yaml`）定义您的数据应用：
- **应用详情** - 名称、版本、描述
- **内容** - 来自 git 仓库的代码、来自存储的数据/模型、QuickSight 仪表板
- **工作流** - 用于编排和自动化的 Airflow DAG
- **阶段** - 部署位置（开发、测试、生产环境）
- **配置** - 特定环境的设置、连接和引导操作

**由数据团队创建和拥有。**定义**部署什么**和**部署到哪里**。无需 CI/CD 知识。

### 应用程序
您正在部署的数据/分析工作负载：
- Airflow DAG 和 Python 脚本
- Jupyter 笔记本和数据文件
- ML 模型和训练代码
- ETL pipeline 和转换
- GenAI 代理和 MCP 服务器
- 基础模型配置

### 阶段
映射到 SageMaker Unified Studio 项目的部署环境（开发、测试、生产）：
- 域和区域配置
- 项目名称和设置
- 资源连接（S3、Airflow、Athena、Glue）
- 特定环境的参数
- 基于 git 部署的可选分支映射

### Stage-to-Project Mapping (阶段到项目的映射)

每个应用程序阶段部署到专用的 SageMaker Unified Studio (SMUS) 项目。根据您的架构和 CI/CD 方法，一个项目可以托管单个应用程序或多个应用程序。阶段项目是具有自己治理的独立实体：

- **所有权和访问：**每个阶段项目都有自己的所有者和贡献者，可能与开发项目不同。生产项目通常比开发环境有更严格的访问限制。
- **多域和多区域：**阶段项目可以属于不同的 SMUS 域、AWS 账户和区域。例如，您的开发阶段可能部署到 us-east-1 的开发域，而生产部署到 eu-west-1 的生产域。
- **灵活架构：**组织可以根据安全、合规和运营要求，选择每个应用程序专用项目（隔离）或托管多个应用程序的共享项目（整合）。

这种分离实现了真正的环境隔离，具有独立的访问控制、合规边界和区域数据驻留要求。

### 工作流
执行应用程序的编排逻辑。工作流服务于两个目的：

**1. 部署时：**在部署期间创建所需的 AWS 资源
- 配置基础设施（S3 存储桶、数据库、IAM 角色）
- 配置连接和权限
- 设置监控和日志记录

**2. 运行时：**执行持续的数据和 ML pipeline
- 计划执行（每日、每小时等）
- 事件驱动触发器（S3 上传、API 调用）
- 数据处理和转换
- 模型训练和推理

工作流以 YAML 格式定义为 Airflow DAG（有向无环图）。支持 [MWAA (Managed Workflows for Apache Airflow)](https://aws.amazon.com/managed-workflows-for-apache-airflow/) 和 [Amazon MWAA Serverless](https://aws.amazon.com/blogs/big-data/introducing-amazon-mwaa-serverless/) ([用户指南](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html))。

### CI/CD 自动化
自动化部署的 GitHub Actions 工作流（或其他 CI/CD 系统）：
- **由 DevOps 团队创建和拥有**
- 定义**如何**和**何时**部署
- 运行测试和质量检查
- 管理跨目标的升级
- 执行安全和合规策略
- 示例：`.github/workflows/deploy.yml`

**关键见解：**DevOps 团队创建适用于任何应用程序的通用、可重用工作流。他们不需要知道应用程序使用 Glue、SageMaker 还是 Bedrock - CLI 处理所有 AWS 服务交互。工作流只需调用 `aws-smus-cicd-cli deploy`，CLI 就会完成其余工作。

### 部署模式

**Bundle-based (Artifact):** Create versioned archive → deploy archive to stages (基于包的（制品）：创建版本化归档 → 将归档部署到各阶段)
- 适用于：审计跟踪、回滚能力、合规性
- 命令：`aws-smus-cicd-cli bundle` 然后 `aws-smus-cicd-cli deploy --manifest app.tar.gz`

**Direct (Git-based):** Deploy directly from sources without intermediate artifacts (直接（基于 Git）：直接从源代码部署，无中间制品)
- 适用于：更简单的工作流、快速迭代、以 git 为真实来源
- 命令：`aws-smus-cicd-cli deploy --manifest manifest.yaml --stage test`

这两种模式都适用于任何存储和 git 内容源的组合。

---

## 示例应用

展示如何使用 SMUS CI/CD 部署不同工作负载的实际案例。

### 📊 分析 - QuickSight 仪表板

部署交互式 BI 仪表板,包含用于数据准备的自动化 Glue ETL 管道。使用 QuickSight 资产包、Athena 查询和 GitHub 数据集集成,支持环境特定配置。

**AWS 服务:** QuickSight • Glue • Athena • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-dashboard-glue-quicksight.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-dashboard-glue-quicksight.yml)

**部署过程:** 应用代码部署到 S3,创建并执行 Glue 作业和 Airflow 工作流,创建 QuickSight 仪表板/数据源/数据集,并启动 QuickSight 摄取以使用最新数据刷新仪表板。

<details>
<summary><b>📁 应用结构</b></summary>

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

**主要文件:**
- **Glue 作业**: 用于数据库设置、ETL 和验证的 Python 脚本
- **工作流**: 定义用于编排的 Airflow DAG 
- **QuickSight 包**: 仪表板、数据集和数据源
- **测试**: 验证数据质量和仪表板功能

</details>

[View Full Example →](docs/examples-guide.md#-analytics---quicksight-dashboard)

---

### 📓 数据工程 - Notebooks

"Deploy Jupyter notebooks with parallel execution orchestration for data analysis and ETL workflows" (部署 Jupyter notebooks 用于数据分析和 ETL 工作流,支持并行执行编排。展示了与 MLflow 集成的 notebook 部署,用于实验跟踪。)

**AWS Services:** SageMaker Notebooks • MLflow • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-data-notebooks.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-data-notebooks.yml)

"What happens during deployment:" (部署过程:notebooks 和工作流定义上传到 S3,创建 Airflow DAG 用于并行 notebook 执行,配置 MLflow 连接用于实验跟踪,notebooks 可按需或按计划运行。)

<details>
<summary><b>📁 应用结构</b></summary>

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

**主要文件:**
- **Notebooks**: 3个用于 ML 和分析工作流的 Jupyter notebooks
- **工作流**: 使用 Airflow 的并行执行编排
- **测试**: 验证 notebook 执行和输出

</details>

[View Full Example →](docs/examples-guide.md#-data-engineering---notebooks)

[继续翻译剩余内容...]

## 文档

### 入门指南
- **[快速入门指南](docs/getting-started/quickstart.md)** - 部署你的第一个应用（10分钟）
- **[管理员指南](docs/getting-started/admin-quickstart.md)** - 设置基础设施（15分钟）

### 指南
- **[Application Manifest](docs/manifest.md)** - 完整的YAML配置参考
- **[CLI Commands](docs/cli-commands.md)** - 所有可用的命令和选项
- **[Bootstrap Actions](docs/bootstrap-actions.md)** - 自动化部署操作和事件驱动workflow
- **[Substitutions & Variables](docs/substitutions-and-variables.md)** - 动态配置
- **[连接指南](docs/connections.md)** - 配置AWS服务集成
- **[GitHub Actions Integration](docs/github-actions-integration.md)** - CI/CD自动化设置
- **[Deployment Metrics](docs/pipeline-deployment-metrics.md)** - 使用EventBridge进行监控

### 参考
- **[Manifest Schema](docs/manifest-schema.md)** - YAML架构验证和结构
- **[Airflow AWS Operators](docs/airflow-aws-operators.md)** - 自定义operator参考

### 示例
- **[示例指南](docs/examples-guide.md)** - 示例应用程序演练
- **[Data Notebooks](docs/examples-guide.md#-data-engineering---notebooks)** - 带有Airflow的Jupyter notebooks
- **[ML Training](docs/examples-guide.md#-machine-learning---training)** - 使用MLflow的SageMaker训练
- **[ML Deployment](docs/examples-guide.md#-machine-learning---deployment)** - SageMaker endpoint部署
- **[QuickSight Dashboard](docs/examples-guide.md#-analytics---quicksight-dashboard)** - 使用Glue的BI仪表板
- **[GenAI Application](docs/examples-guide.md#-generative-ai)** - Bedrock agents和知识库

### 开发
- **[开发者指南](developer/developer-guide.md)** - 完整的开发指南，包含架构、测试和workflow
- **[AI Assistant Context](developer/AmazonQ.md)** - AI助手上下文（Amazon Q, Kiro）
- **[测试概述](tests/README.md)** - 测试基础设施

### 支持
- **问题**: [GitHub Issues](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues)
- **文档**: [docs/](docs/)
- **示例**: [examples/](examples/)

---

## 安全提示

⚠️ **请勿**从 PyPI 安装 - 务必从 AWS 官方源代码安装。

```bash
# ✅ 正确 - 从 AWS 官方仓库安装
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .

# ❌ 错误 - 不要使用 PyPI
pip install aws-smus-cicd-cli  # 可能包含恶意代码
```

---

## 许可证

本项目采用 MIT-0 许可证。详情请查看 [LICENSE](../../LICENSE)。

---

<div align="center">
  <img src="docs/readme-qr-code.png" alt="扫描查看 README" width="200"/>
  <p><em>扫描二维码在 GitHub 上查看此 README</em></p>
</div>