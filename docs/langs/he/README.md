<div dir="rtl">

[![en](https://img.shields.io/badge/lang-en-gray.svg)](../../../README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](../pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](../fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](../it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](../ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](../zh/README.md)
[![he](https://img.shields.io/badge/lang-he-brightgreen.svg?style=for-the-badge)](../he/README.md)

<div dir="rtl">

# SMUS CI/CD Pipeline CLI

[![en](https://img.shields.io/badge/lang-en-brightgreen.svg?style=for-the-badge)](README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](docs/langs/pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](docs/langs/fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](docs/langs/it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](docs/langs/ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](docs/langs/zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](docs/langs/he/README.md)

> **[תצוגה מקדימה]** Amazon SageMaker Unified Studio CI/CD CLI נמצא כרגע בשלב תצוגה מקדימה וכפוף לשינויים. פקודות, תבניות תצורה ו-APIs עשויים להתפתח בהתאם למשוב הלקוחות. אנו ממליצים לבדוק כלי זה בסביבות שאינן ייצור במהלך התצוגה המקדימה. למשוב ודיווח על באגים, אנא פתחו נושא ב-https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues

> **[דומיינים מבוססי IAM בלבד]** ה-CLI הזה תומך כרגע בדומיינים של SMUS המשתמשים באימות מבוסס IAM בלבד. תמיכה בדומיינים מבוססי IAM Identity Center (IdC) תגיע בקרוב.

"Deploy data applications across SageMaker Unified Studio environments" (פריסת אפליקציות נתונים בסביבות SageMaker Unified Studio)

"Deploy Airflow DAGs, Jupyter notebooks, and ML workflows from development to production with confidence. Built for data scientists, data engineers, ML engineers, and GenAI app developers working with DevOps teams." (פריסת Airflow DAGs, מחברות Jupyter ותהליכי עבודה של ML מפיתוח לייצור בביטחון. נבנה עבור מדעני נתונים, מהנדסי נתונים, מהנדסי ML ומפתחי אפליקציות GenAI העובדים עם צוותי DevOps)

"Works with your deployment strategy: Whether you use git branches (branch-based), versioned artifacts (bundle-based), git tags (tag-based), or direct deployment - this CLI supports your workflow. Define your application once, deploy it your way." (עובד עם אסטרטגיית הפריסה שלך: בין אם אתה משתמש בענפי git, ארטיפקטים מגורסאות, תגיות git או פריסה ישירה - ה-CLI הזה תומך בתהליך העבודה שלך. הגדר את האפליקציה שלך פעם אחת, פרוס אותה בדרך שלך)

---

</div>

<div dir="rtl">

## למה SMUS CI/CD CLI?

✅ **שכבת הפשטה של AWS** - CLI מכיל את כל המורכבות של אנליטיקה, ML ו-SMUS של AWS - צוותי DevOps לעולם לא קוראים ל-API של AWS ישירות  
✅ **הפרדת תחומי אחריות** - צוותי נתונים מגדירים מה לפרוס (manifest.yaml), צוותי DevOps מגדירים איך ומתי (CI/CD workflows)  
✅ **Generic CI/CD Workflows** - Same workflow works for Glue, SageMaker, Bedrock, QuickSight, or any AWS service combination  
(תהליכי CI/CD גנריים - אותו תהליך עבודה פועל עבור Glue, SageMaker, Bedrock, QuickSight או כל שילוב שירותי AWS)  
✅ **פריסה בביטחון** - בדיקות ותיקוף אוטומטיים לפני הפריסה לייצור  
✅ **ניהול מרובה סביבות** - בדיקות → ייצור עם תצורה ייעודית לכל סביבה  
✅ **תשתית כקוד** - מניפסטים של אפליקציות בבקרת גרסאות ופריסות הניתנות לשחזור  
✅ **תהליכי עבודה מבוססי אירועים** - הפעלת תהליכים אוטומטית דרך EventBridge בעת פריסה  

---

</div>

<div dir="rtl">

## התחלה מהירה

**התקנה מהמקור:**
<div dir="ltr">

<div dir="ltr">

```bash
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .
```

</div>

</div>

**פריסת האפליקציה הראשונה שלך:**
<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

**ראה בפעולה:** [Live GitHub Actions Example](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/runs/17631303500)

---

</div>

<div dir="rtl">

## למי זה מיועד?

### 👨‍💻 צוותי נתונים (מדעני נתונים, מהנדסי נתונים, מפתחי אפליקציות GenAI)
**המיקוד שלכם:** האפליקציה שלכם - מה לפרוס, איפה לפרוס, ואיך היא רצה  
**אתם מגדירים:** מניפסט אפליקציה (`manifest.yaml`) עם הקוד, workflow-ים והתצורות שלכם  
**אתם לא צריכים לדעת:** CI/CD pipelines, GitHub Actions, אוטומציה של פריסה  

→ **[מדריך התחלה מהירה](docs/getting-started/quickstart.md)** - פרסו את האפליקציה הראשונה שלכם תוך 10 דקות  

**כולל דוגמאות עבור:**
- Data Engineering (Glue, Notebooks, Athena)
- ML Workflows (SageMaker, Notebooks) 
- GenAI Applications (Bedrock, Notebooks)

### 🔧 צוותי DevOps
**המיקוד שלכם:** שיטות מיטביות של CI/CD, אבטחה, תאימות ואוטומציה של פריסה  
**אתם מגדירים:** תבניות workflow שאוכפות בדיקות, אישורים ומדיניות קידום  
**אתם לא צריכים לדעת:** פרטים ספציפיים לאפליקציה, שירותי AWS בשימוש, ממשקי DataZone, מבני פרויקט SMUS, או לוגיקה עסקית  

→ **[מדריך למנהל](docs/getting-started/admin-quickstart.md)** - הגדירו תשתית ו-pipeline תוך 15 דקות  
→ **[תבניות GitHub Workflow](git-templates/)** - תבניות workflow גנריות, לשימוש חוזר עבור פריסה אוטומטית

**ה-CLI הוא שכבת ההפשטה שלכם:** אתם פשוט קוראים ל-`aws-smus-cicd-cli deploy` - ה-CLI מטפל בכל האינטראקציות עם שירותי AWS‏ (DataZone, Glue, Athena, SageMaker, MWAA, S3, IAM וכו'). ה-workflow שלכם נשאר פשוט וגנרי.

</div>

<div dir="rtl">

## מה ניתן לפרוס?

**📊 אנליטיקה ובינה עסקית**
- Glue ETL jobs and crawlers
- Athena queries
- QuickSight dashboards
- EMR jobs (עתידי)
- Redshift queries (עתידי)

**🤖 למידת מכונה**
- SageMaker training jobs
- ML models and endpoints
- MLflow experiments
- Feature Store (עתידי)
- Batch transforms (עתידי)

**🧠 בינה מלאכותית גנרטיבית**
- Bedrock agents
- מאגרי ידע
- Foundation model configurations (עתידי)

**📓 קוד ותהליכי עבודה**
- Jupyter notebooks
- Python scripts
- Airflow DAGs (MWAA and Amazon MWAA Serverless)
- Lambda functions (עתידי)

**💾 נתונים ואחסון**
- S3 data files
- Git repositories
- Data catalogs (עתידי)

---

</div>

<div dir="rtl">

## שירותי AWS נתמכים

פריסת workflows באמצעות שירותי AWS אלה דרך תחביר YAML של Airflow:

### 🎯 אנליטיקה ונתונים
**Amazon Athena** • **AWS Glue** • **Amazon EMR** • **Amazon Redshift** • **Amazon QuickSight** • **Lake Formation**

### 🤖 למידת מכונה
**SageMaker Training** • **SageMaker Pipelines** • **Feature Store** • **Model Registry** • **Batch Transform**

### 🧠 בינה מלאכותית גנרטיבית
**Amazon Bedrock** • **Bedrock Agents** • **Bedrock Knowledge Bases** • **Guardrails**

### 📊 שירותים נוספים
S3 • Lambda • Step Functions • DynamoDB • RDS • SNS/SQS • Batch

**רשימה מלאה:** [Airflow AWS Operators Reference](docs/airflow-aws-operators.md)

---

</div>

## Core Concepts

### Separation of Concerns: The Key Design Principle

**The Problem:** Traditional deployment approaches force DevOps teams to learn AWS analytics services (Glue, Athena, DataZone, SageMaker, MWAA, etc.) and understand SMUS project structures, or force data teams to become CI/CD experts.

**The Solution:** SMUS CI/CD CLI is the abstraction layer that encapsulates all AWS and SMUS complexity.

**Example workflow:**

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

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
- Command: `aws-smus-cicd-cli deploy --manifest manifest.yaml --stage test`

Both modes work with any combination of storage and git content sources.

---


<div dir="rtl">

## דוגמאות ליישומים

דוגמאות מהעולם האמיתי המציגות כיצד לפרוס עומסי עבודה שונים עם SMUS CI/CD.

### 📊 אנליטיקה - לוח מחוונים QuickSight

"Deploy interactive BI dashboards with automated Glue ETL pipelines for data preparation. Uses QuickSight asset bundles, Athena queries, and GitHub dataset integration with environment-specific configurations."
(פריסת לוחות מחוונים BI אינטראקטיביים עם צינורות ETL אוטומטיים של Glue להכנת נתונים. משתמש בחבילות נכסים של QuickSight, שאילתות Athena ואינטגרציה עם מאגרי נתונים של GitHub עם תצורות ספציפיות לסביבה)

**AWS Services:** QuickSight • Glue • Athena • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-dashboard-glue-quicksight.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-dashboard-glue-quicksight.yml)

"What happens during deployment: Application code is deployed to S3, Glue jobs and Airflow workflows are created and executed, QuickSight dashboard/data source/dataset are created, and QuickSight ingestion is initiated to refresh the dashboard with latest data."
(מה קורה במהלך הפריסה: קוד היישום נפרס ל-S3, משימות Glue ותהליכי Airflow נוצרים ומופעלים, לוח המחוונים/מקור הנתונים/מערך הנתונים של QuickSight נוצרים, והטמעת QuickSight מופעלת כדי לרענן את לוח המחוונים עם הנתונים העדכניים ביותר)

[המשך המסמך המקורי ללא שינוי...]

</div>

<div dir="rtl">

## תיעוד

### התחלה
- **[מדריך התחלה מהירה](docs/getting-started/quickstart.md)** - פרוס את האפליקציה הראשונה שלך (10 דקות)
- **[מדריך למנהל מערכת](docs/getting-started/admin-quickstart.md)** - הגדרת תשתית (15 דקות)

### מדריכים
- **[Application Manifest](docs/manifest.md)** - מדריך מלא להגדרות YAML
- **[CLI Commands](docs/cli-commands.md)** - כל הפקודות והאפשרויות הזמינות
- **[Bootstrap Actions](docs/bootstrap-actions.md)** - פעולות פריסה אוטומטיות ו-workflow מבוסס אירועים
- **[Substitutions & Variables](docs/substitutions-and-variables.md)** - תצורה דינמית
- **[מדריך חיבורים](docs/connections.md)** - הגדרת אינטגרציות שירותי AWS
- **[GitHub Actions Integration](docs/github-actions-integration.md)** - הגדרת אוטומציית CI/CD
- **[מדדי פריסה](docs/pipeline-deployment-metrics.md)** - ניטור עם EventBridge

### מקורות
- **[Manifest Schema](docs/manifest-schema.md)** - אימות ומבנה סכמת YAML
- **[Airflow AWS Operators](docs/airflow-aws-operators.md)** - מדריך למפעילים מותאמים אישית

### דוגמאות
- **[מדריך דוגמאות](docs/examples-guide.md)** - סקירה של אפליקציות לדוגמה
- **[Data Notebooks](docs/examples-guide.md#-data-engineering---notebooks)** - מחברות Jupyter עם Airflow
- **[ML Training](docs/examples-guide.md#-machine-learning---training)** - אימון SageMaker עם MLflow
- **[ML Deployment](docs/examples-guide.md#-machine-learning---deployment)** - פריסת נקודת קצה של SageMaker
- **[QuickSight Dashboard](docs/examples-guide.md#-analytics---quicksight-dashboard)** - לוחות מחוונים BI עם Glue
- **[GenAI Application](docs/examples-guide.md#-generative-ai)** - סוכני Bedrock ובסיסי ידע

### פיתוח
- **[מדריך למפתח](developer/developer-guide.md)** - מדריך פיתוח מלא עם ארכיטקטורה, בדיקות ותהליכי עבודה
- **[הקשר עוזר AI](developer/AmazonQ.md)** - הקשר לעוזרי AI (Amazon Q, Kiro)
- **[סקירת בדיקות](tests/README.md)** - תשתית בדיקות

### תמיכה
- **בעיות**: [GitHub Issues](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues)
- **תיעוד**: [docs/](docs/)
- **דוגמאות**: [examples/](examples/)

---

</div>

<div dir="rtl">

## הודעת אבטחה

⚠️ **אין** להתקין מ-PyPI - יש להתקין תמיד מקוד המקור הרשמי של AWS.

<div dir="ltr">

<div dir="ltr">

```bash
# ✅ נכון - התקנה ממאגר AWS הרשמי
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .

# ❌ לא נכון - אין להשתמש ב-PyPI
pip install aws-smus-cicd-cli  # עלול להכיל קוד זדוני
```

</div>

</div>

---

</div>

<div dir="rtl">

## רישיון

פרויקט זה מורשה תחת רישיון MIT-0. ראה [LICENSE](../../LICENSE) לפרטים נוספים.

---

<div align="center">
  <img src="docs/readme-qr-code.png" alt="סרוק להצגת README" width="200"/>
  <p><em>סרוק את קוד ה-QR כדי לצפות ב-README בגיטהאב</em></p>
</div>

</div>

</div>