<div dir="rtl">

[![en](https://img.shields.io/badge/lang-en-gray.svg)](../../../README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](../pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](../fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](../it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](../ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](../zh/README.md)
[![he](https://img.shields.io/badge/lang-he-brightgreen.svg?style=for-the-badge)](../he/README.md)

← [Back to Main README](../../../README.md)

<div dir="rtl">

# SMUS CI/CD Pipeline CLI


> **[תחומי IAM + IdC]** CLI זה תומך בתחומי SMUS מבוססי IAM ומבוססי IAM Identity Center (IdC). עבור תחומי IdC, ייתכן שיידרש הגדרה נוספת (רשת VPC, הרשאות Lake Formation, מדיניות IAM מוטמעות) — ראה את סקריפטי ההגדרה בכל תיקיית דוגמה.

**אוטומציה של פריסת אפליקציות נתונים על פני סביבות SageMaker Unified Studio**

פרוס DAG של Airflow, מחברות Jupyter וזרימות עבודה ML מפיתוח לייצור בביטחון. נבנה עבור מדעני נתונים, מהנדסי נתונים, מהנדסי ML ומפתחי אפליקציות GenAI העובדים עם צוותי DevOps.

**עובד עם אסטרטגיית הפריסה שלך:** בין אם אתה משתמש בענפי git (מבוסס ענפים), חפצים עם גרסאות (מבוסס חבילות), תגיות git (מבוסס תגיות), או פריסה ישירה - CLI זה תומך בזרימת העבודה שלך. הגדר את האפליקציה שלך פעם אחת, פרוס אותה בדרך שלך.

---

</div>

<div dir="rtl">

## למה SMUS CI/CD CLI?

✅ **שכבת הפשטה של AWS** - ה-CLI עוטף את כל המורכבות של אנליטיקה, ML ו-SMUS ב-AWS - צוותי DevOps לעולם לא קוראים ל-API של AWS ישירות  
✅ **הפרדת אחריות** - צוותי נתונים מגדירים מה לפרוס (manifest.yaml), צוותי DevOps מגדירים איך ומתי (תהליכי CI/CD)  
✅ **תהליכי CI/CD גנריים** - אותו תהליך עובד עבור Glue, SageMaker, Bedrock, QuickSight, או כל שילוב של שירותי AWS  
✅ **פריסה בביטחון** - אימות dry-run לפני פריסה ובדיקות אוטומטיות לפני ייצור  
✅ **ניהול מרובה סביבות** - Test → Prod עם תצורה ספציפית לסביבה  
✅ **Infrastructure as Code** - מניפסטים של אפליקציות בבקרת גרסאות ופריסות ניתנות לשחזור  
✅ **תהליכים מונעי אירועים** - הפעלת תהליכים אוטומטית דרך EventBridge בעת פריסה  

---

</div>

<div dir="rtl">

## התחלה מהירה

**התקנה:**
<div dir="ltr">

<div dir="ltr">

```bash
pip install aws-smus-cicd-cli
```

</div>

</div>

**פרוס את האפליקציה הראשונה שלך:**
<div dir="ltr">

<div dir="ltr">

```bash
# אימות תצורה
aws-smus-cicd-cli describe --manifest manifest.yaml --connect

# יצירת חבילת פריסה (אופציונלי)
aws-smus-cicd-cli bundle --manifest manifest.yaml

# תצוגה מקדימה של הפריסה (ריצה יבשה)
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml --dry-run

# פריסה לסביבת בדיקה
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml

# הרצת בדיקות אימות
aws-smus-cicd-cli test --manifest manifest.yaml --targets test

# ניקוי בסיום
aws-smus-cicd-cli destroy --manifest manifest.yaml --targets test --force
```

</div>

</div>

**ראה את זה בפעולה:** [דוגמה חיה ב-GitHub Actions](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/runs/24535194879)

---

</div>

<div dir="rtl">

## יצירת Manifest באמצעות AI (Skill)

לא רוצים לכתוב `manifest.yaml` ידנית? מאגר זה כולל [Agent Skill](https://agentskills.io/) שסורק את פרויקט SageMaker Unified Studio שלכם ויוצר manifest לפריסה — וגם workflow תזמור כאשר יש צורך בכך. הוסיפו אותו לסוכן קידוד (Kiro, Amazon Q CLI, Claude Code, או כל סוכן תואם [AgentSkills](https://agentskills.io/)), ואז שאלו *"צור manifest של SMUS CI/CD עבור הפרויקט שלי."*

→ **[יצירת Manifest באמצעות AI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/generate-manifest-skill.md)** - הוראות התקנה ושימוש

---

</div>

<div dir="rtl">

## למי זה מיועד?

### 👨‍💻 צוותי Data (מדעני נתונים, מהנדסי נתונים, מפתחי אפליקציות GenAI)
**אתם מתמקדים ב:** האפליקציה שלכם - מה לפרוס, איפה לפרוס, ואיך היא רצה  
**אתם מגדירים:** מניפסט אפליקציה (`manifest.yaml`) עם הקוד, תהליכי העבודה והקונפיגורציות שלכם  
**אתם לא צריכים לדעת:** צינורות CI/CD, GitHub Actions, אוטומציית פריסה  

→ **[מדריך התחלה מהירה](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/quickstart.md)** - פרסו את האפליקציה הראשונה שלכם ב-10 דקות  

**כולל דוגמאות עבור:**
- הנדסת נתונים (Glue, Notebooks, Athena)
- תהליכי עבודה ML (SageMaker, Notebooks)
- אפליקציות GenAI (Bedrock, Notebooks)

### 🔧 צוותי DevOps
**אתם מתמקדים ב:** שיטות עבודה מומלצות CI/CD, אבטחה, ציות ואוטומציית פריסה  
**אתם מגדירים:** תבניות תהליכי עבודה שאוכפות מדיניות בדיקות, אישורים וקידום  
**אתם לא צריכים לדעת:** פרטים ספציפיים לאפליקציה, שירותי AWS בשימוש, APIs של DataZone, מבני פרויקט SMUS, או לוגיקה עסקית  

→ **[מדריך מנהל](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/admin-quickstart.md)** - הגדירו תשתית וצינורות ב-15 דקות  
→ **[תבניות GitHub Workflow](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/git-templates/)** - תבניות תהליכי עבודה גנריות וניתנות לשימוש חוזר לפריסה אוטומטית

**ה-CLI הוא שכבת ההפשטה שלכם:** אתם פשוט קוראים ל-`aws-smus-cicd-cli deploy` - ה-CLI מטפל בכל האינטראקציות עם שירותי AWS (DataZone, Glue, Athena, SageMaker, MWAA, S3, IAM וכו'). תהליכי העבודה שלכם נשארים פשוטים וגנריים.

---

</div>

<div dir="rtl">

## מה אפשר לפרוס?

**📊 אנליטיקה ו-BI**
- עבודות ETL וסורקים של Glue
- שאילתות Athena
- לוחות מחוונים של QuickSight
- עבודות EMR (עתידי)
- שאילתות Redshift (עתידי)

**🤖 Machine Learning**
- עבודות אימון של SageMaker
- מודלים ונקודות קצה של ML
- ניסויים של MLflow
- Feature Store (עתידי)
- Batch transforms (עתידי)

**🧠 Generative AI**
- סוכנים של Bedrock
- מאגרי ידע
- תצורות מודלים בסיסיים (עתידי)

**📓 קוד וזרימות עבודה**
- מחברות Jupyter
- מחברות SageMaker Unified Studio
- סקריפטים של Python
- DAGs של Airflow (MWAA ו-Amazon MWAA Serverless)
- פונקציות Lambda (עתידי)

**💾 נתונים ואחסון**
- קבצי נתונים ב-S3
- מאגרי Git
- משאבי קטלוג DataZone (מילונים, מונחי מילון, סוגי טפסים, סוגי נכסים, נכסים, מוצרי נתונים, טפסי מטא-דאטה)

---

</div>

<div dir="rtl">

## שירותי AWS נתמכים

פרוס תהליכי עבודה באמצעות שירותי AWS אלה דרך תחביר YAML של Airflow:

### 🎯 אנליטיקה ונתונים
**Amazon Athena** • **AWS Glue** • **Amazon EMR** • **Amazon Redshift** • **Amazon QuickSight** • **Lake Formation**

### 🤖 למידת מכונה
**SageMaker Training** • **SageMaker Pipelines** • **Feature Store** • **Model Registry** • **Batch Transform**

### 🧠 בינה מלאכותית גנרטיבית
**Amazon Bedrock** • **Bedrock Agents** • **Bedrock Knowledge Bases** • **Guardrails**

### 📊 שירותים נוספים
S3 • Lambda • Step Functions • DynamoDB • RDS • SNS/SQS • Batch

**ראה רשימה מלאה:** [Airflow AWS Operators Reference](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md)

---

</div>

<div dir="rtl">

## מושגי ליבה

### הפרדת אחריות: עקרון התכנון המרכזי

**הבעיה:** גישות פריסה מסורתיות מאלצות צוותי DevOps ללמוד שירותי אנליטיקה של AWS (Glue, Athena, DataZone, SageMaker, MWAA וכו') ולהבין מבני פרויקט SMUS, או מאלצות צוותי נתונים להפוך למומחי CI/CD.

**הפתרון:** SMUS CI/CD CLI הוא שכבת ההפשטה שמכילה את כל המורכבות של AWS ו-SMUS.

**דוגמה לתהליך עבודה:**

<div dir="ltr">

<div dir="ltr">

```
1. צוות DevOps                 2. צוות נתונים                    3. SMUS CI/CD CLI (ההפשטה)
   ↓                               ↓                              ↓
מגדיר את התהליך                מגדיר את התוכן                  קריאות Workflow:
- בדיקה במיזוג                 - עבודות Glue                    aws-smus-cicd-cli deploy --manifest manifest.yaml
- אישור לייצור                 - אימון SageMaker                  ↓
- סריקות אבטחה                 - שאילתות Athena                 CLI מטפל בכל המורכבות של AWS:
- כללי התראות                  - מבנה קבצים                     - DataZone APIs
                                                              - Glue/Athena/SageMaker APIs
מגדיר תשתית                                                    - פריסת MWAA
- חשבון ואזור                                                  - ניהול S3
- תפקידי IAM                                                   - הגדרת IAM
- משאבים                                                       - הקמת תשתית

עובד עבור כל אפליקציה!
אין צורך בידע בשירותי
ML/Analytics/GenAI!
```

</div>

</div>

**צוותי DevOps מתמקדים ב:**
- שיטות עבודה מומלצות של CI/CD (בדיקות, אישורים, התראות)
- שערי אבטחה ותאימות
- תזמור פריסה
- ניטור והתראות

**SMUS CI/CD CLI מטפל בכל המורכבות של AWS:**
- ניהול דומיין ופרויקט DataZone
- AWS Glue, Athena, SageMaker, MWAA APIs
- ניהול אחסון ו-artifacts ב-S3
- תפקידי והרשאות IAM
- הגדרות חיבור
- מנויים על נכסי קטלוג
- פריסת Workflow ל-Airflow
- הקמת תשתית
- בדיקות ואימות

**צוותי נתונים מתמקדים ב:**
- קוד אפליקציה ו-workflows
- אילו שירותי AWS להשתמש (Glue, Athena, SageMaker וכו')
- הגדרות סביבה
- לוגיקה עסקית

**תוצאה:** 
- **צוותי DevOps לעולם לא קוראים ישירות ל-AWS APIs** - הם פשוט קוראים ל-`aws-smus-cicd-cli deploy`
- **workflows של CI/CD הם גנריים** - אותו workflow עובד עבור אפליקציות Glue, אפליקציות SageMaker או אפליקציות Bedrock
- צוותי נתונים לעולם לא נוגעים בהגדרות CI/CD
- שני הצוותים עובדים באופן עצמאי תוך שימוש במומחיות שלהם

---

### Application Manifest
קובץ YAML הצהרתי (`manifest.yaml`) שמגדיר את אפליקציית הנתונים שלך:
- **פרטי אפליקציה** - שם, גרסה, תיאור
- **תוכן** - קוד ממאגרי git, נתונים/מודלים מאחסון, לוחות מחוונים של QuickSight
- **Workflows** - Airflow DAGs לתזמור ואוטומציה
- **Stages** - לאן לפרוס (סביבות dev, test, prod)
- **הגדרות** - הגדרות ספציפיות לסביבה, חיבורים ופעולות bootstrap

**נוצר ובבעלות צוותי נתונים.** מגדיר **מה** לפרוס ו**לאן**. אין צורך בידע ב-CI/CD.

### Application
עומס העבודה של נתונים/אנליטיקה שמפורס:
- Airflow DAGs וסקריפטים של Python
- מחברות Jupyter וקבצי נתונים
- מודלים של ML וקוד אימון
- צינורות ETL וטרנספורמציות
- סוכני GenAI ושרתי MCP
- הגדרות מודל יסוד

### Stage
סביבת פריסה (dev, test, prod) הממופה לפרויקט SageMaker Unified Studio:
- הגדרת דומיין ואזור
- שם ההגדרות של הפרויקט
- חיבורי משאבים (S3, Airflow, Athena, Glue)
- פרמטרים ספציפיים לסביבה
- מיפוי branch אופציונלי לפריסות מבוססות git

### מיפוי Stage-to-Project

כל stage של אפליקציה מפורס לפרויקט ייעודי של SageMaker Unified Studio (SMUS). פרויקט יכול לארח אפליקציה בודדת או מספר אפליקציות בהתאם לארכיטקטורה ולמתודולוגיית CI/CD שלך. פרויקטי stage הם ישויות עצמאיות עם ממשל משלהן:

- **בעלות וגישה:** לכל פרויקט stage יש קבוצה משלו של בעלים ותורמים, שעשויים להיות שונים מפרויקט הפיתוח. לפרויקטי ייצור יש בדרך כלל גישה מוגבלת בהשוואה לסביבות פיתוח.
- **Multi-Domain ו-Multi-Region:** פרויקטי stage יכולים להשתייך לדומיינים שונים של SMUS, חשבונות AWS ואזורים. לדוגמה, ה-stage dev שלך עשוי לפרוס לדומיין פיתוח ב-us-east-1, בעוד prod מפרוס לדומיין ייצור ב-eu-west-1.
- **ארכיטקטורה גמישה:** ארגונים יכולים לבחור בין פרויקטים ייעודיים לכל אפליקציה (בידוד) או פרויקטים משותפים המארחים מספר אפליקציות (איחוד), בהתבסס על דרישות אבטחה, תאימות ותפעול.

הפרדה זו מאפשרת בידוד אמיתי של סביבות עם בקרות גישה עצמאיות, גבולות תאימות ודרישות שהייה אזורית של נתונים.

### Workflow
לוגיקת תזמור שמבצעת את האפליקציה שלך. Workflows משרתים שני מטרות:

**1. בזמן פריסה:** יצירת משאבי AWS נדרשים במהלך הפריסה
- הקמת תשתית (S3 buckets, מסדי נתונים, תפקידי IAM)
- הגדרת חיבורים והרשאות
- הקמת ניטור ורישום

**2. זמן ריצה:** ביצוע צינורות נתונים ו-ML מתמשכים
- ביצוע מתוזמן (יומי, שעתי וכו')
- טריגרים מונעי אירועים (העלאות S3, קריאות API)
- עיבוד נתונים וטרנספורמציות
- אימון מודל והסקה

Workflows מוגדרים כ-Airflow DAGs (Directed Acyclic Graphs) בפורמט YAML. תומך ב-[MWAA (Managed Workflows for Apache Airflow)](https://aws.amazon.com/managed-workflows-for-apache-airflow/) וב-[Amazon MWAA Serverless](https://aws.amazon.com/blogs/big-data/introducing-amazon-mwaa-serverless/) ([מדריך משתמש](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html)).

### אוטומציה של CI/CD
workflows של GitHub Actions (או מערכות CI/CD אחרות) שמבצעות אוטומציה של הפריסה:
- **נוצר ובבעלות צוותי DevOps**
- מגדיר **איך** ו**מתי** לפרוס
- מריץ בדיקות ושערי איכות
- מנהל קידום בין targets
- אוכף מדיניות אבטחה ותאימות
- דוגמה: `.github/workflows/deploy.yml`

**תובנה מרכזית:** צוותי DevOps יוצרים workflows גנריים וניתנים לשימוש חוזר שעובדים עבור כל אפליקציה. הם לא צריכים לדעת אם האפליקציה משתמשת ב-Glue, SageMaker או Bedrock - ה-CLI מטפל בכל האינטראקציות עם שירותי AWS. ה-workflow פשוט קורא ל-`aws-smus-cicd-cli deploy` וה-CLI עושה את השאר.

### מצבי פריסה

**מבוסס Bundle (Artifact):** יצירת ארכיון עם גרסה → פריסת ארכיון ל-stages
- טוב עבור: מסלולי ביקורת, יכולת rollback, תאימות
- פקודה: `aws-smus-cicd-cli bundle` ואז `aws-smus-cicd-cli deploy --manifest app.tar.gz`

**ישיר (מבוסס Git):** פריסה ישירות ממקורות ללא artifacts ביניים
- טוב עבור: workflows פשוטים יותר, איטרציה מהירה, git כמקור אמת
- פקודה: `aws-smus-cicd-cli deploy --manifest manifest.yaml --targets test`

שני המצבים עובדים עם כל שילוב של מקורות תוכן מאחסון ו-git.

---

</div>

## Example Applications

Real-world examples showing how to deploy different workloads with SMUS CI/CD.

### 📊 Analytics - QuickSight Dashboard
Deploy interactive BI dashboards with automated Glue ETL pipelines for data preparation. Uses QuickSight asset bundles, Athena queries, and GitHub dataset integration with environment-specific configurations.

**AWS Services:** QuickSight • Glue • Athena • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-dashboard-glue-quicksight.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-dashboard-glue-quicksight.yml)

**What happens during deployment:** Application code is deployed to S3, Glue jobs and Airflow workflows are created and executed, QuickSight dashboard/data source/dataset are created, and QuickSight ingestion is initiated to refresh the dashboard with latest data.

<details>
<summary><b>📁 App Structure</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

**Key Files:**
- **Glue Jobs**: Python scripts for database setup, ETL, and validation
- **Workflow**: YAML defining Airflow DAG for orchestration
- **QuickSight Bundle**: Dashboard, datasets, and data sources
- **Tests**: Validate data quality and dashboard functionality

</details>

<details>
<summary><b>View Airflow Workflow</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

</details>

<details>
<summary><b>View Manifest</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

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

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

**Key Files:**
- **Notebooks**: 3 Jupyter notebooks for ML and analytics workflows
- **Workflow**: Parallel execution orchestration with Airflow
- **Tests**: Validate notebook execution and outputs

</details>

<details>
<summary><b>View Manifest</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

</details>

<details>
<summary><b>View Airflow Workflow</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

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

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

**Key Files:**
- **Training Script**: SageMaker training job implementation
- **Workflow**: Airflow DAG for training orchestration
- **Notebook**: Interactive training workflow
- **Tests**: Validate model registration and training

</details>

<details>
<summary><b>View Manifest</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

</details>

<details>
<summary><b>View Airflow Workflow</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

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

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

**Key Files:**
- **Inference Handler**: Custom inference logic for endpoint
- **Workflow**: Airflow DAG for endpoint deployment
- **Notebook**: Interactive deployment workflow
- **Tests**: Validate endpoint deployment and predictions

</details>

<details>
<summary><b>View Manifest</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

</details>

<details>
<summary><b>View Airflow Workflow</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

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

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

**Key Files:**
- **Agent Code**: Bedrock agent and knowledge base management
- **Workflow**: Airflow DAG for GenAI deployment
- **Notebook**: Interactive agent deployment
- **Tests**: Validate agent functionality

</details>

<details>
<summary><b>View Manifest</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

</details>

<details>
<summary><b>View Airflow Workflow</b></summary>

<div dir="ltr">

<div dir="ltr">

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

</div>

</div>

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

<div dir="ltr">

<div dir="ltr">

```bash
# Run setup for data-notebooks (IdC domain)
TEST_DOMAIN_REGION=us-east-1 python examples/analytic-workflow/data-notebooks/idc_domain_project_setup.py

# Run setup for ML training (IdC domain)
TEST_DOMAIN_REGION=us-east-1 python examples/analytic-workflow/ml/training/idc_domain_project_setup.py

# Dry run to preview changes
python examples/analytic-workflow/data-notebooks/idc_domain_project_setup.py --dry-run
```

</div>

</div>

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


<div dir="rtl">

## תיעוד

### תחילת העבודה
- **[מדריך התחלה מהירה](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/quickstart.md)** - פרוס את האפליקציה הראשונה שלך (10 דקות)
- **[מדריך למנהלים](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/getting-started/admin-quickstart.md)** - הגדרת תשתית (15 דקות)
- **[יצירת Manifest באמצעות AI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/generate-manifest-skill.md)** - כישור סוכן שסורק את הפרויקט שלך ויוצר manifest

### מדריכים
- **[Application Manifest](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest.md)** - מדריך מלא להגדרות YAML
- **[פקודות CLI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/cli-commands.md)** - כל הפקודות והאפשרויות הזמינות
- **[מדריך Rollback](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/rollback-guide.md)** - התאוששות מפריסות כושלות ואוטומציה של rollback
- **[Bootstrap Actions](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/bootstrap-actions.md)** - פעולות פריסה אוטומטיות וזרימות עבודה מונעות אירועים
- **[Substitutions & Variables](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/substitutions-and-variables.md)** - הגדרות דינמיות
- **[מדריך Connections](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/connections.md)** - הגדרת אינטגרציות עם שירותי AWS
- **[אינטגרציה עם GitHub Actions](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-actions-integration.md)** - הגדרת אוטומציה של CI/CD
- **[מדריך אפליקציה ל-GitHub Workflow](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-workflow-application-guide.md)** - מדריך למנהלי אפליקציות לפריסה ישירה מענף
- **[מדריך DevOps ל-GitHub Workflow](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/github-workflow-devops-guide.md)** - מדריך DevOps לפריסה ישירה מענף
- **[מדדי פריסה](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-deployment-metrics.md)** - ניטור באמצעות EventBridge
- **[מדריך ייבוא/ייצוא קטלוג](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/catalog-import-export-guide.md)** - קידום משאבי קטלוג DataZone בין סביבות
- **[מדריך מהיר לייבוא/ייצוא קטלוג](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/catalog-import-export-quick-reference.md)** - מדריך מהיר לפריסת קטלוג
- **[סנכרון Notebook (דוגמה E2E)](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/examples/e2e-notebook-sync/README.md)** - ייצוא וסנכרון notebooks בין סביבות (מצב bundle-deploy)
- **[הגדרת MCP](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/mcp-configuration.md)** - מדריך הגדרת שרת MCP
- **[דוגמאות שיחה עם Q CLI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/q-cli-conversation-examples.md)** - דוגמאות שיחה עם Q CLI

### עיון
- **[Manifest Schema](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/manifest-schema.md)** - אימות ומבנה סכמת YAML
- **[Airflow AWS Operators](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-aws-operators.md)** - מדריך operators מותאמים אישית
- **[סיכום Airflow ב-SMUS CI/CD](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/airflow-smus-cicd-summary.md)** - סקירה כללית של תפקיד Airflow ב-SMUS CI/CD
- **[ארכיטקטורה](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/architecture.md)** - תיעוד ארכיטקטורת CLI
- **[דיאגרמת ארכיטקטורת Pipeline](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pipeline-architecture-diagram.md)** - סקירה כללית של ארכיטקטורת pipeline CI/CD

### דוגמאות
- **[מדריך דוגמאות](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md)** - הדרכה על אפליקציות לדוגמה
- **[Data Notebooks](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-data-engineering---notebooks)** - מחברות Jupyter עם Airflow
- **[אימון ML](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---training)** - אימון SageMaker עם MLflow
- **[פריסת ML](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-machine-learning---deployment)** - פריסת endpoint של SageMaker
- **[לוח בקרה QuickSight](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-analytics---quicksight-dashboard)** - לוחות בקרה BI עם Glue
- **[אפליקציית GenAI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/examples-guide.md#-generative-ai)** - סוכני Bedrock ובסיסי ידע

### פיתוח
- **[מדריך למפתחים](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/developer/developer-guide.md)** - מדריך פיתוח מלא עם ארכיטקטורה, בדיקות וזרימות עבודה
- **[מדריך פיתוח](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/development.md)** - זרימות עבודה לפיתוח, בדיקות והנחיות תרומה
- **[פרסום ב-PyPI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/docs/pypi-publishing.md)** - הגדרת פרסום ב-PyPI
- **[הקשר לעוזר AI](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/developer/AmazonQ.md)** - הקשר לעוזרי AI (Amazon Q, Kiro)
- **[סקירת בדיקות](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/tests/README.md)** - תשתית בדיקות

### תמיכה
- **בעיות**: [GitHub Issues](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues)
- **תיעוד**: [docs/](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/docs/)
- **דוגמאות**: [examples/](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/tree/main/examples/)

---

</div>

<div dir="rtl">

## הודעת אבטחה

תמיד התקן מחבילת PyPI הרשמית של AWS או מקוד המקור.

<div dir="ltr">

<div dir="ltr">

```bash
# ✅ נכון - התקנה מחבילת PyPI הרשמית של AWS
pip install aws-smus-cicd-cli

# ✅ גם נכון - התקנה מקוד המקור הרשמי של AWS
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .
```

</div>

</div>

---

</div>

<div dir="rtl">

## רישיון

פרויקט זה מורשה תחת Apache License, Version 2.0. ראה [LICENSE](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/blob/main/LICENSE) לפרטים נוספים.

---

<div align="center">
  <img src="docs/readme-qr-code.png" alt="Scan to view README" width="200"/>
  <p><em>סרוק את קוד ה-QR כדי לצפות ב-README זה ב-GitHub</em></p>
</div>

</div>

</div>