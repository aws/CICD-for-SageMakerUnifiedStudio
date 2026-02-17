"""CDK stack for COVID ETL pipeline — replaces the MWAA workflow entirely.

Provisions all resources that the Glue workflow previously created at runtime:
- Glue databases: covid19_db, covid19_summary_db
- Glue tables: us_simplified (CSV), us_state_summary (Parquet)
- Glue jobs: setup-covid-db, summary-glue, set-permission-check
- Glue workflow with conditional triggers (setup → summary → permissions)
- IAM role for Glue jobs with S3 + Catalog + LakeFormation access
- Lake Formation permissions for specified grant roles
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    aws_lakeformation as lf,
)
from constructs import Construct


class GlueEtlStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        project_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.stage = stage
        self.project_name = project_name

        # --- Context parameters (injected by bootstrap or CLI) ---
        s3_bucket = self.node.try_get_context("s3Bucket") or ""
        s3_prefix = self.node.try_get_context("s3Prefix") or "shared"
        iam_role_name = self.node.try_get_context("iamRoleName") or ""
        grant_roles = self.node.try_get_context("grantRoles") or ""

        # S3 locations derived from the DataZone project bucket
        data_location = f"s3://{s3_bucket}/{s3_prefix}/repos/data/"
        summary_output = (
            f"s3://{s3_bucket}/{s3_prefix}/"
            f"dashboard-glue-quick-cdk/output/databases/covid19_summary_db/"
        )
        script_base = (
            f"s3://{s3_bucket}/{s3_prefix}/"
            f"dashboard-glue-quick-cdk/bundle"
        )

        # ----------------------------------------------------------------
        # Glue Databases
        # ----------------------------------------------------------------
        raw_db = glue.CfnDatabase(
            self, "CovidRawDb",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="covid19_db",
                description=f"COVID-19 raw data ({stage})",
            ),
        )

        summary_db = glue.CfnDatabase(
            self, "CovidSummaryDb",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="covid19_summary_db",
                description=f"COVID-19 summary data ({stage})",
            ),
        )

        # ----------------------------------------------------------------
        # Glue Tables
        # ----------------------------------------------------------------
        us_simplified = glue.CfnTable(
            self, "UsSimplifiedTable",
            catalog_id=self.account,
            database_name="covid19_db",
            table_input=glue.CfnTable.TableInputProperty(
                name="us_simplified",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="date", type="string"),
                        glue.CfnTable.ColumnProperty(name="country", type="string"),
                        glue.CfnTable.ColumnProperty(name="province", type="string"),
                        glue.CfnTable.ColumnProperty(name="confirmed", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="recovered", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="deaths", type="bigint"),
                    ],
                    location=data_location,
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        parameters={
                            "field.delim": ",",
                            "skip.header.line.count": "1",
                        },
                    ),
                ),
            ),
        )
        us_simplified.add_dependency(raw_db)

        us_state_summary = glue.CfnTable(
            self, "UsStateSummaryTable",
            catalog_id=self.account,
            database_name="covid19_summary_db",
            table_input=glue.CfnTable.TableInputProperty(
                name="us_state_summary",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="country", type="string"),
                        glue.CfnTable.ColumnProperty(name="total_confirmed", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="total_deaths", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="avg_daily_confirmed", type="double"),
                        glue.CfnTable.ColumnProperty(name="days_reported", type="bigint"),
                    ],
                    location=summary_output,
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    ),
                ),
            ),
        )
        us_state_summary.add_dependency(summary_db)

        # ----------------------------------------------------------------
        # IAM Role for Glue Jobs
        # ----------------------------------------------------------------
        glue_role = iam.Role(
            self, "GlueJobRole",
            role_name=f"covid-etl-glue-role-{stage}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )

        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject", "s3:PutObject",
                    "s3:DeleteObject", "s3:ListBucket",
                ],
                resources=[
                    f"arn:aws:s3:::amazon-sagemaker-*-{self.account}-*",
                    f"arn:aws:s3:::amazon-sagemaker-*-{self.account}-*/*",
                ],
            )
        )

        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:CreateDatabase", "glue:GetDatabase",
                    "glue:CreateTable", "glue:GetTable",
                    "glue:DeleteTable", "glue:UpdateTable",
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/covid19_db",
                    f"arn:aws:glue:{self.region}:{self.account}:database/covid19_summary_db",
                    f"arn:aws:glue:{self.region}:{self.account}:table/covid19_db/*",
                    f"arn:aws:glue:{self.region}:{self.account}:table/covid19_summary_db/*",
                ],
            )
        )

        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "lakeformation:GrantPermissions",
                    "lakeformation:GetDataLakeSettings",
                ],
                resources=["*"],
            )
        )

        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=[
                    f"arn:aws:athena:{self.region}:{self.account}:workgroup/primary",
                ],
            )
        )

        # ----------------------------------------------------------------
        # Glue Jobs (mirrors the 3 tasks from the original workflow)
        # ----------------------------------------------------------------
        job_role_arn = (
            f"arn:aws:iam::{self.account}:role/{iam_role_name}"
            if iam_role_name
            else glue_role.role_arn
        )

        setup_job = glue.CfnJob(
            self, "SetupCovidDbJob",
            name=f"setup-covid-db-job-{stage}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"{script_base}/glue_setup_covid_db.py",
            ),
            role=job_role_arn,
            glue_version="4.0",
            max_retries=0,
            timeout=180,
            default_arguments={
                "--BUCKET_NAME": s3_bucket,
                "--REGION_NAME": self.region,
            },
        )

        summary_job = glue.CfnJob(
            self, "SummaryGlueJob",
            name=f"summary-glue-job-{stage}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"{script_base}/glue_covid_summary_job.py",
            ),
            role=job_role_arn,
            glue_version="4.0",
            max_retries=0,
            timeout=180,
            default_arguments={
                "--DATABASE_NAME": "covid19_db",
                "--TABLE_NAME": "us_simplified",
                "--SUMMARY_DATABASE_NAME": "covid19_summary_db",
                "--S3_DATABASE_PATH": summary_output,
                "--BUCKET_NAME": s3_bucket,
            },
        )

        permission_job = glue.CfnJob(
            self, "SetPermissionCheckJob",
            name=f"set-permission-check-job-{stage}",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"{script_base}/glue_set_permission_check.py",
            ),
            role=job_role_arn,
            glue_version="4.0",
            max_retries=0,
            timeout=180,
            default_arguments={
                "--BUCKET_NAME": s3_bucket,
                "--REGION_NAME": self.region,
                "--ROLES": grant_roles,
            },
        )

        # ----------------------------------------------------------------
        # Glue Workflow (DAG: setup → summary → permissions)
        # ----------------------------------------------------------------
        workflow = glue.CfnWorkflow(
            self, "CovidEtlWorkflow",
            name=f"covid-etl-workflow-{stage}",
            description=f"COVID ETL pipeline ({stage})",
        )

        start_trigger = glue.CfnTrigger(
            self, "StartTrigger",
            name=f"covid-etl-start-{stage}",
            type="ON_DEMAND",
            workflow_name=workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(job_name=setup_job.name),
            ],
        )
        start_trigger.add_dependency(workflow)
        start_trigger.add_dependency(setup_job)

        summary_trigger = glue.CfnTrigger(
            self, "SummaryTrigger",
            name=f"covid-etl-summary-{stage}",
            type="CONDITIONAL",
            start_on_creation=True,
            workflow_name=workflow.name,
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=setup_job.name,
                        state="SUCCEEDED",
                    ),
                ],
            ),
            actions=[
                glue.CfnTrigger.ActionProperty(job_name=summary_job.name),
            ],
        )
        summary_trigger.add_dependency(workflow)
        summary_trigger.add_dependency(summary_job)

        permission_trigger = glue.CfnTrigger(
            self, "PermissionTrigger",
            name=f"covid-etl-permissions-{stage}",
            type="CONDITIONAL",
            start_on_creation=True,
            workflow_name=workflow.name,
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=summary_job.name,
                        state="SUCCEEDED",
                    ),
                ],
            ),
            actions=[
                glue.CfnTrigger.ActionProperty(job_name=permission_job.name),
            ],
        )
        permission_trigger.add_dependency(workflow)
        permission_trigger.add_dependency(permission_job)

        # ----------------------------------------------------------------
        # Lake Formation Permissions
        # ----------------------------------------------------------------
        if grant_roles:
            role_list = [r.strip() for r in grant_roles.split(",") if r.strip()]
            for idx, role_name in enumerate(role_list):
                role_arn = f"arn:aws:iam::{self.account}:role/{role_name}"

                for db_idx, db_name in enumerate(["covid19_db", "covid19_summary_db"]):
                    lf.CfnPermissions(
                        self, f"LFDbPerm{idx}Db{db_idx}",
                        data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                            data_lake_principal_identifier=role_arn,
                        ),
                        resource=lf.CfnPermissions.ResourceProperty(
                            database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                                name=db_name,
                            ),
                        ),
                        permissions=["DESCRIBE"],
                    )

                for tbl_idx, (db_name, tbl_name) in enumerate([
                    ("covid19_db", "us_simplified"),
                    ("covid19_summary_db", "us_state_summary"),
                ]):
                    lf.CfnPermissions(
                        self, f"LFTblPerm{idx}Tbl{tbl_idx}",
                        data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                            data_lake_principal_identifier=role_arn,
                        ),
                        resource=lf.CfnPermissions.ResourceProperty(
                            table_resource=lf.CfnPermissions.TableResourceProperty(
                                database_name=db_name,
                                name=tbl_name,
                            ),
                        ),
                        permissions=["DESCRIBE", "SELECT"],
                    )

        # ----------------------------------------------------------------
        # Outputs
        # ----------------------------------------------------------------
        cdk.CfnOutput(self, "RawDatabaseName", value="covid19_db")
        cdk.CfnOutput(self, "SummaryDatabaseName", value="covid19_summary_db")
        cdk.CfnOutput(self, "GlueRoleArn", value=glue_role.role_arn)
        cdk.CfnOutput(self, "WorkflowName", value=workflow.name)
        cdk.CfnOutput(self, "SetupJobName", value=setup_job.name)
        cdk.CfnOutput(self, "SummaryJobName", value=summary_job.name)
        cdk.CfnOutput(self, "PermissionJobName", value=permission_job.name)
        cdk.CfnOutput(self, "Stage", value=stage)
