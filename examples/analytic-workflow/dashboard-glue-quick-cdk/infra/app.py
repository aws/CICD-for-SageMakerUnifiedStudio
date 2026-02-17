#!/usr/bin/env python3
"""CDK app for the Dashboard-Glue-Quick-CDK COVID ETL pipeline infrastructure."""

import os

import aws_cdk as cdk

from stacks.glue_etl_stack import GlueEtlStack

app = cdk.App()

stage = app.node.try_get_context("stage") or "dev"
project_name = app.node.try_get_context("projectName") or "marketing"
region = app.node.try_get_context("region") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
account = os.environ.get("CDK_DEFAULT_ACCOUNT", "")

GlueEtlStack(
    app,
    f"CovidEtlInfra-{stage}",
    stage=stage,
    project_name=project_name,
    env=cdk.Environment(account=account, region=region),
)

app.synth()
