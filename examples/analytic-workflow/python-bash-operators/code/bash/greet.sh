#!/usr/bin/env bash
# Operator code for the BashOperator task.
#
# This single .sh file is referenced directly by the workflow's `code` field.
# MWAA Serverless uploads it as the Code package; the BashOperator invokes it
# from the DAGs working directory (bash_command: "bash greet.sh").
set -euo pipefail

echo "Hello from BashOperator running custom code on MWAA Serverless!"
date
