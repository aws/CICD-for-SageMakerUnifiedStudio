"""Operator code for the PythonOperator task (entry module).

This module and ``helpers.py`` are packaged together into
``hello_package.zip`` with both files at the archive root. The workflow's
``code`` field points at that .zip, which the CLI uploads to S3 as-is; MWAA
Serverless extracts it into the DAGs directory, so ``import helpers`` resolves
and ``greeting.hello_world`` is callable at run time.

The .zip is committed as a pre-built artifact - the CLI uploads it verbatim and
does not repackage. To rebuild it after editing these modules:

    cd code/python
    zip -j hello_package.zip greeting.py helpers.py -x "*__pycache__*" "*.pyc"
"""

import helpers


def hello_world():
    """Callable invoked by PythonOperator (python_callable: greeting.hello_world)."""
    message = helpers.build_message()
    print(message)
    return "python-operator-ok"
