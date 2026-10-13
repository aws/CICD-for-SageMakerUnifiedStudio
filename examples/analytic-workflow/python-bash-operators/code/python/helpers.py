"""Second module packaged into hello_package.zip alongside greeting.py.

``greeting.hello_world`` imports ``build_message`` from here, which only works
if the .zip archive (with both files at its root) is uploaded and unpacked
correctly - so this exercises the multi-file .zip path end to end.
"""


def build_message() -> str:
    return "Hello from PythonOperator running custom multi-file .zip code on MWAA Serverless!"
