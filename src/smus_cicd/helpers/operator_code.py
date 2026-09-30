"""Shared rules for MWAA Serverless operator code (`code` field).

Single source of truth for what a workflow's ``code`` may point at and which
bundle/S3 entries are build artifacts to skip, so the deploy path
(``commands.deploy``) and the dry-run path
(``commands.dry_run.checkers.workflow_checker``) cannot drift on the accepted
types or their error message.

Operator ``code`` must be a single file MWAA Serverless accepts as-is: a ``.py``
module, a ``.sh`` script, or a pre-built ``.zip`` archive. The CLI does not
repackage — multiple modules or third-party dependencies must be packaged into a
``.zip`` (files at the archive root) by the caller.
"""

from __future__ import annotations

import os
from typing import Optional, Tuple

# Operator code file types MWAA Serverless accepts, uploaded as-is.
OPERATOR_CODE_EXTENSIONS: Tuple[str, ...] = (".py", ".sh", ".zip")


def is_code_artifact(path: str) -> bool:
    """Return True if a bundle/S3 path is a build artifact to skip.

    Compiled bytecode (``__pycache__``/``.pyc``) and JupyterLab checkpoint
    copies must never be treated as operator code.
    """
    return (
        "__pycache__/" in path
        or path.endswith(".pyc")
        or ".ipynb_checkpoints/" in path
    )


def code_ref_error(code_path: str) -> Optional[str]:
    """Validate a workflow ``code`` reference; return an error string or None.

    A valid reference names a single ``.py``/``.sh``/``.zip`` file. A directory
    (no extension) or any other extension is rejected — the caller must
    pre-package a ``.zip``. Returns ``None`` when the reference is acceptable.
    """
    ext = os.path.splitext(os.path.basename(str(code_path).rstrip("/")))[1].lower()
    if ext in OPERATOR_CODE_EXTENSIONS:
        return None
    return (
        f"operator code '{code_path}' must be a single .py/.sh file or a "
        f"pre-built .zip archive. To bundle multiple modules or third-party "
        f"dependencies, package them into a .zip (files at the archive root) "
        f"and point `code` at that .zip"
    )
