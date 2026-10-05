"""Unit tests for the shared operator-code rules (helpers/operator_code.py)."""

from smus_cicd.helpers.operator_code import (
    OPERATOR_CODE_EXTENSIONS,
    code_ref_error,
    is_code_artifact,
)


def test_accepted_extensions():
    assert OPERATOR_CODE_EXTENSIONS == (".py", ".sh", ".zip")


def test_code_ref_error_accepts_supported_files():
    assert code_ref_error("code/python/greeting.py") is None
    assert code_ref_error("code/bash/greet.sh") is None
    assert code_ref_error("code/python/hello_package.zip") is None


def test_code_ref_error_rejects_directory():
    # No extension -> directory reference -> error.
    err = code_ref_error("code/python")
    assert err and "pre-built .zip" in err


def test_code_ref_error_rejects_trailing_slash_directory():
    err = code_ref_error("code/python/")
    assert err is not None


def test_code_ref_error_rejects_unsupported_extension():
    err = code_ref_error("code/python/module.txt")
    assert err and "must be a single .py/.sh file" in err


def test_is_code_artifact():
    assert is_code_artifact("code/__pycache__/mod.cpython-312.pyc")
    assert is_code_artifact("code/mod.pyc")
    assert is_code_artifact("code/.ipynb_checkpoints/x-checkpoint.py")
    assert not is_code_artifact("code/greeting.py")
    assert not is_code_artifact("code/hello_package.zip")
