"""Unit tests for _generate_workflow_name character sanitization.

MWAA Serverless workflow names allow only [A-Za-z0-9_]. A DataZone project
name containing a space (e.g. "Silver Layer Project") previously flowed into
CreateWorkflow and failed with a 400, because only hyphens were replaced.
"""

import re
from types import SimpleNamespace

from smus_cicd.commands.deploy import _generate_workflow_name


def _cfg(project_name):
    return SimpleNamespace(project=SimpleNamespace(name=project_name))


def test_hyphen_still_becomes_underscore():
    name = _generate_workflow_name("my-app", "silver-etl", _cfg("Gold-Proj"))
    assert name == "my_app_Gold_Proj_silver_etl"


def test_space_is_sanitized():
    name = _generate_workflow_name("MyApp", "silver_etl", _cfg("Silver Layer Project"))
    assert " " not in name
    assert name == "MyApp_Silver_Layer_Project_silver_etl"


def test_only_allowed_chars_remain():
    name = _generate_workflow_name("a.b", "c/d", _cfg("E F!"))
    assert re.fullmatch(r"[A-Za-z0-9_]+", name)
