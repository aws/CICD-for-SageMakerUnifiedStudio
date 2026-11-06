"""Unit tests for the direct (bundle-less) git deploy support added for the
Git V2 end-to-end example (feat(deploy): Direct deploy from content.git).

Covers the four behavior changes called out in that commit, none of which
had dedicated unit tests before this file:

1. ``manifest_requires_bundle()`` — a manifest sourced only from
   ``content.git`` (or local-only ``content.storage``) does not require a
   bundle archive; a ``content.storage`` item with a ``connectionName``
   still does.
2. ``_resolve_deployment_s3_location()`` — falls back to git results when
   storage results have no usable S3 URI, and still prefers storage results
   when both are present (backward compatibility, since storage is passed
   first by callers).
3. ``_git_path_matches()`` / ``_filter_git_tree()`` — the
   ``content.git[].include``/``exclude`` pattern forms documented in the
   manifest and in ``docs/manifest.md`` (exact path, directory prefix,
   recursive prefix ``**``, glob within a directory, the ``fnmatch``
   crosses-directory-separators caveat), and that filtering actually prunes
   files on disk.
4. ``_deploy_git_direct()`` — clones a repository, applies
   ``content.git[].include``/``exclude`` before upload, and skips upload
   entirely with no files returned when the filter matches nothing.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from typing import List, Optional
from unittest.mock import MagicMock, patch

from smus_cicd.commands.deploy import (
    _deploy_git_direct,
    _filter_git_tree,
    _git_path_matches,
    _resolve_deployment_s3_location,
)
from smus_cicd.helpers.bundle_storage import manifest_requires_bundle

# ---------------------------------------------------------------------------
# 1. manifest_requires_bundle()
# ---------------------------------------------------------------------------


@dataclass
class _StorageItem:
    name: str
    connectionName: Optional[str] = None


@dataclass
class _GitItem:
    repository: str
    url: str = "https://example.com/repo.git"


@dataclass
class _Content:
    storage: List[_StorageItem] = field(default_factory=list)
    git: List[_GitItem] = field(default_factory=list)


@dataclass
class _Manifest:
    content: Optional[_Content] = None


class TestManifestRequiresBundle:
    """A git-only (or local-storage-only) manifest never requires a bundle."""

    def test_git_only_manifest_does_not_require_bundle(self):
        manifest = _Manifest(
            content=_Content(git=[_GitItem(repository="repo1")], storage=[])
        )
        assert manifest_requires_bundle(manifest) is False

    def test_local_storage_only_does_not_require_bundle(self):
        """A content.storage item with no connectionName is local-filesystem
        content, deployed directly from disk — no bundle needed either.
        """
        manifest = _Manifest(
            content=_Content(storage=[_StorageItem(name="code", connectionName=None)])
        )
        assert manifest_requires_bundle(manifest) is False

    def test_storage_item_with_connection_requires_bundle(self):
        """A storage item with a connectionName was captured from a remote
        S3 connection at bundle time and cannot be reproduced at deploy
        time — a bundle is mandatory.
        """
        manifest = _Manifest(
            content=_Content(
                storage=[_StorageItem(name="code", connectionName="default.s3_shared")]
            )
        )
        assert manifest_requires_bundle(manifest) is True

    def test_mixed_git_and_local_storage_does_not_require_bundle(self):
        manifest = _Manifest(
            content=_Content(
                git=[_GitItem(repository="repo1")],
                storage=[_StorageItem(name="code", connectionName=None)],
            )
        )
        assert manifest_requires_bundle(manifest) is False

    def test_mixed_git_and_remote_storage_requires_bundle(self):
        """Even one connectionName-backed storage item is enough to force
        a bundle, regardless of any git items also present.
        """
        manifest = _Manifest(
            content=_Content(
                git=[_GitItem(repository="repo1")],
                storage=[_StorageItem(name="code", connectionName="default.s3_shared")],
            )
        )
        assert manifest_requires_bundle(manifest) is True

    def test_no_content_does_not_require_bundle(self):
        manifest = _Manifest(content=None)
        assert manifest_requires_bundle(manifest) is False

    def test_empty_content_does_not_require_bundle(self):
        manifest = _Manifest(content=_Content())
        assert manifest_requires_bundle(manifest) is False


# ---------------------------------------------------------------------------
# 2. _resolve_deployment_s3_location()
# ---------------------------------------------------------------------------


class TestResolveDeploymentS3Location:
    """Resolves the deployed S3 bucket/prefix, preferring earlier groups."""

    def test_falls_back_to_git_results_when_storage_empty(self):
        """A git-only manifest has no storage_results at all; the function
        must still resolve a location from git_results so workflow.create
        doesn't abort with 'S3 location not available'.
        """
        storage_results = []
        git_results = [(["a.yaml", "b.yaml"], "s3://my-bucket/Git-V2-test-repo-1/")]

        bucket, prefix = _resolve_deployment_s3_location(storage_results, git_results)

        assert bucket == "my-bucket"
        assert prefix == "Git-V2-test-repo-1/"

    def test_prefers_storage_results_when_both_present(self):
        """Backward compatibility: when both groups have a usable URI, the
        first group passed (conventionally storage) wins.
        """
        storage_results = [(["x.py"], "s3://storage-bucket/code/")]
        git_results = [(["y.yaml"], "s3://git-bucket/repo/")]

        bucket, prefix = _resolve_deployment_s3_location(storage_results, git_results)

        assert bucket == "storage-bucket"
        assert prefix == "code/"

    def test_skips_storage_items_with_no_s3_uri_falls_to_git(self):
        """A storage item that failed to deploy (None s3_uri) must not block
        resolution — the scan continues into later storage entries and then
        the git group.
        """
        storage_results = [(None, None)]
        git_results = [(["a.yaml"], "s3://git-bucket/repo/")]

        bucket, prefix = _resolve_deployment_s3_location(storage_results, git_results)

        assert bucket == "git-bucket"
        assert prefix == "repo/"

    def test_skips_malformed_bucketless_uri(self):
        """'s3://' with no bucket (a connection with an empty s3Uri) is
        unusable and must not be returned as a false-positive location.
        """
        storage_results = [(["x"], "s3://")]
        git_results = [(["a.yaml"], "s3://real-bucket/repo/")]

        bucket, prefix = _resolve_deployment_s3_location(storage_results, git_results)

        assert bucket == "real-bucket"
        assert prefix == "repo/"

    def test_no_usable_uri_returns_none_none(self):
        bucket, prefix = _resolve_deployment_s3_location([(None, None)], [])
        assert (bucket, prefix) == (None, None)

    def test_empty_prefix_when_uri_has_no_path(self):
        bucket, prefix = _resolve_deployment_s3_location([(["x"], "s3://bucket-only")])
        assert bucket == "bucket-only"
        assert prefix == ""

    def test_none_and_empty_groups_are_skipped_without_error(self):
        """Callers may pass None or [] for a group that produced nothing;
        the function must tolerate both without raising.
        """
        bucket, prefix = _resolve_deployment_s3_location(
            None, [], [(["a.yaml"], "s3://bucket/prefix/")]
        )
        assert bucket == "bucket"
        assert prefix == "prefix/"


# ---------------------------------------------------------------------------
# 3. _git_path_matches() / _filter_git_tree()
# ---------------------------------------------------------------------------


class TestGitPathMatches:
    """Pattern forms documented in the manifest and docs/manifest.md."""

    def test_exact_file_match(self):
        assert _git_path_matches(
            "notebooks/sales-summary.ipynb", ["notebooks/sales-summary.ipynb"]
        )
        assert not _git_path_matches(
            "notebooks/other.ipynb", ["notebooks/sales-summary.ipynb"]
        )

    def test_directory_prefix_without_trailing_slash(self):
        assert _git_path_matches("notebooks/a.ipynb", ["notebooks"])
        assert not _git_path_matches("notebooks.txt", ["notebooks"])

    def test_directory_prefix_with_trailing_slash(self):
        assert _git_path_matches("notebooks/a.ipynb", ["notebooks/"])

    def test_recursive_prefix_double_star(self):
        assert _git_path_matches("jobs/sub/a.py", ["jobs/**"])
        assert _git_path_matches("jobs/a.py", ["jobs/**"])
        assert not _git_path_matches("other/a.py", ["jobs/**"])

    def test_glob_within_a_directory(self):
        assert _git_path_matches("notebooks/sales.ipynb", ["notebooks/*.ipynb"])
        # fnmatch's '*' crosses directory separators (the documented caveat
        # exercised in test_bare_glob_crosses_directory_separators below), so
        # a nested file under notebooks/ also matches this pattern — anchoring
        # to a single directory level is not possible with a bare '*'.
        assert _git_path_matches("notebooks/sub/sales.ipynb", ["notebooks/*.ipynb"])
        assert not _git_path_matches("other/sales.ipynb", ["notebooks/*.ipynb"])

    def test_bare_glob_crosses_directory_separators(self):
        """Documented caveat: the matcher is fnmatch-based, so a bare
        '*.ipynb' matches both a root-level file and a nested one — unlike
        a shell glob, which would only match the root-level file.
        """
        assert _git_path_matches("sales.ipynb", ["*.ipynb"])
        assert _git_path_matches("notebooks/sales.ipynb", ["*.ipynb"])

    def test_nested_wildcard(self):
        assert _git_path_matches("jobs/myjob/script.py", ["jobs/*/*.py"])
        assert not _git_path_matches("jobs/script.py", ["jobs/*/*.py"])

    def test_no_patterns_matches_nothing(self):
        assert not _git_path_matches("a.py", [])

    def test_empty_and_whitespace_patterns_are_ignored(self):
        assert not _git_path_matches("a.py", ["", "   "])

    def test_separator_normalization_uses_host_os_sep(self):
        """The function normalizes rel_path via `rel_path.replace(os.sep, '/')`
        — on this host (os.sep == '/'), that's a no-op, so paths are expected
        to already use forward slashes (as os.path.relpath produces on
        POSIX). This documents the actual, host-dependent behavior rather
        than asserting cross-platform normalization that doesn't happen.
        """
        assert _git_path_matches("notebooks/a.ipynb", ["notebooks/*.ipynb"])
        assert os.sep == "/"  # sanity: this test suite runs on POSIX


class TestFilterGitTree:
    """Prunes a cloned tree in place to the include/exclude selection."""

    def _make_tree(self, root):
        os.makedirs(os.path.join(root, "notebooks"))
        os.makedirs(os.path.join(root, "querybooks"))
        for rel in [
            "README.md",
            ".gitignore",
            "notebooks/sales.ipynb",
            "notebooks/ported-notebook.ipynb",
            "querybooks/report.sqlnb",
        ]:
            path = os.path.join(root, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as fh:
                fh.write("x")

    def test_no_include_keeps_everything(self, tmp_path):
        self._make_tree(tmp_path)
        kept = _filter_git_tree(str(tmp_path), include=None, exclude=None)
        assert kept == sorted(
            [
                "README.md",
                ".gitignore",
                "notebooks/sales.ipynb",
                "notebooks/ported-notebook.ipynb",
                "querybooks/report.sqlnb",
            ]
        )

    def test_include_prunes_to_matching_files_only(self, tmp_path):
        self._make_tree(tmp_path)
        kept = _filter_git_tree(
            str(tmp_path), include=["notebooks/*.ipynb"], exclude=None
        )
        assert kept == sorted(
            ["notebooks/sales.ipynb", "notebooks/ported-notebook.ipynb"]
        )
        # README.md must actually be deleted from disk, not just excluded
        # from the returned list.
        assert not os.path.exists(tmp_path / "README.md")
        assert not os.path.exists(tmp_path / "querybooks")  # emptied dir removed

    def test_exclude_applied_after_include(self, tmp_path):
        self._make_tree(tmp_path)
        kept = _filter_git_tree(
            str(tmp_path),
            include=["notebooks/**"],
            exclude=["notebooks/ported-notebook.ipynb"],
        )
        assert kept == ["notebooks/sales.ipynb"]
        assert not os.path.exists(tmp_path / "notebooks" / "ported-notebook.ipynb")

    def test_empty_directories_are_pruned(self, tmp_path):
        self._make_tree(tmp_path)
        _filter_git_tree(str(tmp_path), include=["README.md"], exclude=None)
        assert not os.path.exists(tmp_path / "notebooks")
        assert not os.path.exists(tmp_path / "querybooks")
        assert os.path.exists(tmp_path / "README.md")

    def test_no_matches_returns_empty_list(self, tmp_path):
        self._make_tree(tmp_path)
        kept = _filter_git_tree(
            str(tmp_path), include=["does-not-exist/*.foo"], exclude=None
        )
        assert kept == []


# ---------------------------------------------------------------------------
# 4. _deploy_git_direct()
# ---------------------------------------------------------------------------


@dataclass
class _GitTargetConfig:
    """Stub for deployment_configuration.git[] (GitTargetConfig)."""

    name: str
    connectionName: str = "default.s3_shared"
    targetDirectory: str = ""


@dataclass
class _GitContentConfig:
    """Stub for content.git[] (GitContentConfig)."""

    repository: str
    url: str
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None


def _write_fake_clone(clone_path, files):
    """Simulate `git clone` populating clone_path with the given relative
    file paths (contents are irrelevant for these tests).
    """

    def _fake_clone(cmd, **kwargs):
        for rel in files:
            full = os.path.join(clone_path, rel)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w") as fh:
                fh.write("x")
        return MagicMock(returncode=0)

    return _fake_clone


class TestDeployGitDirect:
    """Clones, filters via include/exclude, and uploads — all without a
    bundle archive.
    """

    def test_applies_include_exclude_before_upload(self, tmp_path):
        manifest = _Manifest(
            content=_Content(
                git=[
                    _GitContentConfig(
                        repository="Git-V2-test-repo-1",
                        url="https://example.com/repo.git",
                        include=["notebooks/*.ipynb"],
                    )
                ]
            )
        )
        git_config = _GitTargetConfig(
            name="Git-V2-test-repo-1", targetDirectory="Git-V2-test-repo-1"
        )

        files_in_clone = [
            "README.md",
            "notebooks/sales.ipynb",
            "notebooks/other.ipynb",
        ]
        captured_upload_dir = {}

        def _fake_deploy_files(source_dir, connection, target_dir, region, _src):
            # Snapshot which files survive the filter at upload time.
            staged = []
            for r, _d, fnames in os.walk(source_dir):
                for f in fnames:
                    staged.append(
                        os.path.relpath(os.path.join(r, f), source_dir).replace(
                            os.sep, "/"
                        )
                    )
            captured_upload_dir["staged"] = sorted(staged)
            return True

        with patch("smus_cicd.commands.deploy.tempfile.TemporaryDirectory") as mock_td:
            # Use a real temp dir under tmp_path so the fake clone can write
            # real files for _filter_git_tree to prune.
            real_temp = str(tmp_path / "clone_root")
            os.makedirs(real_temp)
            mock_td.return_value.__enter__.return_value = real_temp

            with patch(
                "subprocess.run",
                side_effect=_write_fake_clone(
                    os.path.join(real_temp, "Git-V2-test-repo-1"), files_in_clone
                ),
            ), patch(
                "smus_cicd.commands.deploy._get_project_connection",
                return_value={"s3Uri": "s3://my-bucket/Git-V2-test-repo-1/"},
            ), patch(
                "smus_cicd.commands.deploy.deployment.deploy_files",
                side_effect=_fake_deploy_files,
            ):
                deployed_files, s3_uri = _deploy_git_direct(
                    git_config, manifest, "test-project", {"region": "us-east-1"}
                )

        # Both notebooks/*.ipynb files survive the filter; README.md (outside
        # the include pattern) is pruned before upload.
        assert captured_upload_dir["staged"] == [
            "notebooks/other.ipynb",
            "notebooks/sales.ipynb",
        ]
        assert s3_uri == "s3://my-bucket/Git-V2-test-repo-1/"
        assert deployed_files is not None
        assert "notebooks/sales.ipynb" in deployed_files
        assert "notebooks/other.ipynb" in deployed_files
        assert "README.md" not in deployed_files

    def test_no_matches_skips_upload_entirely(self, tmp_path):
        """If include/exclude matches nothing, _deploy_git_direct must not
        call deploy_files at all and must return (None, None).
        """
        manifest = _Manifest(
            content=_Content(
                git=[
                    _GitContentConfig(
                        repository="repo1",
                        url="https://example.com/repo.git",
                        include=["does-not-exist/*.foo"],
                    )
                ]
            )
        )
        git_config = _GitTargetConfig(name="repo1")

        real_temp = str(tmp_path / "clone_root")
        os.makedirs(real_temp)

        with patch("smus_cicd.commands.deploy.tempfile.TemporaryDirectory") as mock_td:
            mock_td.return_value.__enter__.return_value = real_temp
            with patch(
                "subprocess.run",
                side_effect=_write_fake_clone(
                    os.path.join(real_temp, "repo1"), ["README.md"]
                ),
            ), patch(
                "smus_cicd.commands.deploy.deployment.deploy_files"
            ) as mock_deploy:
                deployed_files, s3_uri = _deploy_git_direct(
                    git_config, manifest, "test-project", {"region": "us-east-1"}
                )

        mock_deploy.assert_not_called()
        assert deployed_files is None
        assert s3_uri is None

    def test_missing_content_git_entry_returns_none(self):
        """A deployment_configuration.git[] item with no matching
        content.git[] entry (name mismatch, or content.git not declared) is
        a manifest error — must fail gracefully, not raise.
        """
        manifest = _Manifest(content=_Content(git=[]))
        git_config = _GitTargetConfig(name="repo1")

        deployed_files, s3_uri = _deploy_git_direct(
            git_config, manifest, "test-project", {"region": "us-east-1"}
        )

        assert (deployed_files, s3_uri) == (None, None)

    def test_clone_timeout_returns_none_gracefully(self, tmp_path):
        manifest = _Manifest(
            content=_Content(
                git=[_GitContentConfig(repository="repo1", url="https://x/repo.git")]
            )
        )
        git_config = _GitTargetConfig(name="repo1")

        with patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="git clone", timeout=180),
        ):
            deployed_files, s3_uri = _deploy_git_direct(
                git_config, manifest, "test-project", {"region": "us-east-1"}
            )

        assert (deployed_files, s3_uri) == (None, None)
