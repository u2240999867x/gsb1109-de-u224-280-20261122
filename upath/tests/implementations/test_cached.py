import pytest

from upath import UPath
from upath.implementations.cached import SimpleCachePath

from ..cases import BaseTests
from ..utils import OverrideMeta
from ..utils import overrides_base


class TestSimpleCachePath(BaseTests, metaclass=OverrideMeta):
    @pytest.fixture(autouse=True)
    def path(self, local_testdir):
        if not local_testdir.startswith("/"):
            local_testdir = "/" + local_testdir
        path = f"simplecache::memory:{local_testdir}"
        self.path = UPath(path)
        self.prepare_file_system()

    @overrides_base
    def test_is_correct_class(self):
        assert isinstance(self.path, SimpleCachePath)


import hashlib
import json
import os
import zipfile

from fsspec.implementations.zip import ZipFileSystem

from upath._chain import FSSpecChainParser
from upath.implementations.cached import ReleaseDigestMismatch
from upath.implementations.cached import ReleaseUnavailable
from upath.implementations.cached import VerifiedSimpleCacheFileSystem


REPORT_MEMBER = "tables/sales.csv"
CHECKS_MEMBER = "tables/checks.csv"


def _make_report_zip(path, checks_value):
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(REPORT_MEMBER, "source,shared\n")
        zf.writestr(CHECKS_MEMBER, f"checks,{checks_value}\n")
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def _report_uri(archive_path):
    return (
        f"simplecache::zip://{REPORT_MEMBER}::"
        f"simplecache::file://{archive_path}"
    )


def _report_path(archive_path, digest, outer, inner):
    return UPath(
        _report_uri(archive_path),
        cache_options={
            "outer": {"cache_storage": str(outer)},
            "inner": {"cache_storage": str(inner), "sha256": digest},
        },
    )


def test_two_cache_layers_can_be_configured_and_viewed(tmp_path):
    archive = tmp_path / "report.zip"
    digest = _make_report_zip(archive, "A")
    outer = tmp_path / "outer"
    inner = tmp_path / "inner"
    path = _report_path(archive, digest, outer, inner)

    assert path.cache_options["outer"] == {"cache_storage": str(outer)}
    assert path.cache_options["inner"] == {
        "cache_storage": str(inner),
        "sha256": digest,
    }
    assert path.release_sha256 == digest
    assert path.read_text() == "source,shared\n"


def test_chain_roundtrips_with_plain_json_options(tmp_path):
    archive = tmp_path / "report.zip"
    digest = _make_report_zip(archive, "A")
    outer = tmp_path / "outer"
    inner = tmp_path / "inner"
    path = _report_path(archive, digest, outer, inner)
    path.read_text()

    options = json.loads(json.dumps(dict(path.storage_options)))
    reopened = UPath.from_uri(str(path), **options)
    assert reopened.read_text() == "source,shared\n"


def test_parent_derivation_keeps_release_identity(tmp_path):
    base = tmp_path / "archives"
    base.mkdir()
    a_archive = base / "a.zip"
    b_archive = base / "b.zip"
    a_digest = _make_report_zip(a_archive, "A")
    b_digest = _make_report_zip(b_archive, "B")
    outer_a = tmp_path / "outer-a"
    outer_b = tmp_path / "outer-b"
    inner = tmp_path / "inner"
    path_a = _report_path(a_archive, a_digest, outer_a, inner)
    path_b = _report_path(b_archive, b_digest, outer_b, inner)

    assert path_a.read_text() == "source,shared\n"
    assert path_b.read_text() == "source,shared\n"
    assert (path_a.parent / "checks.csv").read_text() == "checks,A\n"
    assert (path_b.parent / "checks.csv").read_text() == "checks,B\n"


def test_digest_mismatch_reports_expected_and_actual(tmp_path):
    archive = tmp_path / "report.zip"
    digest_a = _make_report_zip(archive, "A")
    digest_b = _make_report_zip(archive, "B")
    assert digest_a != digest_b

    outer = tmp_path / "outer"
    inner = tmp_path / "inner"
    path = _report_path(archive, digest_a, outer, inner)
    with pytest.raises(ReleaseDigestMismatch) as excinfo:
        path.read_text()

    error = excinfo.value
    assert isinstance(error, ReleaseUnavailable)
    assert error.expected_sha256 == digest_a
    assert error.actual_sha256 == digest_b
    assert digest_a in str(error)
    assert digest_b in str(error)
    assert os.listdir(inner) == []


def test_verified_digest_is_reused_without_remote_access(tmp_path):
    archive = tmp_path / "report.zip"
    digest = _make_report_zip(archive, "A")
    outer = tmp_path / "outer"
    inner = tmp_path / "inner"
    path = _report_path(archive, digest, outer, inner)
    path.read_text()

    archive.unlink()
    options = json.loads(json.dumps(dict(path.storage_options)))
    options["cache_storage"] = str(tmp_path / "new-outer")
    reopened = UPath.from_uri(str(path), **options)
    assert reopened.read_text() == "source,shared\n"


def test_verified_archive_stays_in_shared_inner_dir(tmp_path):
    archive = tmp_path / "report.zip"
    digest = _make_report_zip(archive, "A")
    outer = tmp_path / "outer"
    inner = tmp_path / "inner"
    path = _report_path(archive, digest, outer, inner)
    path.read_text()
    assert os.path.isfile(inner / digest)
    assert hashlib.sha256(open(inner / digest, "rb").read()).hexdigest() == digest


def test_failed_attempt_does_not_poison_retry(tmp_path):
    archive = tmp_path / "report.zip"
    digest_a = _make_report_zip(archive, "A")
    outer = tmp_path / "outer"
    inner = tmp_path / "inner"
    path = _report_path(archive, digest_a, outer, inner)

    _make_report_zip(archive, "B")
    with pytest.raises(ReleaseDigestMismatch):
        path.read_text()
    assert sorted(os.listdir(inner)) == []

    _make_report_zip(archive, "A")
    retried = UPath.from_uri(
        str(path), **json.loads(json.dumps(dict(path.storage_options)))
    )
    assert retried.read_text() == "source,shared\n"
    assert (retried.parent / "checks.csv").read_text() == "checks,A\n"


def test_legacy_single_cache_storage_is_attributed_to_the_cache(tmp_path):
    pth = UPath(
        "simplecache::memory://a/b",
        simplecache={"cache_storage": str(tmp_path)},
    )
    assert pth.cache_options["outer"] == {"cache_storage": str(tmp_path)}
    assert dict(pth.storage_options)["cache_storage"] == str(tmp_path)


def test_ambiguous_cache_storage_is_reported_before_access(tmp_path):
    uri = (
        "simplecache::zip://tables/sales.csv::"
        "simplecache::ftp://127.0.0.1:2121/daily/report.zip"
    )
    with pytest.raises(ValueError) as excinfo:
        UPath(uri, cache_storage=str(tmp_path))
    message = str(excinfo.value)
    assert "cache_storage" in message
    assert "[0, 2]" in message
    assert "cache_options" in message


def test_explicit_port_and_url_port_are_the_same_source():
    digest = "a" * 64
    explicit = UPath(
        f"simplecache::zip://{REPORT_MEMBER}::"
        "simplecache::ftp://127.0.0.1:2121/daily/report.zip",
        ftp={"port": 2122},
        cache_options={"inner": {"sha256": digest}},
    )
    inline = UPath(
        f"simplecache::zip://{REPORT_MEMBER}::"
        "simplecache::ftp://127.0.0.1:2122/daily/report.zip",
        cache_options={"inner": {"sha256": digest}},
    )
    explicit_ftps = explicit._chain._segments[-1].storage_options
    inline_ftps = inline._chain._segments[-1].storage_options
    assert explicit_ftps["port"] == inline_ftps["port"] == 2122
    assert explicit_ftps["host"] == inline_ftps["host"] == "127.0.0.1"
    # without a port override the default source is still addressable
    default = UPath(
        f"simplecache::zip://{REPORT_MEMBER}::"
        "simplecache::ftp://127.0.0.1:2121/daily/report.zip"
    )
    assert default._chain._segments[-1].storage_options["port"] == 2121


def test_verified_cache_filesystem_is_registered():
    from fsspec.registry import get_filesystem_class

    assert get_filesystem_class("simplecache") is VerifiedSimpleCacheFileSystem


def test_zip_filesystem_digest_detects_replaced_archive(tmp_path):
    import fsspec

    archive = tmp_path / "report.zip"
    digest_a = _make_report_zip(archive, "A")
    inner = tmp_path / "inner"
    open_file = fsspec.open(
        f"simplecache::file://{archive}",
        "rb",
        cache_storage=str(inner),
        sha256=digest_a,
    )
    with open_file as f:
        assert f.read().startswith(b"PK")
    assert (inner / digest_a).is_file()

    digest_b = _make_report_zip(archive, "B")
    assert digest_a != digest_b
    other = fsspec.open(
        f"simplecache::file://{archive}",
        "rb",
        cache_storage=str(tmp_path / "inner-b"),
        sha256=digest_a,
    )
    with pytest.raises(ReleaseDigestMismatch):
        with other as f:
            f.read()
