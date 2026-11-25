"""Offline tests for coverage-aware directory listing caching.

These tests drive ``S3FileSystem`` with a mocked ``_call_s3`` so that the
number of S3 queries is observable without moto or a network connection.
"""

import asyncio
import copy
from unittest.mock import AsyncMock

import pytest

from s3fs import S3FileSystem


def make_entry(bucket, key, size=1):
    return {
        "Key": key,
        "Size": size,
        "ETag": '"etag"',
        "LastModified": None,
        "StorageClass": "STANDARD",
    }


def listing_page(contents=(), common=(), truncated=False, token=None):
    page = {
        "Contents": list(contents),
        "CommonPrefixes": [{"Prefix": p} for p in common],
        "KeyCount": len(contents) + len(common),
        "IsTruncated": truncated,
    }
    if truncated:
        page["NextContinuationToken"] = token or "next-token"
    return page


@pytest.fixture()
def fs():
    filesystem = S3FileSystem(asynchronous=True, skip_instance_cache=True)
    filesystem.req_kw = {}
    yield filesystem


def run(coro):
    return asyncio.run(coro)


def test_partial_sample_caches_seen_names_but_not_absences(fs):
    # First page: two files plus a common prefix, and the server says more
    # pages exist. Second page holds a third file.
    page1 = listing_page(
        contents=[make_entry("b", "dir/a"), make_entry("b", "dir/b")],
        common=["dir/sub/"],
        truncated=True,
        token="page-2",
    )
    page2 = listing_page(contents=[make_entry("b", "dir/c")])
    empty_stem = listing_page()

    calls = []

    async def call_s3(method, *args, **kwargs):
        calls.append((method, kwargs))
        if method != "list_objects_v2":
            return {"ContentLength": 1}
        prefix = kwargs.get("Prefix", "")
        if prefix == "dir/zzz/":
            page = empty_stem
        elif kwargs.get("ContinuationToken"):
            page = page2
        else:
            page = page1
        # _fill_info mutates the response in place; mimic fresh responses.
        return copy.deepcopy(page)

    fs._call_s3 = AsyncMock(side_effect=call_s3)

    entries = run(fs._lsdir("b/dir", max_items=2))
    names = sorted(e["name"] for e in entries)
    assert names == ["b/dir/a", "b/dir/b", "b/dir/sub"]

    # The sample is cached as positive facts.
    cached = {f["name"] for f in fs.dircache["b/dir"]}
    assert {"b/dir/a", "b/dir/b", "b/dir/sub"} <= cached
    # But it proves no absences.
    assert "b/dir" not in fs.listing_coverage

    # A seen file is answered from the sample without another listing.
    listing_calls_before = len(calls)
    assert run(fs._exists("b/dir/a")) is True
    assert len(calls) == listing_calls_before

    # info() reuses the sampled record for a name the sample did see.
    listing_calls_before = len(calls)
    info = run(fs._info("b/dir/a"))
    assert info["name"] == "b/dir/a"
    assert len(calls) == listing_calls_before

    # The sampled common prefix is known to be a directory.
    assert run(fs._isdir("b/dir/sub")) is True

    # An unseen, later-page file must not be treated as absent: the cache
    # returns no verdict, so _info falls back to a head_object probe.
    listing_calls_before = len(calls)
    assert run(fs._exists("b/dir/c")) is True
    assert calls[listing_calls_before][0] == "head_object"
    assert fs._ls_from_cache("b/dir/c") is None

    # Querying an explicitly empty prefix gives a scoped negative proof.
    assert run(fs._lsdir("b/dir", prefix="zzz/")) == []
    assert fs.listing_coverage.proves_absence("b/dir", "zzz/thing")
    # The proof is limited to the queried stem.
    assert not fs.listing_coverage.proves_absence("b/dir", "c")
    assert not fs.listing_coverage.is_complete("b/dir", "")

    # The negative proof answers exists() without another query.
    listing_calls_before = len(calls)
    assert run(fs._exists("b/dir/zzz/thing")) is False
    assert len(calls) == listing_calls_before

    # Re-querying the same empty stem reuses the proof.
    listing_calls_before = len(calls)
    assert run(fs._lsdir("b/dir", prefix="zzz/")) == []
    assert len(calls) == listing_calls_before

    # A full listing still has to go back to S3 and then sees page two.
    full = run(fs._lsdir("b/dir"))
    assert {e["name"] for e in full} == {
        "b/dir/a",
        "b/dir/b",
        "b/dir/c",
        "b/dir/sub",
    }
    assert fs.listing_coverage.is_complete("b/dir", "")
    # Once complete, an actually absent name raises from the cache.
    with pytest.raises(FileNotFoundError):
        fs._ls_from_cache("b/dir/missing")


def test_completed_pagination_is_reusable(fs):
    page1 = listing_page(
        contents=[make_entry("b", "dir/a")], truncated=True, token="page-2"
    )
    page2 = listing_page(contents=[make_entry("b", "dir/b")])

    async def call_s3(method, *args, **kwargs):
        return copy.deepcopy(
            page2 if kwargs.get("ContinuationToken") else page1
        )

    fs._call_s3 = AsyncMock(side_effect=call_s3)

    first = run(fs._lsdir("b/dir"))
    assert {e["name"] for e in first} == {"b/dir/a", "b/dir/b"}
    assert fs._call_s3.await_count == 2

    # The exhausted listing is fully reusable without any S3 traffic.
    second = run(fs._lsdir("b/dir"))
    assert {e["name"] for e in second} == {"b/dir/a", "b/dir/b"}
    assert fs._call_s3.await_count == 2


def test_stem_maxdepth_find_only_covers_its_own_scope(fs):
    stem_page = listing_page(contents=[make_entry("b", "dir/x1")])
    full_page = listing_page(
        contents=[
            make_entry("b", "dir/x1"),
            make_entry("b", "dir/other"),
        ]
    )

    async def call_s3(method, *args, **kwargs):
        page = stem_page if kwargs.get("Prefix") == "dir/x" else full_page
        return copy.deepcopy(page)

    fs._call_s3 = AsyncMock(side_effect=call_s3)

    result = run(fs._find("b/dir", prefix="x", maxdepth=1))
    assert result == ["b/dir/x1"]

    # The stem query proves only the stem, never the whole directory.
    assert fs.listing_coverage.is_complete("b/dir", "x")
    assert not fs.listing_coverage.is_complete("b/dir", "")
    # A shallow, delimiter-based proof must not satisfy a recursive traversal.
    assert not fs.listing_coverage.is_complete("b/dir", "", recursive=True)

    # A later full traversal cannot reuse the stem result and queries S3.
    calls_before = fs._call_s3.await_count
    full = run(fs._find("b/dir"))
    assert fs._call_s3.await_count > calls_before
    assert set(full) == {"b/dir/x1", "b/dir/other"}


def test_shallow_listing_does_not_negate_nested_names(fs):
    # A delimiter listing (what walk/maxdepth find uses) sees one immediate
    # file and a subdirectory, but nothing below the subdirectory.
    shallow = listing_page(
        contents=[make_entry("b", "dir/f1")], common=["dir/sub/"]
    )
    recursive = listing_page(
        contents=[
            make_entry("b", "dir/f1"),
            make_entry("b", "dir/sub/f2"),
        ]
    )

    async def call_s3(method, *args, **kwargs):
        if kwargs.get("Delimiter") == "/":
            return copy.deepcopy(shallow)
        return copy.deepcopy(recursive)

    fs._call_s3 = AsyncMock(side_effect=call_s3)

    run(fs._lsdir("b/dir"))
    # Immediate names are covered, nested names are not.
    assert fs.listing_coverage.is_complete("b/dir", "")
    assert not fs.listing_coverage.is_complete("b/dir", "", recursive=True)
    # A nested name must not be reported absent from the shallow proof.
    assert fs._ls_from_cache("b/dir/sub/f2") is None
    with pytest.raises(FileNotFoundError):
        # An immediate, truly absent name is still provably missing.
        fs._ls_from_cache("b/dir/absent")

    # A recursive traversal cannot reuse the shallow proof and queries S3.
    calls_before = fs._call_s3.await_count
    out = run(fs._find("b/dir"))
    assert fs._call_s3.await_count > calls_before
    assert set(out) == {"b/dir/f1", "b/dir/sub/f2"}
    assert fs.listing_coverage.is_complete("b/dir", "", recursive=True)
    # After the recursive proof, nested absences are now provable.
    with pytest.raises(FileNotFoundError):
        fs._ls_from_cache("b/dir/sub/missing")


def test_full_recursive_find_marks_recursive_coverage(fs):
    page = listing_page(
        contents=[
            make_entry("b", "tree/f1"),
            make_entry("b", "tree/nested/f2"),
        ]
    )

    async def call_s3(method, *args, **kwargs):
        return copy.deepcopy(page)

    fs._call_s3 = AsyncMock(side_effect=call_s3)

    run(fs._find("b/tree"))
    assert fs.listing_coverage.is_complete("b/tree", "", recursive=True)
    assert fs.listing_coverage.is_complete("b/tree/nested", "", recursive=True)
    # The recursive proof also answers nested-name absences.
    with pytest.raises(FileNotFoundError):
        fs._ls_from_cache("b/tree/nested/missing")


def test_write_invalidates_ancestor_proofs_but_keeps_independent_dirs(fs):
    full_dir = listing_page(contents=[make_entry("b", "keep/f1")])
    other_dir = listing_page(contents=[make_entry("b", "other/g1")])

    async def call_s3(method, *args, **kwargs):
        if method != "list_objects_v2":
            return {}
        if kwargs.get("Prefix", "").startswith("keep/"):
            return copy.deepcopy(full_dir)
        return copy.deepcopy(other_dir)

    fs._call_s3 = AsyncMock(side_effect=call_s3)

    run(fs._lsdir("b/keep"))
    run(fs._lsdir("b/other"))
    assert fs.listing_coverage.is_complete("b/keep", "")
    assert fs.listing_coverage.is_complete("b/other", "")

    # A stale complete listing would otherwise answer isdir negatively.
    assert run(fs._isdir("b/keep/new-sub")) is False

    # Simulate the cache invalidation a write performs: the new object's
    # parent and every ancestor lose both listings and proofs.
    fs.invalidate_cache("b/keep/new-sub")
    assert "b/keep" not in fs.dircache
    assert "b/keep" not in fs.listing_coverage
    assert "b" not in fs.listing_coverage

    # The depth assertion no longer trusts the old proof and re-queries S3.
    calls_before = fs._call_s3.await_count
    run(fs._isdir("b/keep/new-sub"))
    assert fs._call_s3.await_count > calls_before

    # An independent directory keeps its complete, reusable cache.
    assert fs.listing_coverage.is_complete("b/other", "")
    calls_before = fs._call_s3.await_count
    assert run(fs._lsdir("b/other"))
    assert fs._call_s3.await_count == calls_before


def test_refresh_replaces_stale_listing_and_scoped_proof(fs):
    initial = listing_page(
        contents=[make_entry("b", "dir/a"), make_entry("b", "dir/b")]
    )
    after_delete = listing_page(contents=[make_entry("b", "dir/a")])
    pages = iter([copy.deepcopy(initial), copy.deepcopy(after_delete)])

    async def call_s3(method, *args, **kwargs):
        return next(pages)

    fs._call_s3 = AsyncMock(side_effect=call_s3)

    run(fs._lsdir("b/dir"))
    assert {f["name"] for f in fs.dircache["b/dir"]} == {
        "b/dir/a",
        "b/dir/b",
    }

    # A forced refresh must drop the name the server no longer returns.
    refreshed = run(fs._lsdir("b/dir", refresh=True))
    assert [f["name"] for f in refreshed] == ["b/dir/a"]
    assert [f["name"] for f in fs.dircache["b/dir"]] == ["b/dir/a"]
    assert fs.listing_coverage.is_complete("b/dir", "")
    with pytest.raises(FileNotFoundError):
        fs._ls_from_cache("b/dir/b")


def test_empty_stem_does_not_negate_siblings(fs):
    async def call_s3(method, *args, **kwargs):
        return listing_page()

    fs._call_s3 = AsyncMock(side_effect=call_s3)
    run(fs._lsdir("b/dir", prefix="empty/"))
    with pytest.raises(FileNotFoundError):
        fs._ls_from_cache("b/dir/empty/nested")
    # Sibling names remain unknown rather than proven absent.
    assert fs._ls_from_cache("b/dir/sibling") is None
