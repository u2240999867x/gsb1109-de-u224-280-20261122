import array
import os
import pickle
import platform
import uuid

import pytest

import fsspec
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem


def test_mapping_prefix(tmpdir):
    tmpdir = str(tmpdir)
    os.makedirs(os.path.join(tmpdir, "afolder"))
    open(os.path.join(tmpdir, "afile"), "w").write("test")
    open(os.path.join(tmpdir, "afolder", "anotherfile"), "w").write("test2")

    m = fsspec.get_mapper(f"file://{tmpdir}")
    assert "afile" in m
    assert m["afolder/anotherfile"] == b"test2"

    fs = fsspec.filesystem("file")
    m2 = fs.get_mapper(tmpdir)
    m3 = fs.get_mapper(f"file://{tmpdir}")

    assert m == m2 == m3


@pytest.mark.parametrize("protocol", ["file", "memory"])
@pytest.mark.parametrize("key", ["a", "a/nested"])
def test_check_preserves_contents(tmp_path, protocol, key):
    url = f"{protocol}://{tmp_path.as_posix()}/mapping"
    mapper = fsspec.get_mapper(url, create=True)
    contents = {key: b"existing data", "other": b"more data"}
    mapper.update(contents)

    checked = fsspec.get_mapper(url, check=True)

    assert dict(checked) == contents


def test_getitems_errors(tmpdir):
    tmpdir = str(tmpdir)
    os.makedirs(os.path.join(tmpdir, "afolder"))
    open(os.path.join(tmpdir, "afile"), "w").write("test")
    open(os.path.join(tmpdir, "afolder", "anotherfile"), "w").write("test2")
    m = fsspec.get_mapper(f"file://{tmpdir}")
    assert m.getitems(["afile", "bfile"], on_error="omit") == {"afile": b"test"}
    with pytest.raises(KeyError):
        m.getitems(["afile", "bfile"])
    out = m.getitems(["afile", "bfile"], on_error="return")
    assert isinstance(out["bfile"], KeyError)
    m = fsspec.get_mapper(f"file://{tmpdir}", missing_exceptions=())
    assert m.getitems(["afile", "bfile"], on_error="omit") == {"afile": b"test"}
    with pytest.raises(FileNotFoundError):
        m.getitems(["afile", "bfile"])


def test_ops():
    MemoryFileSystem.store.clear()
    m = fsspec.get_mapper("memory://")
    assert not m
    assert list(m) == []

    with pytest.raises(KeyError):
        m["hi"]

    assert m.pop("key", 0) == 0

    m["key0"] = b"data"
    assert list(m) == ["key0"]
    assert m["key0"] == b"data"

    m.clear()

    assert list(m) == []


def test_pickle():
    m = fsspec.get_mapper("memory://")
    assert isinstance(m.fs, MemoryFileSystem)
    m["key"] = b"data"
    m2 = pickle.loads(pickle.dumps(m))
    assert list(m) == list(m2)
    assert m.missing_exceptions == m2.missing_exceptions


def test_keys_view():
    # https://github.com/fsspec/filesystem_spec/issues/186
    m = fsspec.get_mapper("memory://")
    m["key"] = b"data"

    keys = m.keys()
    assert len(keys) == 1
    # check that we don't consume the keys
    assert len(keys) == 1
    m.clear()


def test_multi():
    m = fsspec.get_mapper("memory:///")
    data = {"a": b"data1", "b": b"data2"}
    m.setitems(data)

    assert m.getitems(list(data)) == data
    m.delitems(list(data))
    assert not list(m)


@pytest.mark.parametrize("bulk", [False, True], ids=["setitem", "setitems"])
@pytest.mark.parametrize("value", [array.array("i", [1]), bytearray(b"123")])
def test_setitem_types(bulk, value):
    m = fsspec.get_mapper("memory://")
    if bulk:
        m.setitems({"a": value})
    else:
        m["a"] = value
    assert m["a"] == bytes(value)


def test_setitem_numpy():
    m = fsspec.get_mapper("memory://")
    np = pytest.importorskip("numpy")
    m["c"] = np.array(1, dtype="<i4")  # scalar
    assert m["c"] == b"\x01\x00\x00\x00"
    m["c"] = np.array([1, 2], dtype="<i4")  # array
    assert m["c"] == b"\x01\x00\x00\x00\x02\x00\x00\x00"
    m["c"] = np.array(
        np.datetime64("2000-01-01T23:59:59.999999999"), dtype="<M8[ns]"
    )  # datetime64 scalar
    assert m["c"] == b"\xff\xff\x91\xe3c\x9b#\r"
    m["c"] = np.array(
        [
            np.datetime64("1900-01-01T23:59:59.999999999"),
            np.datetime64("2000-01-01T23:59:59.999999999"),
        ],
        dtype="<M8[ns]",
    )  # datetime64 array
    assert m["c"] == b"\xff\xff}p\xf8fX\xe1\xff\xff\x91\xe3c\x9b#\r"
    m["c"] = np.array(
        np.timedelta64(3155673612345678901, "ns"), dtype="<m8[ns]"
    )  # timedelta64 scalar
    assert m["c"] == b"5\x1c\xf0Rn4\xcb+"
    m["c"] = np.array(
        [
            np.timedelta64(450810516049382700, "ns"),
            np.timedelta64(3155673612345678901, "ns"),
        ],
        dtype="<m8[ns]",
    )  # timedelta64 scalar
    assert m["c"] == b',M"\x9e\xc6\x99A\x065\x1c\xf0Rn4\xcb+'


def test_empty_url():
    m = fsspec.get_mapper()
    assert isinstance(m.fs, LocalFileSystem)


def test_fsmap_access_with_root_prefix(tmp_path):
    # "/a" and "a" are the same for LocalFileSystem
    tmp_path.joinpath("a").write_bytes(b"data")
    m = fsspec.get_mapper(f"file://{tmp_path}")
    assert m["/a"] == m["a"] == b"data"

    # "/a" and "a" differ for MemoryFileSystem
    m = fsspec.get_mapper(f"memory://{uuid.uuid4()}")
    m["/a"] = b"data"

    assert m["/a"] == b"data"
    with pytest.raises(KeyError):
        _ = m["a"]


@pytest.mark.parametrize(
    "key",
    [
        pytest.param(b"k", id="bytes"),
        pytest.param(1234, id="int"),
        pytest.param((1,), id="tuple"),
        pytest.param([""], id="list"),
    ],
)
def test_fsmap_non_str_keys(key):
    m = fsspec.get_mapper()

    # Once the deprecation period passes
    # FSMap.__getitem__ should raise TypeError for non-str keys
    #   with pytest.raises(TypeError):
    #       _ = m[key]

    with pytest.warns(DeprecationWarning):
        with pytest.raises(KeyError):
            _ = m[key]


def test_fsmap_error_on_protocol_keys():
    root = uuid.uuid4()
    m = fsspec.get_mapper(f"memory://{root}", create=True)
    m["a"] = b"data"

    assert m["a"] == b"data"
    with pytest.raises(KeyError):
        _ = m[f"memory://{root}/a"]


def test_fsmap_access_with_suffix(tmp_path):
    tmp_path.joinpath("b").mkdir()
    tmp_path.joinpath("b", "a").write_bytes(b"data")
    if platform.system() == "Windows":
        # on Windows opening a directory will raise PermissionError
        # see: https://bugs.python.org/issue43095
        missing_exceptions = (
            FileNotFoundError,
            IsADirectoryError,
            NotADirectoryError,
            PermissionError,
        )
    else:
        missing_exceptions = None
    m = fsspec.get_mapper(f"file://{tmp_path}", missing_exceptions=missing_exceptions)
    with pytest.raises(KeyError):
        _ = m["b/"]
    assert m["b/a/"] == b"data"


def test_fsmap_dirfs():
    m = fsspec.get_mapper("memory://")

    fs = m.dirfs
    assert isinstance(fs, fsspec.implementations.dirfs.DirFileSystem)
    assert fs.path == m.root


@pytest.mark.parametrize("protocol", ["memory", "file"])
@pytest.mark.parametrize("default", [None, False, 0, b"", []])
def test_pop_missing_key_returns_explicit_default(tmp_path, protocol, default):
    mapper = fsspec.get_mapper(
        f"{protocol}://{tmp_path.as_posix()}/mapping", create=True
    )

    assert mapper.pop("missing", default) is default


@pytest.mark.parametrize("protocol", ["memory", "file"])
def test_pop_without_default_raises_for_missing_key(tmp_path, protocol):
    mapper = fsspec.get_mapper(
        f"{protocol}://{tmp_path.as_posix()}/mapping", create=True
    )

    with pytest.raises(KeyError, match="missing"):
        mapper.pop("missing")


@pytest.mark.parametrize("protocol", ["memory", "file"])
def test_pop_existing_key_returns_and_removes_value(tmp_path, protocol):
    mapper = fsspec.get_mapper(
        f"{protocol}://{tmp_path.as_posix()}/mapping", create=True
    )
    mapper["key"] = b"value"

    assert mapper.pop("key", None) == b"value"
    assert "key" not in mapper


def _local_mapper(tmp_path, root_exists):
    """Mapper over a local root, with parents NOT created on write."""
    fs = LocalFileSystem(auto_mkdir=False, skip_instance_cache=True)
    root = tmp_path / "mapping"
    if root_exists:
        root.mkdir(parents=True)
    return fs.get_mapper(root.as_posix())


SETITEMS_BATCHES = {
    "flat": {"a": b"1", "b": b"2"},
    "nested": {"top/key": b"1"},
    "shared_parent": {"p/one": b"1", "p/two": b"2", "p/three": b""},
    "mixed_depths": {
        "flat": b"0",
        "d1/k": b"1",
        "d1/d2/k": b"2",
        "d1/d2/d3/d4/k": b"4",
        "other/d2/k": b"x",
    },
}


@pytest.mark.parametrize("root_exists", [True, False])
@pytest.mark.parametrize("batch", list(SETITEMS_BATCHES))
def test_setitems_local_creates_parents_like_setitem(tmp_path, root_exists, batch):
    # GH#1041: setitems must write nested keys wherever __setitem__ can
    values = SETITEMS_BATCHES[batch]
    single = _local_mapper(tmp_path / "single", root_exists)
    bulk = _local_mapper(tmp_path / "bulk", root_exists)

    for k, v in values.items():
        single[k] = v
    bulk.setitems(values)

    assert dict(bulk) == dict(single) == values
    assert not bulk.fs.auto_mkdir


@pytest.mark.parametrize("batch", list(SETITEMS_BATCHES))
def test_setitems_memory_nested(batch):
    # control: memory already accepted nested keys before GH#1041
    fs = MemoryFileSystem(skip_instance_cache=True)
    m = fs.get_mapper(f"/setitems-{uuid.uuid4().hex}")
    m.setitems(SETITEMS_BATCHES[batch])
    assert dict(m) == SETITEMS_BATCHES[batch]


def test_setitems_local_overwrite_and_buffer_values(tmp_path):
    m = _local_mapper(tmp_path, root_exists=True)
    m["existing"] = b"old"
    batch = {"existing": b"new", "flat": b"f", "missing/k": b"m"}
    m.setitems(batch)
    assert dict(m) == batch
    m.setitems({"missing/k": bytearray(b"again"), "existing": array.array("b", [7])})
    assert m["missing/k"] == b"again"
    assert m["existing"] == bytes(array.array("b", [7]))


def test_setitems_empty_batch_creates_nothing(tmp_path):
    m = _local_mapper(tmp_path, root_exists=False)
    m.setitems({})
    assert not (tmp_path / "mapping").exists()


def test_setitems_parent_is_file_still_errors(tmp_path):
    m = _local_mapper(tmp_path, root_exists=True)
    m["blocker"] = b"file"
    # The exact error from creating a directory below a file is platform dependent.
    with pytest.raises(OSError):
        m.setitems({"blocker/child": b"x"})
    assert m["blocker"] == b"file"
    assert (tmp_path / "mapping" / "blocker").is_file()


class _RecordingFS(MemoryFileSystem):
    """Memory FS that records bulk writes and directory creation."""

    protocol = "recording"

    def __init__(self, *args, pipe_errors=(), mkdirs_error=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = []
        self.pipe_errors = list(pipe_errors)
        self.mkdirs_error = mkdirs_error

    def pipe(self, path, value=None, **kwargs):
        self.calls.append(("pipe", dict(path)))
        if self.pipe_errors:
            raise self.pipe_errors.pop(0)
        return super().pipe(path, value, **kwargs)

    def makedirs(self, path, exist_ok=False):
        self.calls.append(("makedirs", path, exist_ok))
        if self.mkdirs_error is not None:
            raise self.mkdirs_error
        return super().makedirs(path, exist_ok=exist_ok)


def _recording_mapper(**kwargs):
    fs = _RecordingFS(skip_instance_cache=True, **kwargs)
    return fs.get_mapper(f"/rec-{uuid.uuid4().hex}")


def test_setitems_creates_distinct_parents_before_writing():
    m = _recording_mapper()
    m.setitems({"x/a": b"1", "x/b": b"2", "y/z/c": b"3", "d": b"4"})
    root = m.root
    assert m.fs.calls == [
        ("makedirs", f"{root}/x", True),
        ("makedirs", f"{root}/y/z", True),
        ("makedirs", root, True),
        (
            "pipe",
            {
                f"{root}/x/a": b"1",
                f"{root}/x/b": b"2",
                f"{root}/y/z/c": b"3",
                f"{root}/d": b"4",
            },
        ),
    ]


def test_setitems_empty_batch_keeps_bulk_call():
    m = _recording_mapper()
    m.setitems({})
    assert m.fs.calls == [("pipe", {})]


def test_setitems_mkdirs_error_prevents_writing():
    failure = PermissionError("mk")
    m = _recording_mapper(mkdirs_error=failure)
    with pytest.raises(PermissionError) as exc:
        m.setitems({"x/a": b"1"})
    assert exc.value is failure
    assert m.fs.calls == [("makedirs", f"{m.root}/x", True)]
    assert "x/a" not in m


@pytest.mark.parametrize(
    "error_type", [FileNotFoundError, PermissionError, NotADirectoryError, ValueError]
)
def test_setitems_write_errors_not_retried(error_type):
    failure = error_type("write failed")
    m = _recording_mapper(pipe_errors=[failure])
    with pytest.raises(error_type) as exc:
        m.setitems({"x/a": b"1"})
    assert exc.value is failure
    assert [c[0] for c in m.fs.calls] == ["makedirs", "pipe"]


@pytest.mark.parametrize("root_exists", [True, False])
def test_setitems_local_transaction_creates_parents(tmp_path, root_exists):
    m = _local_mapper(tmp_path, root_exists=root_exists)
    values = {"txn/k": b"v", "txn/other": b"other", "deep/dir/k": b"deep"}
    with m.fs.transaction:
        m.setitems(values)
        assert not any(key in m for key in values)
    assert dict(m) == values
    assert not m.fs._intrans


def test_setitems_local_transaction_discard(tmp_path):
    m = _local_mapper(tmp_path, root_exists=True)
    m["existing"] = b"old"
    with pytest.raises(ValueError, match="abort"):
        with m.fs.transaction:
            m.setitems({"existing": b"new", "txn/k": b"v"})
            raise ValueError("abort")
    assert dict(m) == {"existing": b"old"}
    assert not m.fs._intrans


@pytest.mark.parametrize(
    "operation", ["delete", "clear", "pop", "delete-info-error", "delete-isdir"]
)
@pytest.mark.parametrize("error_type", [PermissionError, OSError])
def test_removal_propagates_backend_failures(monkeypatch, operation, error_type):
    fs = MemoryFileSystem(global_store=False, skip_instance_cache=True)
    mapper = fs.get_mapper("/mapping", create=True)
    mapper["key"] = b"value"
    failure = error_type("backend could not remove the file")

    def fail_remove(*args, **kwargs):
        raise failure

    monkeypatch.setattr(fs, "rm", fail_remove)

    if operation == "delete-info-error":

        def fail_info(*args, **kwargs):
            raise RuntimeError("backend could not check the path")

        monkeypatch.setattr(fs, "info", fail_info)
    elif operation == "delete-isdir":
        monkeypatch.setattr(fs, "isdir", lambda path: True)

    with pytest.raises(error_type) as exc:
        if operation.startswith("delete"):
            del mapper["key"]
        elif operation == "clear":
            mapper.clear()
        else:
            mapper.pop("key")

    assert exc.value is failure
    assert mapper["key"] == b"value"


def test_clear_missing_root():
    fs = MemoryFileSystem(global_store=False, skip_instance_cache=True)
    mapper = fs.get_mapper("/missing")

    mapper.clear()

    assert len(mapper) == 0


def test_delete_missing_key():
    fs = MemoryFileSystem(global_store=False, skip_instance_cache=True)
    mapper = fs.get_mapper("/mapping", create=True)

    with pytest.raises(KeyError) as exc:
        del mapper["missing"]

    assert isinstance(exc.value.__cause__, FileNotFoundError)


@pytest.mark.parametrize("error_type", [PermissionError, OSError, FileExistsError])
def test_clear_propagates_recreation_failure(monkeypatch, error_type):
    fs = MemoryFileSystem(global_store=False, skip_instance_cache=True)
    mapper = fs.get_mapper("/mapping", create=True)
    mapper["key"] = b"value"
    failure = error_type("backend could not recreate the directory")

    def fail_mkdir(*args, **kwargs):
        raise failure

    monkeypatch.setattr(fs, "mkdir", fail_mkdir)

    with pytest.raises(error_type) as exc:
        mapper.clear()

    assert exc.value is failure


@pytest.mark.parametrize("protocol", ["memory", "file"])
def test_delete_directory_key_raises_key_error(tmp_path, protocol):
    mapper = fsspec.get_mapper(
        f"{protocol}://{tmp_path.as_posix()}/mapping", create=True
    )
    mapper["dir/key"] = b"value"

    with pytest.raises(KeyError):
        del mapper["dir"]

    assert mapper["dir/key"] == b"value"


def _encoded_fs_and_root(protocol, tmp_path, name="mapping"):
    if protocol == "file":
        fs = LocalFileSystem(skip_instance_cache=True)
        root = tmp_path.joinpath(name).as_posix()
    else:
        fs = MemoryFileSystem(skip_instance_cache=True)
        root = f"/{name}-{uuid.uuid4().hex}"
    return fs, root


def _write_legacy_store(fs, root, values):
    """Populate a root with the plain files a legacy FSMap would write"""
    legacy = fs.get_mapper(root)
    for key, value in values.items():
        legacy[key] = value
    return legacy


@pytest.mark.parametrize("protocol", ["file", "memory"])
def test_encoded_mode_reads_legacy_store_without_migration(tmp_path, protocol):
    fs, root = _encoded_fs_and_root(protocol, tmp_path)
    _write_legacy_store(
        fs, root, {"site/a": b"summary", "__fsmap_keys__": b"reserved-legacy"}
    )

    m = fs.get_mapper(root, mode="encoded")

    assert m.mode == "encoded"
    assert m["site/a"] == b"summary"
    assert m["__fsmap_keys__"] == b"reserved-legacy"
    assert "site/a" in m
    assert "__fsmap_keys__" in m
    assert "site/a/" not in m
    assert "missing" not in m
    with pytest.raises(KeyError):
        _ = m["missing"]
    assert set(m) == {"site/a", "__fsmap_keys__"}
    assert len(m) == 2


@pytest.mark.parametrize("protocol", ["file", "memory"])
def test_encoded_mode_submetrics_reserved_and_trailing_slash(tmp_path, protocol):
    fs, root = _encoded_fs_and_root(protocol, tmp_path)
    _write_legacy_store(
        fs, root, {"site/a": b"summary", "__fsmap_keys__": b"reserved-legacy"}
    )
    m = fs.get_mapper(root, mode="encoded")

    m.setitems(
        {
            "site/a/minute/00": b"m00",
            "site/a/minute/01": b"m01",
            "__fsmap_keys__": b"reserved-new",
            "site/a/": b"trailing",
        }
    )

    # the summary key keeps its old value and is independent from "site/a/"
    assert m["site/a"] == b"summary"
    assert m["site/a/"] == b"trailing"
    assert m["site/a/minute/00"] == b"m00"
    assert m["site/a/minute/01"] == b"m01"
    assert m["__fsmap_keys__"] == b"reserved-new"
    assert "site/a/" in m

    # delete the key ending in a literal slash and reopen the store
    del m["site/a/"]
    fs2, _ = _encoded_fs_and_root(protocol, tmp_path)
    m2 = fs2.get_mapper(root, mode="encoded")

    keys = set(m2)
    assert {"site/a", "site/a/minute/00", "site/a/minute/01", "__fsmap_keys__"} <= keys
    assert "site/a/" not in keys
    assert "site/a/" not in m2
    with pytest.raises(KeyError):
        _ = m2["site/a/"]
    assert m2["site/a"] == b"summary"
    assert m2["site/a/minute/00"] == b"m00"
    assert m2["site/a/minute/01"] == b"m01"
    assert m2["__fsmap_keys__"] == b"reserved-new"
    # internal objects never surface as keys of the mapping
    assert not any("fsmap_meta" in key or "index" in key or "%" in key for key in keys)


@pytest.mark.parametrize("protocol", ["file", "memory"])
def test_encoded_mode_failed_batch_keeps_previous_value(
    tmp_path, protocol, monkeypatch
):
    from fsspec.mapping import encode_key

    fs, root = _encoded_fs_and_root(protocol, tmp_path)
    _write_legacy_store(fs, root, {"site/a": b"summary-old"})
    m = fs.get_mapper(root, mode="encoded")
    m.setitems({"site/a/minute/00": b"m00"})

    # make the physical write of the summary key fail
    bad_name = encode_key("site/a")
    real_pipe_file = fs.pipe_file

    def flaky_pipe_file(path, value, *args, **kwargs):
        if path.rsplit("/", 1)[-1] == bad_name:
            raise OSError("injected write failure")
        return real_pipe_file(path, value, *args, **kwargs)

    monkeypatch.setattr(fs, "pipe_file", flaky_pipe_file)
    with pytest.raises(OSError, match="injected write failure"):
        m.setitems({"site/a": b"summary-new", "site/b": b"new-key"})
    monkeypatch.undo()

    # the failed item still reads its previous value, the item that did
    # reach the disk is accurately readable, also after reopening
    assert m["site/a"] == b"summary-old"
    assert m["site/b"] == b"new-key"
    fs2, _ = _encoded_fs_and_root(protocol, tmp_path)
    m2 = fs2.get_mapper(root, mode="encoded")
    assert m2["site/a"] == b"summary-old"
    assert m2["site/b"] == b"new-key"

    # now the summary update succeeds; deleting it must also remove the old
    # plain file, so the legacy value cannot reappear after reopening
    m2["site/a"] = b"summary-new"
    assert m2["site/a"] == b"summary-new"
    del m2["site/a"]

    fs3, _ = _encoded_fs_and_root(protocol, tmp_path)
    m3 = fs3.get_mapper(root, mode="encoded")
    assert "site/a" not in m3
    with pytest.raises(KeyError):
        _ = m3["site/a"]
    assert m3["site/a/minute/00"] == b"m00"
    assert m3["site/b"] == b"new-key"


@pytest.mark.parametrize("protocol", ["file", "memory"])
def test_encoded_mode_single_and_bulk_roundtrip(tmp_path, protocol):
    fs, root = _encoded_fs_and_root(protocol, tmp_path)
    m = fs.get_mapper(root, mode="encoded", create=True)

    m["site/a"] = b"one"
    m["site/a/minute/00"] = b"two"
    assert m["site/a"] == b"one"
    assert m["site/a/minute/00"] == b"two"
    assert len(m) == 2

    m.setitems({"k1": b"v1", "k2": b"v2"})
    assert m.getitems(["k1", "k2", "site/a"]) == {
        "k1": b"v1",
        "k2": b"v2",
        "site/a": b"one",
    }
    out = m.getitems(["k1", "missing"], on_error="return")
    assert out["k1"] == b"v1"
    assert isinstance(out["missing"], KeyError)
    assert m.getitems(["k1", "missing"], on_error="omit") == {"k1": b"v1"}
    with pytest.raises(KeyError):
        m.getitems(["k1", "missing"])

    m.delitems(["k1", "k2"])
    assert "k1" not in m
    assert set(m) == {"site/a", "site/a/minute/00"}

    # reopening the same directory gives the same view
    fs2, _ = _encoded_fs_and_root(protocol, tmp_path)
    m2 = fs2.get_mapper(root, mode="encoded")
    assert dict(m2) == {"site/a": b"one", "site/a/minute/00": b"two"}

    m2.clear()
    assert list(m2) == []
    fs3, _ = _encoded_fs_and_root(protocol, tmp_path)
    assert list(fs3.get_mapper(root, mode="encoded")) == []


@pytest.mark.parametrize("protocol", ["file", "memory"])
def test_encoded_mode_metadata_never_a_business_key(tmp_path, protocol):
    fs, root = _encoded_fs_and_root(protocol, tmp_path)
    m = fs.get_mapper(root, mode="encoded", create=True)
    m["__fsmap_keys__"] = b"business"
    m["__fsmap_meta__"] = b"looks-internal"
    m["a/b/c"] = b"deep"

    fs2, _ = _encoded_fs_and_root(protocol, tmp_path)
    m2 = fs2.get_mapper(root, mode="encoded")
    keys = set(m2)
    assert keys == {"__fsmap_keys__", "__fsmap_meta__", "a/b/c"}
    assert m2["__fsmap_keys__"] == b"business"
    assert m2["__fsmap_meta__"] == b"looks-internal"
    # the physical metadata directory does not leak into the mapping
    assert not any(key.startswith("__fsmap_meta__/") for key in keys)
    assert "index.json" not in keys


def test_encoded_mode_pickle_roundtrip():
    fs = MemoryFileSystem(skip_instance_cache=True)
    root = f"/pickle-{uuid.uuid4().hex}"
    m = fs.get_mapper(root, mode="encoded", create=True)
    m["a/b"] = b"data"

    m2 = pickle.loads(pickle.dumps(m))
    assert m2.mode == "encoded"
    assert m2["a/b"] == b"data"
    assert list(m2) == ["a/b"]


def test_fsmap_invalid_mode():
    fs = MemoryFileSystem(skip_instance_cache=True)
    with pytest.raises(ValueError, match="mode"):
        fs.get_mapper("/mapping", mode="bogus")
    with pytest.raises(ValueError, match="mode"):
        fsspec.get_mapper("memory://mapping", mode="bogus")


def test_legacy_mode_is_the_default(tmp_path):
    fs = LocalFileSystem(skip_instance_cache=True)
    m = fs.get_mapper(tmp_path.as_posix())
    assert m.mode == "legacy"
    m["a"] = b"data"
    assert (tmp_path / "a").read_bytes() == b"data"
