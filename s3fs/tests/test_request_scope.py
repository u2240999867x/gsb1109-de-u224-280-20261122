import asyncio
import io

import pytest
from botocore.session import get_session

from s3fs import S3FileSystem
from s3fs.utils import ParamKwargsHelper, RequestScope, UnsupportedRequestOption


class Body:
    def __init__(self, data):
        self._stream = io.BytesIO(data)
        self.closed = False

    async def read(self, size=-1):
        return self._stream.read(size)

    def close(self):
        self.closed = True


class FakeClient:
    """Minimal S3 client standing in for an operation-model-compatible one."""

    def __init__(self, data=b"0123456789abcdefghij", owner_support=True, calls=None):
        self.data = data
        self.calls = calls if calls is not None else []
        self.meta = self
        self.service_model = get_session().get_service_model("s3")
        self._owner_support = owner_support

    def operation_model(self, name):
        return self.service_model.operation_model(name)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        async def call(**kwargs):
            self.calls.append((name, kwargs, self))
            if not self._owner_support and "ExpectedBucketOwner" in kwargs:
                # Behave like a client whose model has no such member: the
                # parameter would be rejected before the transfer starts.
                raise AssertionError("ExpectedBucketOwner should not be sent")
            if name == "get_object":
                data = self.data
                if "Range" in kwargs:
                    start, end = kwargs["Range"].removeprefix("bytes=").split("-")
                    data = data[int(start) : int(end) + 1 if end else None]
                return {"Body": Body(data), "ContentLength": len(data)}
            if name == "head_object":
                return {
                    "ContentLength": len(self.data),
                    "ETag": '"abc"',
                    "LastModified": 0,
                }
            if name == "list_objects_v2":
                return {
                    "Contents": [
                        {
                            "Key": kwargs.get("Prefix", ""),
                            "Size": len(self.data),
                        }
                    ],
                    "KeyCount": 1,
                }
            if name == "head_bucket":
                return {}
            return {}

        call.__name__ = name
        call.__self__ = self
        return call


class LimitedMeta:
    """Client metadata whose operation models omit owner verification."""

    def __init__(self, service_model):
        self.service_model = self
        self._service_model = service_model

    def operation_model(self, name):
        return self._service_model.operation_model(name)

    def __getattr__(self, name):
        return getattr(self._service_model, name)


class LimitedServiceModel:
    """Operation models without the owner parameter, like an older client."""

    def __init__(self, service_model):
        self._service_model = service_model

    def operation_model(self, name):
        model = self._service_model.operation_model(name)
        if model.input_shape is not None:
            model.input_shape.members.pop("ExpectedBucketOwner", None)
        return model


class LimitedClient(FakeClient):
    """A client whose operation model lacks ``ExpectedBucketOwner`` support."""

    def __init__(self, *args, **kwargs):
        super().__init__(owner_support=False, *args, **kwargs)
        self.meta = LimitedMeta(LimitedServiceModel(self.service_model))


def make_fs(client):
    """Build a filesystem directly on ``client`` (bypassing session setup)."""
    fs = S3FileSystem(asynchronous=True, skip_instance_cache=True)
    fs._s3 = client
    fs._kwargs_helper = ParamKwargsHelper(client)
    return fs


def test_request_scope_matches_prefixes_and_operation_groups():
    scope = RequestScope(
        {
            "public": {"RequestPayer": None},
            "paid": {
                "operations": {
                    "head": {"RequiredOwner": "111122223333"},
                    "list": {
                        "ExpectedBucketOwner": "111122223333",
                        "RequestPayer": "requester",
                    },
                    "get": {
                        "ExpectedBucketOwner": "111122223333",
                        "RequestPayer": "requester",
                    },
                }
            },
        }
    )
    assert scope.options_for("public/file")[0] == {"RequestPayer": None}
    head, required = scope.options_for("paid/file", "head_object")
    assert head["ExpectedBucketOwner"] == "111122223333"
    assert required == {"ExpectedBucketOwner"}
    list_opts, _ = scope.options_for("paid/", "list_objects_v2")
    assert list_opts["RequestPayer"] == "requester"


def test_cached_listing_revalidated_when_owner_required(tmp_path):
    calls = []
    client = FakeClient(calls=calls)
    fs = make_fs(client)
    fs.dircache["data/nightly"] = [
        {"name": "data/nightly/file", "type": "file", "size": 3, "Key": "data/nightly/file"}
    ]
    fs.request_scope = RequestScope({"data/nightly": {"RequiredOwner": "111"}})

    listing = asyncio.run(fs._lsdir("data/nightly"))
    assert listing
    assert any(name == "list_objects_v2" for name, _, _ in calls)
    assert calls[0][1]["ExpectedBucketOwner"] == "111"


def test_required_owner_unsupported_fails_before_transfer(tmp_path):
    calls = []
    unsupported = LimitedClient(calls=calls)
    fs = make_fs(unsupported)
    fs.request_scope = RequestScope({"paid": {"RequiredOwner": "111"}})

    target = tmp_path / "out"
    with pytest.raises(UnsupportedRequestOption) as excinfo:
        asyncio.run(fs._get_file("paid/file", str(target)))
    assert excinfo.value.method == "get_object"
    assert not any(name == "get_object" for name, _, _ in calls)
    assert not target.exists()


def test_swapping_client_picks_up_new_capabilities(tmp_path):
    calls = []
    unsupported = LimitedClient(calls=calls)
    fs = make_fs(unsupported)
    fs.request_scope = RequestScope({"paid": {"RequiredOwner": "111"}})
    with pytest.raises(UnsupportedRequestOption):
        asyncio.run(fs._get_file("paid/file", str(tmp_path / "first")))

    supported = FakeClient(calls=calls)
    fs._s3 = supported
    fs._kwargs_helper = ParamKwargsHelper(supported)
    asyncio.run(fs._get_file("paid/file", str(tmp_path / "second")))
    assert (tmp_path / "second").read_bytes() == supported.data
    assert calls[-1][1]["ExpectedBucketOwner"] == "111"


def test_local_override_only_affects_this_copy(tmp_path):
    calls = []
    client = FakeClient(calls=calls)
    fs = make_fs(client)
    fs.requester_pays = True
    fs.req_kw = {"RequestPayer": "requester"}
    fs.request_scope = RequestScope({"paid": {"RequestPayer": "requester"}})

    asyncio.run(
        fs._get_file(
            "public/copy", str(tmp_path / "copy"), requester_pays=False
        )
    )
    assert "RequestPayer" not in calls[-1][1]

    asyncio.run(fs._get_file("paid/file", str(tmp_path / "paid")))
    assert calls[-1][1]["RequestPayer"] == "requester"


def test_batch_get_applies_scope_to_discovery_and_download(tmp_path):
    calls = []
    client = FakeClient(calls=calls)
    fs = make_fs(client)
    fs.requester_pays = True
    fs.req_kw = {"RequestPayer": "requester"}
    scope = {"paid": {"RequestPayer": None}}

    async def fake_expand(path, recursive=False, maxdepth=None):
        return ["paid/a", "paid/b"]

    async def not_a_dir(path):
        return False

    fs._expand_path = fake_expand
    fs._isdir = not_a_dir
    asyncio.run(
        fs._get(
            "paid/*",
            str(tmp_path) + "/",
            request_scope=scope,
        )
    )
    names = [name for name, _, _ in calls]
    assert names.count("get_object") == 2
    for name, kwargs, _ in calls:
        if name == "get_object":
            assert "RequestPayer" not in kwargs


def test_head_without_key_falls_back_to_prefix_listing():
    """A HEAD that reports no key is followed by a prefix LIST, with the
    per-operation owner/payer conventions applied to each request."""
    calls = []

    class DirectoryOnlyClient(FakeClient):
        def __getattr__(self, name):
            if name.startswith("_"):
                raise AttributeError(name)

            async def call(**kwargs):
                self.calls.append((name, kwargs, self))
                if name == "head_object":
                    raise FileNotFoundError("no such key")
                if name == "list_objects_v2":
                    return {
                        "Contents": [
                            {"Key": kwargs["Prefix"] + "child", "Size": 1}
                        ],
                        "KeyCount": 1,
                    }
                return {}

            call.__name__ = name
            call.__self__ = self
            return call

    client = DirectoryOnlyClient(calls=calls)
    fs = make_fs(client)
    fs.request_scope = RequestScope(
        {
            "data": {
                "operations": {
                    "head": {"RequiredOwner": "111"},
                    "list": {
                        "ExpectedBucketOwner": "111",
                        "RequestPayer": "requester",
                    },
                }
            }
        }
    )
    info = asyncio.run(fs._info("data/nightly"))
    assert info["type"] == "directory"

    head = next(kwargs for name, kwargs, _ in calls if name == "head_object")
    assert head["ExpectedBucketOwner"] == "111"
    assert "RequestPayer" not in head

    listing = next(kwargs for name, kwargs, _ in calls if name == "list_objects_v2")
    assert listing["Prefix"] == "nightly/"
    assert listing["ExpectedBucketOwner"] == "111"
    assert listing["RequestPayer"] == "requester"


def test_per_file_requester_pays_is_only_for_that_copy():
    """An explicit per-file payer wins for that file and does not leak."""
    fs = S3FileSystem(
        asynchronous=True,
        requester_pays=True,
        skip_instance_cache=True,
    )
    fs._s3 = FakeClient()
    fs._kwargs_helper = ParamKwargsHelper(fs._s3)
    fs.request_scope = RequestScope({"paid": {"RequestPayer": None}})

    paid = fs._open("paid/file", requester_pays=True, size=0)
    public = fs._open("public/file", requester_pays=False, size=0)
    try:
        assert paid._scope_options["RequestPayer"] == "requester"
        assert public._scope_options["RequestPayer"] is None
        # The filesystem-wide default is untouched.
        assert fs.req_kw == {"RequestPayer": "requester"}
    finally:
        paid.buffer = None
        public.buffer = None


def test_missing_payer_support_is_reported_for_the_method():
    """The one common convention missing from the client is RequestPayer on
    bucket-level HEAD; a scope that requires it must fail before that call."""
    calls = []
    fs = make_fs(FakeClient(calls=calls))
    fs.request_scope = RequestScope(
        {
            "data": {
                "operations": {
                    "head": {
                        "RequiredOwner": "111",
                        "RequestPayer": "requester",
                        "required": [
                            "ExpectedBucketOwner",
                            "RequestPayer",
                        ],
                    }
                }
            }
        }
    )
    # head_bucket supports ExpectedBucketOwner but not RequestPayer; with both
    # required the request layer must name the method rather than drop it.
    with pytest.raises(UnsupportedRequestOption) as excinfo:
        asyncio.run(fs._exists("data"))
    assert excinfo.value.method == "head_bucket"
    assert "RequestPayer" in excinfo.value.options
    assert not any(name == "head_bucket" for name, _, _ in calls)


def test_owner_required_info_does_not_accept_cached_listing():
    calls = []

    class DeniedHeadClient(FakeClient):
        def __getattr__(self, name):
            if name.startswith("_"):
                raise AttributeError(name)

            async def call(**kwargs):
                self.calls.append((name, kwargs, self))
                if name == "head_object":
                    raise PermissionError("denied")
                if name == "list_objects_v2":
                    return {"Contents": [], "KeyCount": 0}
                return {}

            call.__name__ = name
            call.__self__ = self
            return call

    client = DeniedHeadClient(calls=calls)
    fs = make_fs(client)
    fs.dircache["data/nightly"] = [
        {"name": "data/nightly/item", "type": "file", "size": 1}
    ]
    fs.request_scope = RequestScope({"data/nightly": {"RequiredOwner": "111"}})

    # The object is already in the listing cache, but a denied HEAD must not be
    # turned into a directory hit; the owner cannot be verified.
    with pytest.raises(PermissionError):
        asyncio.run(fs._info("data/nightly/item"))
    head_calls = [kwargs for name, kwargs, _ in calls if name == "head_object"]
    assert head_calls and head_calls[0]["ExpectedBucketOwner"] == "111"


def test_discovery_keeps_payer_while_local_copy_disables_it(tmp_path):
    """An explicit requester-pays override applies to that copy's GET only;
    prefix discovery still uses the filesystem's payer convention."""
    calls = []

    class ListingClient(FakeClient):
        def __getattr__(self, name):
            if name.startswith("_"):
                raise AttributeError(name)

            async def call(**kwargs):
                self.calls.append((name, kwargs, self))
                if name == "list_objects_v2":
                    return {
                        "Contents": [
                            {"Key": kwargs["Prefix"] + "a", "Size": 2},
                            {"Key": kwargs["Prefix"] + "b", "Size": 2},
                        ],
                        "KeyCount": 2,
                    }
                if name == "get_object":
                    return {"Body": Body(self.data), "ContentLength": len(self.data)}
                return {}

            call.__name__ = name
            call.__self__ = self
            return call

    client = ListingClient(calls=calls)
    fs = make_fs(client)
    fs.requester_pays = True
    fs.req_kw = {"RequestPayer": "requester"}

    async def fake_expand(path, recursive=False, maxdepth=None):
        # Exercise the real listing path so discovery's own request options
        # can be observed.
        await fs._lsdir("paid", refresh=True)
        return ["paid/a", "paid/b"]

    async def not_a_dir(path):
        return False

    fs._expand_path = fake_expand
    fs._isdir = not_a_dir

    asyncio.run(
        fs._get(
            "paid/*",
            str(tmp_path) + "/",
            requester_pays=False,
        )
    )
    discovery = [kw for name, kw, _ in calls if name == "list_objects_v2"]
    transfers = [kw for name, kw, _ in calls if name == "get_object"]
    assert discovery and all(kw["RequestPayer"] == "requester" for kw in discovery)
    assert transfers and all("RequestPayer" not in kw for kw in transfers)


def test_optional_options_are_filtered_by_method_after_client_swap():
    """Optional configuration keeps being filtered per method; swapping the
    client refreshes what the model accepts."""
    calls = []
    fs = make_fs(FakeClient(calls=calls))
    fs.s3_additional_kwargs = {"RequestPayer": "requester"}

    async def run():
        await fs._call_s3("head_bucket", Bucket="b")

    asyncio.run(run())
    name, kwargs, _ = calls[-1]
    assert name == "head_bucket"
    assert "RequestPayer" not in kwargs

    calls.clear()
    fs._s3 = FakeClient(calls=calls)
    fs._kwargs_helper = ParamKwargsHelper(fs._s3)
    asyncio.run(run())
    assert "RequestPayer" not in calls[-1][1]
