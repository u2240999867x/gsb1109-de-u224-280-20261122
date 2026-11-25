import errno
import fnmatch
import logging
from contextlib import contextmanager, AsyncExitStack
from botocore.exceptions import ClientError


logger = logging.getLogger("s3fs")


class UnsupportedRequestOption(ValueError, TypeError):
    """A required S3 request option is not supported by the chosen client.

    This is raised instead of silently dropping the option when a scope rule
    marks the option as required (for example ``ExpectedBucketOwner`` for a
    bucket-owner verification).
    """

    def __init__(self, message, method=None, options=()):
        super().__init__(message)
        self.method = method
        self.options = tuple(options)


@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass


class S3BucketRegionCache:
    # See https://github.com/aio-libs/aiobotocore/issues/866
    # for details.

    def __init__(self, session, **client_kwargs):
        self._session = session
        self._stack = AsyncExitStack()
        self._client = None
        self._client_kwargs = client_kwargs
        self._buckets = {}
        self._regions = {}

    async def get_bucket_client(self, bucket_name=None):
        if bucket_name in self._buckets:
            return self._buckets[bucket_name]

        general_client = await self.get_client()
        if bucket_name is None:
            return general_client

        try:
            response = await general_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            logger.debug("RC: HEAD_BUCKET call for %r has failed", bucket_name)
            response = e.response

        region = (
            response["ResponseMetadata"]
            .get("HTTPHeaders", {})
            .get("x-amz-bucket-region")
        )

        if not region:
            logger.debug(
                "RC: No region in HEAD_BUCKET call response for %r, returning the general client",
                bucket_name,
            )
            return general_client

        if region not in self._regions:
            logger.debug(
                "RC: Creating a new regional client for %r on the region %r",
                bucket_name,
                region,
            )
            self._regions[region] = await self._stack.enter_async_context(
                self._session.create_client(
                    "s3", region_name=region, **self._client_kwargs
                )
            )

        client = self._buckets[bucket_name] = self._regions[region]
        return client

    async def get_client(self):
        if not self._client:
            self._client = await self._stack.enter_async_context(
                self._session.create_client("s3", **self._client_kwargs)
            )
        return self._client

    async def clear(self):
        logger.debug("RC: discarding all clients")
        self._buckets.clear()
        self._regions.clear()
        self._client = None
        await self._stack.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_args):
        await self.clear()


class FileExpired(IOError):
    """
    Is raised, when the file content has been changed from a different process after
    opening the file. Reading the file would lead to invalid or inconsistent output.
    This can also be triggered by outdated file-information inside the directory cache.
    In this case ``S3FileSystem.invalidate_cache`` can be used to force an update of
    the file-information when opening the file.
    """

    def __init__(self, filename: str, e_tag: str):
        super().__init__(
            errno.EBUSY,
            "The remote file corresponding to filename %s and Etag %s no longer exists."
            % (filename, e_tag),
        )


def title_case(string):
    """
    TitleCases a given string.

    Parameters
    ----------
    string : underscore separated string
    """
    return "".join(x.capitalize() for x in string.split("_"))


def _normalise_scope_path(path):
    """Bucket/key form of a path, suitable for prefix matching."""
    path = str(path)
    for prefix in ("s3://", "s3a://"):
        if path.startswith(prefix):
            path = path[len(prefix) :]
    return path.strip("/")


def _apply_shortcuts(options):
    """Translate convenience markers into real S3 request options."""
    options = dict(options)
    if "RequiredOwner" in options:
        owner = options.pop("RequiredOwner")
        if isinstance(owner, str) and owner:
            options["ExpectedBucketOwner"] = owner
        existing = options.get("required")
        if existing is False:
            # Keep the explicit opt-out; the owner check stays optional.
            pass
        elif existing is not True:
            names = set(existing or ())
            names.add("ExpectedBucketOwner")
            options["required"] = names
    return options


def _split_required(options):
    """Pop the ``required`` marker, returning ``(options, required, opted_out)``.

    ``required`` may be ``True`` (all options in the mapping), a sequence of
    option names, or falsy.  A falsy ``required`` leaves options optional, so
    they are still filtered against the per-method model but never fail.
    ``opted_out`` records an explicit ``required: False`` so that an owner
    constraint can stay optional instead of being promoted to required.
    """
    options = dict(options)
    flag = options.pop("required", None)
    if "require" in options:
        require = options.pop("require")
        if flag is None:
            flag = require
    opted_out = flag is False
    if flag is True:
        required = set(options)
    elif flag in (False, None):
        required = set()
    elif isinstance(flag, str):
        if flag in options:
            required = {flag}
        else:
            required = set()
    else:
        required = set(flag)
    return options, required, opted_out


class RequestScope:
    """Path-scoped S3 request options.

    A scope maps bucket/key prefixes to the S3 request options that should be
    sent for matching paths.  It is deliberately simple and serialisable::

        scope = RequestScope(
            {
                "public/": {"RequestPayer": None},
                "paid/": {
                    "Prefix": "paid/",
                    "RequiredOwner": "123456789012",
                    "RequestPayer": "requester",
                },
            }
        )

    Several forms are accepted:

    * a mapping of prefix to option mapping,
    * a list of ``(prefix, options)`` pairs or dictionaries containing one of
      ``prefix``/``path``/``bucket`` alongside the options,
    * a callable ``path -> options``,
    * an existing :class:`RequestScope`.

    Option mappings may also carry ``required`` to say which of the options
    must be supported by the operation actually being called.  A ``None``
    value disables an inherited option for that prefix (the option is dropped
    rather than sent as ``None``).

    For sources whose hand-off list uses different rules for metadata,
    listing, and transfers, nest options under ``operations``::

        RequestScope(
            {
                "paid/": {
                    "operations": {
                        "head": {"RequiredOwner": "123456789012"},
                        "list": {
                            "ExpectedBucketOwner": "123456789012",
                            "RequestPayer": "requester",
                        },
                        "get": {
                            "ExpectedBucketOwner": "123456789012",
                            "RequestPayer": "requester",
                        },
                    }
                }
            }
        )

    The groups ``head``/``list``/``get`` expand to the operations that the
    filesystem actually issues for discovery, directory detection, and data
    transfer; an explicit operation name is also accepted.
    """

    _PREFIX_KEYS = (
        "prefix",
        "path",
        "bucket",
        "scope",
        "source",
        "rules",
    )
    _OPTION_KEYS = ("options", "request", "request_options")
    _OPERATION_KEYS = ("operations", "by_operation", "per_operation")
    _MARKER_KEYS = ("required", "require", "RequiredOwner")
    _OPERATION_GROUPS = {
        "head": ("head_object", "head_bucket", "get_bucket_location"),
        "list": ("list_objects_v2", "list_object_versions", "list_buckets"),
        "get": ("get_object",),
    }

    def __init__(self, rules=None, default=None):
        self._rules = []
        self._default = dict(default or {})
        self._catch_all = []
        if rules is not None:
            self._add(rules)
        self._rules.sort(key=lambda item: len(item[0]))

    # -- construction -----------------------------------------------------
    def _add(self, rules):
        if isinstance(rules, RequestScope):
            self._default.update(rules._default)
            self._rules.extend(rules._rules)
            self._catch_all.extend(rules._catch_all)
        elif callable(rules) and not isinstance(rules, dict):
            self._catch_all.append(rules)
        elif isinstance(rules, dict):
            prefix = self._rule_prefix(rules)
            if prefix is not None:
                self._add_rule(
                    prefix,
                    {k: v for k, v in rules.items() if k not in self._PREFIX_KEYS},
                )
            elif self._is_option_mapping(rules):
                self._default.update(rules)
            else:
                for rule_prefix, options in rules.items():
                    self._add_rule(rule_prefix, options)
        elif isinstance(rules, (list, tuple)):
            for entry in rules:
                if callable(entry) and not isinstance(entry, dict):
                    self._catch_all.append(entry)
                elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                    self._add_rule(entry[0], entry[1])
                elif isinstance(entry, dict):
                    prefix = self._rule_prefix(entry)
                    if prefix is None:
                        self._default.update(entry)
                    else:
                        self._add_rule(
                            prefix,
                            {k: v for k, v in entry.items() if k not in self._PREFIX_KEYS},
                        )
                else:
                    raise TypeError(f"Unsupported scope rule: {entry!r}")
        else:
            raise TypeError(f"Unsupported scope rules: {rules!r}")

    @classmethod
    def _rule_prefix(cls, mapping):
        """Return the prefix if ``mapping`` is a single rule descriptor.

        A descriptor has one of ``prefix``/``path``/``bucket`` set to a
        string; a plain option mapping such as ``{"RequestPayer": None}``
        does not.
        """
        for key in cls._PREFIX_KEYS:
            value = mapping.get(key)
            if isinstance(value, str):
                return value
        return None

    @classmethod
    def _is_option_mapping(cls, mapping):
        """Heuristic: a flat dict of S3 options rather than prefix rules.

        Prefix rules normally have bucket/key-like string keys.  An S3 option
        mapping is recognised by its marker keys or by an upper-case option
        name; a key that is not one of the reserved names and not an option
        (for example ``"paid"`` or ``"bucket/key"``) marks a rules mapping.
        """
        keys = [key for key in mapping if isinstance(key, str)]
        if not keys:
            return False
        reserved = set(cls._PREFIX_KEYS) | set(cls._OPTION_KEYS)
        reserved |= set(cls._OPERATION_KEYS) | set(cls._MARKER_KEYS)
        for key in keys:
            if key in reserved:
                return True
            if key in cls._OPERATION_GROUPS:
                return True
            if key[:1].isupper():
                return True
            return False
        return True

    def _add_rule(self, prefix, options):
        if options is None:
            options = {}
        if isinstance(options, RequestScope):
            for sub_prefix, sub_options in options._rules:
                self._rules.append((f"{prefix}{sub_prefix}", sub_options))
            return
        if not isinstance(options, dict):
            raise TypeError(f"Unsupported scope options for {prefix!r}: {options!r}")
        options = dict(options)
        per_operation = None
        for key in self._OPERATION_KEYS:
            per_operation = options.pop(key, None)
            if per_operation is not None:
                break
        for key in self._OPTION_KEYS:
            nested = options.pop(key, None)
            if nested is not None:
                if not isinstance(nested, dict):
                    raise TypeError(
                        f"Scope {key} for {prefix!r} must be a mapping"
                    )
                options = {**nested, **options}
        options = _apply_shortcuts(options)
        prefix = _normalise_scope_path(prefix)
        self._rules.append((prefix, options))
        if per_operation:
            if not isinstance(per_operation, dict):
                raise TypeError(
                    f"Scope operations for {prefix!r} must be a mapping"
                )
            for group, group_options in per_operation.items():
                methods = self._operations_for(group)
                if not isinstance(group_options, dict):
                    raise TypeError(
                        f"Scope operations[{group!r}] for {prefix!r} must be a mapping"
                    )
                group_options = _apply_shortcuts(group_options)
                for method in methods:
                    self._rules.append(
                        (prefix, {**group_options, "_methods": (method,)})
                    )

    # -- lookup -----------------------------------------------------------
    @classmethod
    def _operations_for(cls, group):
        if isinstance(group, (list, tuple, set, frozenset)):
            return tuple(group)
        if group in cls._OPERATION_GROUPS:
            return cls._OPERATION_GROUPS[group]
        return (group,)

    @staticmethod
    def _matches(prefix, path):
        if not prefix:
            return True
        if any(char in prefix for char in "*?["):
            return fnmatch.fnmatchcase(path, prefix)
        # A prefix rule covers the path itself and everything below it.  A
        # trailing slash is optional, so ``"data/nightly/"`` and
        # ``"data/nightly"`` behave the same.
        return path == prefix or path.startswith(prefix.rstrip("/") + "/")

    def options_for(self, path, operation=None):
        """Return ``(options, required)`` for ``path``.

        ``options`` may contain ``None`` values; those mean "this narrower
        scope turns the option off" and are dropped by the caller only after
        broader defaults have been merged.  ``required`` is the set of option
        names that must reach the wire for this path.

        ``operation`` (such as ``"head_object"``, ``"list_objects_v2"`` or
        ``"get_object"``) selects operation-specific conventions declared with
        an ``operations`` mapping.
        """
        path = _normalise_scope_path(path)
        merged = dict(self._default)
        for rule in self._catch_all:
            extra = rule(path) or {}
            if not isinstance(extra, dict):
                raise TypeError(
                    f"Scope rule callable must return a mapping, got {extra!r}"
                )
            merged.update(extra)
        for prefix, rule in self._rules:
            if not self._matches(prefix, path):
                continue
            methods = rule.get("_methods")
            if methods is not None and operation is not None and operation not in methods:
                continue
            merged.update({k: v for k, v in rule.items() if k != "_methods"})
        merged = _apply_shortcuts(merged)
        options, required, opted_out = _split_required(merged)
        # An owner constraint was supplied for this path, so it is only
        # meaningful if it reaches the request; treat it as required unless
        # the rule opted out with ``required=False``.
        if options.get("ExpectedBucketOwner") is not None and not opted_out:
            required.add("ExpectedBucketOwner")
        required = {name for name in required if options.get(name) is not None}
        return options, required


class ParamKwargsHelper:
    """
    Utility class to help extract the subset of keys that an s3 method is
    actually using

    Parameters
    ----------
    s3 : boto S3FileSystem

    The cache is per client and per operation, so that replacing the
    underlying client (for example with a compatible implementation whose
    operation model exposes different members) is picked up immediately.
    """

    def __init__(self, s3):
        self.s3 = s3
        self._kwarg_cache = {}

    @staticmethod
    def _client_for(s3, method):
        if method is not None and not isinstance(method, str):
            return getattr(method, "__self__", None) or s3
        return s3

    @staticmethod
    def _method_name(method):
        return method if isinstance(method, str) else method.__name__

    def _get_valid_keys(self, method):
        client = self._client_for(self.s3, method)
        method_name = self._method_name(method)
        entry = self._kwarg_cache.get(method_name)
        # Keep the client identity alongside the model, so a replaced client
        # (even one reusing the same id) is never served a stale model.
        if entry is None or entry[0] is not client:
            model = client.meta.service_model.operation_model(
                title_case(method_name)
            )
            valid_keys = (
                set(model.input_shape.members.keys())
                if model.input_shape is not None
                else set()
            )
            entry = (client, valid_keys)
            self._kwarg_cache[method_name] = entry
        return entry[1]

    def valid_keys(self, method):
        return set(self._get_valid_keys(method))

    def supports(self, method, option):
        return option in self._get_valid_keys(method)

    def require(self, method, options):
        missing = sorted(opt for opt in options if not self.supports(method, opt))
        if missing:
            method_name = self._method_name(method)
            raise UnsupportedRequestOption(
                f"{method_name} does not support required request option "
                + ", ".join(missing),
                method=method_name,
                options=missing,
            )

    def filter_dict(self, method, d):
        valid_keys = self._get_valid_keys(method)
        if isinstance(d, SSEParams):
            d = d.to_kwargs()
        return {k: v for k, v in d.items() if k in valid_keys}


class SSEParams:
    def __init__(
        self,
        server_side_encryption=None,
        sse_customer_algorithm=None,
        sse_customer_key=None,
        sse_kms_key_id=None,
    ):
        self.ServerSideEncryption = server_side_encryption
        self.SSECustomerAlgorithm = sse_customer_algorithm
        self.SSECustomerKey = sse_customer_key
        self.SSEKMSKeyId = sse_kms_key_id

    def to_kwargs(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


def _get_brange(size, block):
    """
    Chunk up a file into zero-based byte ranges

    Parameters
    ----------
    size : file size
    block : block size
    """
    for offset in range(0, size, block):
        yield offset, min(offset + block - 1, size - 1)
