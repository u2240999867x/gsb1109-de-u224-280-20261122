

# Usage Guide

Welcome! This guide will help you get started with Universal Pathlib. If you already know Python's `pathlib`, you'll feel right at home—`UPath` works the same way, but for any filesystem.

## Getting Started

First, import what you need:

```python
from upath import UPath
```

That's it! You're ready to work with files anywhere.

---

## Common Tasks

### How do I work with local files?

UPath behaves just like `pathlib.Path` for local files:

```python
import pathlib
from tempfile import NamedTemporaryFile

# Create a local path
tmp = NamedTemporaryFile()
local_path = UPath(tmp.name)

# It's a regular pathlib object!
assert isinstance(local_path, (pathlib.PosixPath, pathlib.WindowsPath))
```

Want to rely on fsspec for local filesystems too? Pass `protocol="file"`:

```python
local_upath = UPath(local_path, protocol="file")
# Now it uses fsspec backend
assert isinstance(local_upath, UPath)
str(local_upath)
# Result: 'file:///tmp/tmpXXXXXX'
```

!!! warning "`as_uri()` returns an RFC 8089 file URI, not an fsspec urlpath"
    `PosixUPath.as_uri()` and `WindowsUPath.as_uri()` behave exactly like
    `pathlib.Path.as_uri()`: special characters are percent-encoded and UNC paths use
    the URI authority (`file://server/share/file`). fsspec, and therefore
    `UPath("file://...")`, treats everything after `file://` literally as a path.
    So `UPath(local_path.as_uri())` points to a different file if the path contains
    characters like spaces, `#` or `%`, or if it's a UNC path. Use
    `type(local_path).from_uri(uri)` to convert a file URI back to a local path.

### How do I connect to cloud storage or remote filesystems?

Just use the appropriate URI scheme. UPath will automatically connect to the right filesystem:

```python
# Amazon S3
s3path = UPath("s3://my-bucket/data/file.txt")

# Google Cloud Storage
gcs_path = UPath("gs://my-bucket/data.csv")

# Azure Blob Storage
az_path = UPath("az://container/blob.parquet")

# Hugging Face Hub
hf_path = UPath("hf://datasets/username/dataset-name/data.csv")

# GitHub repositories
gh_path = UPath("github://fsspec:universal_pathlib@main/")
```

You can also pass connection options as keyword arguments:

```python
# GitHub with explicit parameters
ghpath = UPath('github:/', org='fsspec', repo='universal_pathlib', sha='main')
```

Or use the `protocol` keyword argument to select the implementation:

```python
# Using protocol kwarg for S3
s3path = UPath('my-bucket/data/file.txt', protocol='s3')

# Using protocol kwarg for Azure with configuration
azpath = UPath(
    'container/blob.parquet',
    protocol='az',
    account_name='myaccount',
    account_key='mykey'
)
```

### How do I read a file?

Same as regular pathlib—just call `read_text()` or `read_bytes()`:

```python
# Read from GitHub
ghpath = UPath('github://fsspec:universal_pathlib@main/')
readme = ghpath / "README.md"
first_line = readme.read_text().splitlines()[0]
print(first_line)
```

For more control, use `open()`:

```python
s3path = UPath("s3://spacenet-dataset/LICENSE.md", anon=True)
with s3path.open("rt", encoding="utf-8") as f:
    print(f.read(22))
```

### How do I list files in a directory?

Use `iterdir()` to iterate through contents:

```python
s3path = UPath("s3://spacenet-dataset", anon=True)

for item in s3path.iterdir():
    if item.is_file():
        print(item)
        break
```

### How do I work with path components?

All the familiar `pathlib` attributes work:

```python
ghpath = UPath('github://fsspec:universal_pathlib@main/')
readme = ghpath / "README.md"

# Get different parts of the path
print(readme.name)     # "README.md"
print(readme.stem)     # "README"
print(readme.suffix)   # ".md"
print(readme.parent)   # The parent directory

# Get the full path as a string
print(str(readme))

# Get just the path without the scheme
print(readme.path)
```

### How do I join paths?

Use the `/` operator, just like pathlib:

```python
base = UPath("s3://my-bucket")
data_dir = base / "data" / "processed"
csv_file = data_dir / "output.csv"
```

### How do I check if a file exists?

Use `exists()`, `is_file()`, or `is_dir()`:

```python
s3path = UPath("s3://spacenet-dataset/LICENSE.md")

if s3path.exists():
    print("File exists!")

if s3path.is_file():
    print("It's a file!")
```

### How does UPath handle S3 prefixes that don't exist?

S3 prefixes aren't traditional POSIX directories. UPath follows `pathlib` conventions—checking a non-existent path returns `False`:

```python
from upath import UPath

# Non-existent bucket returns False, just like pathlib
fake_path = UPath("s3://bucket-that-doesnt-exist/my-dir/")
print(fake_path.is_dir())
# False
```

This matches standard `pathlib` behavior:

```python
import pathlib

# pathlib also returns False for non-existent paths
assert pathlib.Path('/path/that/does/not/exist').is_dir() is False
assert pathlib.Path('/path/that/does/not/exist').exists() is False
```

!!! warning "Authentication Required"
    If the bucket exists but you lack credentials, an authentication exception will be raised:

    ```python
    # This bucket exists but requires authentication
    s3_path = UPath("s3://my-private-bucket/data/")
    s3_path.is_dir()  # Raises authentication exception
    ```

### How do I search for files matching a pattern?

Use `glob()` for pattern matching:

```python
s3path = UPath("s3://spacenet-dataset")

# Find all .TIF files in a specific directory
paris_data = s3path / "AOIs" / "AOI_3_Paris"
for tif_file in paris_data.glob("**.TIF"):
    print(tif_file)
    break
```

!!! note "Glob Syntax"
    The glob syntax follows [fsspec conventions](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.glob), which may differ slightly from pathlib's glob.

---

## Advanced Usage

### How do I pass credentials or configuration?

Pass them as keyword arguments when creating the path:

```python
# S3 with custom endpoint and credentials
s3_path = UPath(
    "s3://my-bucket/file.txt",
    endpoint_url="https://s3.custom-domain.com",
    key="ACCESS_KEY",
    secret="SECRET_KEY"
)

# Anonymous access to public buckets
public_s3 = UPath("s3://spacenet-dataset", anon=True)
```

### How do I access the underlying filesystem?

UPath gives you access to the fsspec filesystem when you need lower-level control:

```python
path = UPath("s3://my-bucket/file.txt")

# Access the filesystem object
fs = path.fs

# Get the path string for use with fsspec
path_str = path.path

# Get storage options
options = path.storage_options

# Use fsspec directly if needed
with fs.open(path_str, 'rb') as f:
    data = f.read()
```

### How do I create a custom UPath for a new filesystem?

Let's say you have an fsspec filesystem with protocol `myproto` and the default
implementation does not correctly work for `.is_dir()`. You can then subclass
`UPath` and register your implementation:

```python
from upath import UPath
from upath.registry import register_implementation

class MyCustomPath(UPath):

    # fix specific methods if the filesystem is a bit non-standard
    def is_dir(self, *, follow_symlinks=True):
        # some special way to check if it's a dir
        is_dir = ...
        return is_dir

# Register for your protocol
register_implementation("myproto", MyCustomPath)

# Now it works!
my_path = UPath("myproto://server/path")
```

!!! note "don't extend the API in your subclass"

    You should not extend the API in your UPath subclass.
    If you want to add new methods please use `upath.extensions.ProxyUPath` as a base class

### How do I add custom methods to UPath?

If you need to add domain-specific methods (like `.download()` or `.upload()`), use `ProxyUPath` instead of subclassing `UPath` directly:

```python
from upath import UPath
from upath.extensions import ProxyUPath

class MyCustomPath(ProxyUPath):
    """A path with extra convenience methods."""

    def download(self, local_path):
        """Download this remote file to a local path."""
        local = UPath(local_path)
        local.write_bytes(self.read_bytes())
        return local

    def get_metadata(self):
        """Get custom metadata for this file."""
        stat = self.stat()
        return {
            'size': stat.st_size,
            'modified': stat.st_mtime,
            'name': self.name,
        }

# Use it like a regular UPath
path = MyCustomPath("s3://my-bucket/data.csv", anon=True)

# Access standard UPath methods
print(path.exists())
print(path.name)

# Use your custom methods
metadata = path.get_metadata()
path.download("/tmp/data.csv")
```

The key difference: `ProxyUPath` wraps a `UPath` instance and delegates to it, while keeping your custom methods separate from the core pathlib API.


---

## Supported Filesystems

Universal Pathlib works with any [fsspec](https://filesystem-spec.readthedocs.io/) filesystem. Here are some popular ones:

| Protocol | Filesystem | Install Package |
|----------|-----------|-----------------|
| `file://` | Local files | _(built-in)_ |
| `s3://` | Amazon S3 | `s3fs` |
| `gs://`, `gcs://` | Google Cloud Storage | `gcsfs` |
| `az://`, `abfs://` | Azure Blob Storage | `adlfs` |
| `hf://` | Hugging Face Hub | `huggingface_hub` |
| `github://` | GitHub | _(built-in)_ |
| `http://`, `https://` | HTTP(S) | _(built-in)_ |
| `ssh://`, `sftp://` | SSH/SFTP | `paramiko` |
| `hdfs://` | Hadoop HDFS | `pyarrow` |
| `smb://` | SMB/Samba | `smbprotocol` |
| `webdav://` | WebDAV | `webdav4` |

You can see all available implementations programmatically:

```python
from fsspec.registry import known_implementations

for name, details in sorted(known_implementations.items()):
    print(f"{name}: {details['class']}")
```

!!! tip "Custom Filesystems"
    Built a custom fsspec filesystem? UPath will work with it automatically!

### How do I configure two `simplecache` layers separately?

A chained path can contain more than one cache layer, for example a cache that
holds downloaded archives and an outer cache that holds extracted members:

```python
from upath import UPath

uri = (
    "simplecache::zip://tables/sales.csv::"
    "simplecache::ftp://127.0.0.1:2121/daily/report.zip"
)
path = UPath(
    uri,
    cache_options={
        "outer": {"cache_storage": "/work/member-cache"},
        "inner": {"cache_storage": "/shared/archive-cache"},
    },
)

path.cache_options["outer"]  # {'cache_storage': '/work/member-cache'}
path.cache_options["inner"]  # {'cache_storage': '/shared/archive-cache'}
```

`cache_options` may be a list ordered from outermost to innermost layer, or a
mapping with `outer` and `inner` keys. The keys `outer_cache_options` and
`inner_cache_options`, as well as `simplecache:outer` and `simplecache:inner`,
are accepted too.

A single unattributed `cache_storage` is still valid for a chain with exactly
one cache layer:

```python
UPath("simplecache::s3://bucket/data.csv", simplecache={"cache_storage": "/tmp/cache"})
```

If a chain contains two cache layers and only one unattributed `cache_storage`
is passed, UPath raises a `ValueError` before any filesystem is created. The
message names both candidate chain positions and the supported ways to
disambiguate.

### How do I pin a release digest?

Pass the publisher's whole-archive SHA-256 as `sha256` (aliases such as
`expected_sha256` and `release_digest` are accepted). The digest namespaces both
cache layers, so two releases of the same archive path never share cached data:

```python
path = UPath(
    uri,
    cache_options={
        "outer": {"cache_storage": "/work/member-cache"},
        "inner": {
            "cache_storage": "/shared/archive-cache",
            "sha256": "e3b0c44298fc1c149afbf4c8996fb924...",
        },
    },
)
path.release_sha256  # the configured digest
```

A download is written to a temporary file, hashed after the complete transfer,
and only published atomically when the digest matches. If the upstream path now
serves a different ZIP, reading a member raises `ReleaseDigestMismatch` (a
subclass of `FileNotFoundError` and `ValueError`) with `expected_sha256`,
`actual_sha256` and the failing path. Partial or mismatching transfers never
become visible to other processes, and a later retry is not blocked by them.
A cached archive that no longer matches its digest is never reused.

### How do I hand a path to another process?

`str(path)` is the full chained URI, and `dict(path.storage_options)` is plain
JSON-compatible data. Another process can reopen the exact same chain with:

```python
import json

handoff = {
    "uri": str(path),
    "storage_options": json.loads(json.dumps(dict(path.storage_options))),
}
reopened = UPath.from_uri(handoff["uri"], **handoff["storage_options"])
```

The consumer may replace the outer `cache_storage` with its own directory while
keeping the shared inner archive directory. Cache hits are served without
constructing or connecting the remote filesystem, so previously fetched release
data remains readable while the origin is offline.
