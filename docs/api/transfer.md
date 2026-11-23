# Batch Transfer :package:

The transfer module moves unpacked ZIP members into a manifest-gated store.
Readers can only open members that are listed in the release manifest, and every
run produces a resumable receipt so an interrupted batch can continue from where
it stopped without mixing contents from a replaced ZIP version.

## Usage Example

```python
from upath import UPath
from upath.transfer import transfer_members, read_published_member

# Two UPath aliases pointing at the same physical ZIP are merged once.
sources = [
    UPath("zip://", fo="/daily/package.zip", target_protocol="memory"),
    UPath("zip://", fo="memory:///daily/package.zip"),
]
manifest = UPath("memory:///release/manifest.json")
staging = UPath("memory:///release/staging")

receipt = transfer_members(sources, manifest, staging_dir=staging)

# Persist the receipt between processes and resume later if needed:
blob = receipt.to_json()
receipt = transfer_members(None, manifest, receipt=blob)

# Only members listed in the manifest are visible to readers.
data = read_published_member(manifest, "A", staging_dir=staging)
```

## Public API

::: upath.transfer
    options:
        heading_level: 3
        show_root_heading: true
        show_root_full_path: false
        members:
        - transfer_members
        - read_published_member
        - publish_manifest
        - zip_version
        - open_zip_version
        - TransferReceipt
        - MemberReceipt
        - Location
        - TransferStatus
        - MemberState
        - ManifestConflict
        - SourceVersionNotFound

---

## See Also :link:

- [UPath](index.md) - Main UPath class documentation
- [Implementations](implementations.md) - Built-in UPath subclasses
