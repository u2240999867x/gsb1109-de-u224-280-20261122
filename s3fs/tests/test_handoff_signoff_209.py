import asyncio
import json

from botocore.exceptions import ClientError

from s3fs import S3FileSystem
from s3fs.handoff import Handoff, inspect_handoff


CRC_OK = "8a0c9e41"
CRC_BAD = "f4c51377"
SIZE = 12 * 1024**2


def make_fs(call):
    fs = S3FileSystem(asynchronous=True, skip_instance_cache=True)
    fs._call_s3 = call
    return fs


def no_such_upload():
    return ClientError(
        {"Error": {"Code": "NoSuchUpload", "Message": "upload missing"}},
        "CompleteMultipartUpload",
    )


def invalid_part():
    return ClientError(
        {"Error": {"Code": "InvalidPart", "Message": "bad part"}},
        "CompleteMultipartUpload",
    )


def ticket(tmp_path):
    path = tmp_path / "handoff.json"
    handoff = Handoff(
        path="bucket/feeds/daily.tar",
        length=SIZE,
        crc32=CRC_OK,
        primary="host",
        backup="standby",
        save_path=str(path),
    )
    handoff.primary.record_completion_error(TimeoutError("wire disconnected"))
    handoff.primary.record_completion_error(no_such_upload())
    handoff.backup.add_version("V2")
    handoff.save()
    return handoff


def first_page_call():
    async def call(method, *positional, **kwargs):
        if method == "head_object" and kwargs.get("VersionId") == "V2":
            return {"ChecksumCRC32": CRC_OK, "ChecksumType": "FULL_OBJECT"}
        if method == "list_object_versions":
            return {
                "IsTruncated": False,
                "Versions": [
                    {"Key": "feeds/daily.tar", "VersionId": "V3", "Size": SIZE},
                    {"Key": "feeds/daily.tar", "VersionId": "V2", "Size": SIZE},
                ],
            }
        raise AssertionError((method, kwargs))

    return call


def second_page_call(v1_response, v2_timeout=True):
    pages = [
        {
            "IsTruncated": True,
            "NextKeyMarker": "feeds/daily.tar",
            "NextVersionIdMarker": "V2",
            "Versions": [
                {"Key": "feeds/daily.tar", "VersionId": "V3", "Size": SIZE},
                {"Key": "feeds/daily.tar", "VersionId": "V2", "Size": SIZE},
            ],
        },
        {
            "IsTruncated": False,
            "Versions": [
                {"Key": "feeds/daily.tar", "VersionId": "V1", "Size": SIZE},
            ],
        },
    ]

    async def call(method, *positional, **kwargs):
        if method == "head_object" and kwargs.get("VersionId") == "V2":
            if v2_timeout:
                raise TimeoutError("temporary head timeout")
            return {"ChecksumCRC32": CRC_OK, "ChecksumType": "FULL_OBJECT"}
        if method == "head_object" and kwargs.get("VersionId") == "V1":
            if isinstance(v1_response, Exception):
                raise v1_response
            return v1_response
        if method == "list_object_versions":
            return pages.pop(0)
        raise AssertionError((method, kwargs))

    return call


def test_first_review_verifies_backup_but_waits_for_primary(tmp_path):
    handoff = ticket(tmp_path)
    fs = make_fs(first_page_call())

    result = asyncio.run(inspect_handoff(fs, handoff))

    assert result["status"] == "pending"
    assert handoff.backup.verified is True
    assert handoff.backup.actual_crc32 == CRC_OK
    assert handoff.primary.status == "version_not_found"
    assert result["backup_status"] == "verified"
    assert "primary" in result["reason"]
    assert handoff.accepted_version_id is None

    reloaded = Handoff.load(handoff.save_path)
    assert reloaded.backup.version_id == "V2"
    assert reloaded.verifications["V2"]["status"] == "verified"


def test_resumed_review_signs_primary_v1(tmp_path):
    handoff = ticket(tmp_path)
    asyncio.run(inspect_handoff(make_fs(first_page_call()), handoff))
    handoff.save()

    reloaded = Handoff.load(handoff.save_path)
    fs = make_fs(
        second_page_call({"ChecksumCRC32": CRC_OK, "ChecksumType": "FULL_OBJECT"})
    )
    result = asyncio.run(reloaded.check_async(fs))

    assert result["status"] == "signed"
    assert result["version_id"] == "V1"
    assert result["checksum_crc32"] == CRC_OK
    assert result["service_checksum_crc32"] == CRC_OK


def test_resumed_review_records_v1_mismatch_and_signs_cached_v2(tmp_path):
    handoff = ticket(tmp_path)
    asyncio.run(inspect_handoff(make_fs(first_page_call()), handoff))
    handoff.save()

    reloaded = Handoff.load(handoff.save_path)
    fs = make_fs(
        second_page_call({"ChecksumCRC32": CRC_BAD, "ChecksumType": "FULL_OBJECT"})
    )
    result = asyncio.run(inspect_handoff(fs, reloaded))

    assert result["status"] == "signed"
    assert result["version_id"] == "V2"
    primary = reloaded.primary
    assert primary.status == "checksum_mismatch"
    assert primary.expected_crc32 == CRC_OK
    assert primary.actual_crc32 == CRC_BAD
    assert reloaded.failures[-1]["expected_crc32"] == CRC_OK
    assert reloaded.failures[-1]["actual_crc32"] == CRC_BAD
    assert "last_error" in reloaded.verifications["V2"]


def test_composite_only_or_missing_v1_remains_pending(tmp_path):
    handoff = ticket(tmp_path)
    asyncio.run(inspect_handoff(make_fs(first_page_call()), handoff))
    handoff.save()
    reloaded = Handoff.load(handoff.save_path)

    fs = make_fs(
        second_page_call({"ChecksumType": "COMPOSITE", "ChecksumCRC32": "AAEC"})
    )
    result = asyncio.run(inspect_handoff(fs, reloaded))
    assert result["status"] == "pending"
    assert reloaded.primary.status == "composite_only"

    handoff2 = ticket(tmp_path / "missing")
    fs2 = make_fs(first_page_call())
    result2 = asyncio.run(inspect_handoff(fs2, handoff2))
    assert result2["status"] == "pending"


def test_explicit_first_completion_rejection_allows_backup_signoff():
    handoff = Handoff(path="bucket/key", crc32=CRC_OK)
    state = handoff.primary.record_completion_error(invalid_part())
    assert state == "rejected"
    assert handoff.primary.clear_failure is True
    handoff.backup.add_version("V2")

    async def call(method, **kwargs):
        if method == "head_object":
            return {"ChecksumCRC32": CRC_OK, "ChecksumType": "FULL_OBJECT"}
        raise AssertionError(method)

    result = asyncio.run(inspect_handoff(make_fs(call), handoff))
    assert result["status"] == "signed"
    assert result["version_id"] == "V2"
    assert result["confirmed_crc32"] == CRC_OK


def test_bare_serialized_result_resumes(tmp_path):
    handoff = ticket(tmp_path)
    result = asyncio.run(inspect_handoff(make_fs(first_page_call()), handoff))
    path = tmp_path / "bare.json"
    path.write_text(json.dumps(result.to_dict()))

    loaded = Handoff.load(path)
    assert loaded.path == "bucket/feeds/daily.tar"
    assert loaded.backup.version_id == "V2"
    assert loaded.verifications["V2"]["status"] == "verified"
