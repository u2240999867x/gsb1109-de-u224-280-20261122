"""
Integration tests for copy_rows with attachment archiving.

These tests use a stand-in locator class and local SQLite databases; no
Oracle service is required.
"""
# pylint: disable=missing-docstring
import hashlib
import sqlite3
from textwrap import dedent

import pytest

from etlhelper import (
    AttachmentArchive,
    AttachmentColumn,
    copy_rows,
)
from etlhelper.exceptions import (
    ETLHelperAttachmentDigestError,
    ETLHelperAttachmentLengthError,
)

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()

# Bodies used in the scenario.  P is larger than one read chunk so that
# chunked, incremental writing is exercised.
BODY_O = b"old shared body"
BODY_P = bytes(range(256)) * 10240  # ~2.5 MiB
BODY_R = b"contents of R" * 1000
BODY_Q = b"\xee" * len(BODY_P)  # same length as P, different digest

SHA_P = hashlib.sha256(BODY_P).hexdigest()


class FakeLocator:
    """
    Stand-in for an Oracle locator.  Offsets are 1-based; reads may return
    fewer bytes than requested; empty bytes mark the end.
    """

    def __init__(self, data, max_read=7):
        self.data = data
        self.max_read = max_read

    def read(self, offset, amount):
        start = offset - 1
        if start >= len(self.data):
            return b""
        end = min(start + min(amount, self.max_read), len(self.data))
        return self.data[start:end]


CREATE_OBSERVATIONS_SQL = dedent("""
    CREATE TABLE observations
      (
        site text not null,
        obs_no integer not null,
        version integer not null,
        value text,
        attachment1_id integer references attachments (id),
        attachment2_id integer references attachments (id),
        primary key (site, obs_no)
      )
    """).strip()

CREATE_ATTACHMENTS_SQL = dedent("""
    CREATE TABLE attachments
      (
        id integer primary key,
        body blob not null,
        byte_count integer not null,
        sha256 text not null
      )
    """).strip()

CREATE_CORRECTIONS_SQL = dedent("""
    CREATE TABLE corrections
      (
        site text primary key,
        note text
      )
    """).strip()

CREATE_SOURCE_SQL = dedent("""
    CREATE TABLE source_rows
      (
        seq integer primary key,
        site text,
        obs_no integer,
        version integer,
        value text,
        att1_key text,
        att1_size integer,
        att1_sha text,
        att2_key text,
        att2_size integer,
        att2_sha text
      )
    """).strip()

INSERT_SQL = dedent("""
    INSERT INTO observations
      (site, obs_no, version, value, attachment1_id, attachment2_id)
    VALUES
      (:site, :obs_no, :version, :value, :attachment1_id, :attachment2_id)
    ON CONFLICT (site, obs_no) DO UPDATE SET
      version = excluded.version,
      value = excluded.value,
      attachment1_id = excluded.attachment1_id,
      attachment2_id = excluded.attachment2_id
    """).strip()

ATTACHMENTS_SPEC = AttachmentArchive(
    table="attachments",
    columns=(
        AttachmentColumn(locator="att1_locator", target="attachment1_id",
                         size="att1_size", sha256="att1_sha"),
        AttachmentColumn(locator="att2_locator", target="attachment2_id",
                         size="att2_size", sha256="att2_sha"),
    ),
)

# Contents available to the locators stand-in.  "truncated" ends before its
# declared size; "q" claims to be P but has different bytes.
LOCATOR_DATA = {
    "p": BODY_P,
    "r": BODY_R,
    "q": BODY_Q,
    "truncated": BODY_P[:len(BODY_P) // 2],
    "empty": b"",
}


def make_row(seq, site, obs_no, version, value, att1, att2):
    """Build a source row; attachments are (key, size, sha) tuples or None."""
    row = {
        "seq": seq, "site": site, "obs_no": obs_no, "version": version,
        "value": value,
        "att1_key": None, "att1_size": None, "att1_sha": None,
        "att2_key": None, "att2_size": None, "att2_sha": None,
    }
    for prefix, attachment in (("att1", att1), ("att2", att2)):
        if attachment is not None:
            key, size, sha = attachment
            row[f"{prefix}_key"] = key
            row[f"{prefix}_size"] = size
            row[f"{prefix}_sha"] = sha
    return row


def scenario_rows():
    """Rows of the scenario, in stream order."""
    return [
        # S1 new version; both attachments are valid P, no expected digests
        make_row(1, "S1", 1, 1, "new-s1", ("p", len(BODY_P), None),
                 ("p", len(BODY_P), None)),
        # S2 new version; first is valid new R, second claims to be P but
        # contains same-length bytes with a different digest
        make_row(2, "S2", 2, 1, "new-s2", ("r", len(BODY_R), None),
                 ("q", len(BODY_P), SHA_P)),
        # S3 new observation using valid R, no expected digest
        make_row(3, "S3", 3, 1, "new-s3", ("r", len(BODY_R), None), None),
        # S1 update whose attachment ends before its declared size
        make_row(4, "S1", 1, 2, "bad-s1", ("truncated", len(BODY_P), None),
                 ("p", len(BODY_P), None)),
        # S4 with a zero-length attachment and no second attachment
        make_row(5, "S4", 4, 1, "new-s4", ("empty", 0, None), None),
    ]


def inject_locators(rows):
    """Transform that replaces locator keys with locator stand-ins."""
    for row in rows:
        row = dict(row)
        for prefix in ("att1", "att2"):
            key = row.pop(f"{prefix}_key")
            row[f"{prefix}_locator"] = (
                FakeLocator(LOCATOR_DATA[key]) if key is not None else None)
        yield row


@pytest.fixture(scope="function")
def source_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(CREATE_SOURCE_SQL)
    conn.executemany(
        "INSERT INTO source_rows VALUES "
        "(:seq, :site, :obs_no, :version, :value, "
        ":att1_key, :att1_size, :att1_sha, :att2_key, :att2_size, :att2_sha)",
        scenario_rows())
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def dest_conn(tmp_path):
    conn = sqlite3.connect(tmp_path / "dest.db")
    conn.execute(CREATE_ATTACHMENTS_SQL)
    conn.execute(CREATE_OBSERVATIONS_SQL)
    conn.execute(CREATE_CORRECTIONS_SQL)
    # Pre-existing shared body O, referenced by S1 and S2
    conn.execute("INSERT INTO attachments (id, body, byte_count, sha256) "
                 "VALUES (1, ?, ?, ?)",
                 (BODY_O, len(BODY_O), hashlib.sha256(BODY_O).hexdigest()))
    conn.execute("INSERT INTO observations VALUES ('S1', 1, 0, 'old-s1', 1, 1)")
    conn.execute("INSERT INTO observations VALUES ('S2', 2, 0, 'old-s2', 1, 1)")
    conn.commit()
    yield conn
    conn.close()


def run_copy(source_conn, dest_conn, failed_rows):
    return copy_rows(
        "SELECT * FROM source_rows ORDER BY seq", source_conn,
        INSERT_SQL, dest_conn,
        transform=inject_locators,
        on_error=failed_rows.extend,
        attachments=ATTACHMENTS_SPEC)


def fetch_observation(conn, site, obs_no):
    return conn.execute(
        "SELECT site, obs_no, version, value, attachment1_id, attachment2_id "
        "FROM observations WHERE site = ? AND obs_no = ?",
        (site, obs_no)).fetchone()


def fetch_body(conn, attachment_id):
    return conn.execute(
        "SELECT body, byte_count, sha256 FROM attachments WHERE id = ?",
        (attachment_id,)).fetchone()


def test_copy_rows_with_attachments_scenario(source_conn, dest_conn):
    # Caller registers a site correction in the outer transaction
    dest_conn.execute("INSERT INTO corrections VALUES ('S1', 'renamed')")

    failed_rows = []
    processed, failed = run_copy(source_conn, dest_conn, failed_rows)

    assert (processed, failed) == (5, 2)

    # The callback received each failed input exactly once and the failure
    # kinds are distinguishable
    assert len(failed_rows) == 2
    assert isinstance(failed_rows[0].exception, ETLHelperAttachmentDigestError)
    assert failed_rows[0].row["site"] == "S2"
    assert isinstance(failed_rows[1].exception, ETLHelperAttachmentLengthError)
    assert failed_rows[1].row["site"] == "S1"

    # S1 kept the verified new version; both references point to one P body
    _, _, version, value, att1, att2 = fetch_observation(dest_conn, "S1", 1)
    assert (version, value) == (1, "new-s1")
    assert att1 == att2
    assert fetch_body(dest_conn, att1) == (
        BODY_P, len(BODY_P), SHA_P)

    # S2 kept its original version, business column and references to O
    assert fetch_observation(dest_conn, "S2", 2) == ("S2", 2, 0, "old-s2", 1, 1)

    # S3 references the complete R body
    _, _, _, _, att1, att2 = fetch_observation(dest_conn, "S3", 3)
    assert att2 is None
    assert fetch_body(dest_conn, att1) == (
        BODY_R, len(BODY_R), hashlib.sha256(BODY_R).hexdigest())

    # S4's empty attachment is distinct from having no attachment
    _, _, _, _, att1, att2 = fetch_observation(dest_conn, "S4", 4)
    assert att2 is None
    assert fetch_body(dest_conn, att1) == (b"", 0, EMPTY_SHA256)

    # Identical content was stored once: O, P, R and the empty body only
    bodies = dest_conn.execute(
        "SELECT body, byte_count, sha256 FROM attachments").fetchall()
    assert sorted(bodies) == sorted([
        (BODY_O, len(BODY_O), hashlib.sha256(BODY_O).hexdigest()),
        (BODY_P, len(BODY_P), SHA_P),
        (BODY_R, len(BODY_R), hashlib.sha256(BODY_R).hexdigest()),
        (b"", 0, EMPTY_SHA256),
    ])

    # The caller's correction was neither committed nor rolled back
    assert dest_conn.execute("SELECT * FROM corrections").fetchall() == [
        ("S1", "renamed")]

    # Final commit applies versions, bodies and the correction together
    dest_conn.commit()
    assert fetch_observation(dest_conn, "S1", 1)[2] == 1
    assert dest_conn.execute("SELECT * FROM corrections").fetchall() == [
        ("S1", "renamed")]


def test_copy_rows_with_attachments_rollback(source_conn, dest_conn):
    dest_conn.execute("INSERT INTO corrections VALUES ('S1', 'renamed')")

    failed_rows = []
    processed, failed = run_copy(source_conn, dest_conn, failed_rows)
    assert (processed, failed) == (5, 2)

    # Final rollback withdraws versions, bodies and the correction together
    dest_conn.rollback()
    assert fetch_observation(dest_conn, "S1", 1) == ("S1", 1, 0, "old-s1", 1, 1)
    assert fetch_observation(dest_conn, "S2", 2) == ("S2", 2, 0, "old-s2", 1, 1)
    assert fetch_observation(dest_conn, "S3", 3) is None
    assert dest_conn.execute("SELECT * FROM corrections").fetchall() == []
    assert dest_conn.execute("SELECT * FROM attachments").fetchall() == [
        (1, BODY_O, len(BODY_O), hashlib.sha256(BODY_O).hexdigest())]


def test_copy_rows_with_attachments_raises_without_on_error(source_conn,
                                                            dest_conn):
    with pytest.raises(ETLHelperAttachmentDigestError):
        copy_rows(
            "SELECT * FROM source_rows ORDER BY seq", source_conn,
            INSERT_SQL, dest_conn,
            transform=inject_locators,
            attachments=ATTACHMENTS_SPEC)

    # The failed row left nothing behind; rows before it were archived
    assert fetch_observation(dest_conn, "S1", 1)[2] == 1
    assert fetch_observation(dest_conn, "S2", 2) == ("S2", 2, 0, "old-s2", 1, 1)


def test_copy_rows_with_attachments_correct_declared_digest(source_conn,
                                                            dest_conn):
    # A declared digest that matches new content is archived and shared
    conn = source_conn
    conn.execute("DELETE FROM source_rows")
    conn.executemany(
        "INSERT INTO source_rows VALUES "
        "(:seq, :site, :obs_no, :version, :value, "
        ":att1_key, :att1_size, :att1_sha, :att2_key, :att2_size, :att2_sha)",
        [make_row(1, "S5", 5, 1, "new-s5", ("r", len(BODY_R),
                  hashlib.sha256(BODY_R).hexdigest()),
                  ("r", len(BODY_R), hashlib.sha256(BODY_R).hexdigest()))])
    conn.commit()

    failed_rows = []
    processed, failed = copy_rows(
        "SELECT * FROM source_rows ORDER BY seq", source_conn,
        INSERT_SQL, dest_conn,
        transform=inject_locators,
        on_error=failed_rows.extend,
        attachments=ATTACHMENTS_SPEC)

    assert (processed, failed) == (1, 0)
    _, _, _, _, att1, att2 = fetch_observation(dest_conn, "S5", 5)
    assert att1 == att2
    assert fetch_body(dest_conn, att1) == (
        BODY_R, len(BODY_R), hashlib.sha256(BODY_R).hexdigest())
