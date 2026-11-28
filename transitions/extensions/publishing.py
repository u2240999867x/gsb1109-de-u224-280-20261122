"""Recoverable publication commands for ``transitions``.

The module adds an opt-in command path to a machine trigger.  A publication
command selects candidates, stores its trial results and selected manifest in
SQLite before anything is sent, and only advances the locally confirmed
receipt after the warehouse acknowledges the manifest.

Ordinary trigger calls are delegated to ``Event.trigger`` unchanged.
"""

from __future__ import absolute_import

import hashlib
import itertools
import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from functools import partial

from ..core import Event, EventData, Machine, listify


PENDING_STATES = ("prepared", "sent")


class PublishingError(Exception):
    """Base class for publication related errors."""


class CommandConflict(PublishingError):
    """Raised when a command id is reused with different content."""


class PendingCommandError(PublishingError):
    """Raised when a new command is blocked by an unconfirmed command.

    Attributes:
        pending_command_id (str): The command which has not been confirmed yet.
        confirmed_version (int): The version that is still locally confirmed.
    """

    def __init__(self, pending_command_id, confirmed_version):
        self.pending_command_id = pending_command_id
        self.confirmed_version = confirmed_version
        super(PendingCommandError, self).__init__(
            "Command '%s' has not been confirmed yet (confirmed version: %s)"
            % (pending_command_id, confirmed_version))


class NoAcceptedCandidate(PublishingError):
    """Raised when all candidates of a command were rejected."""


class StaleReceiptError(PublishingError):
    """Raised when a manifest is not building on the current receipt."""


class VersionConflict(PublishingError):
    """Raised when a partition version already accepts another manifest."""


class ReceiptMismatch(PublishingError):
    """Raised when a receipt does not extend the locally confirmed chain."""


class ResponseLost(PublishingError):
    """Raised by a transport when a response cannot be delivered.

    The warehouse may already have accepted the manifest.  The same command
    must be sent again to obtain the original receipt.
    """

    def __init__(self, message="The warehouse response was lost", response=None):
        self.response = response
        super(ResponseLost, self).__init__(message)


class ResponseNotReady(PublishingError):
    """Raised when a deferred warehouse response has not been released yet."""


def _canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=repr)


def _digest(value):
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _freeze(value):
    if isinstance(value, dict):
        return {key: _freeze(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_freeze(item) for item in value]
    return value


def _coerce_manifest(manifest):
    if isinstance(manifest, Manifest):
        return manifest
    if isinstance(manifest, dict):
        return Manifest.from_dict(manifest)
    raise TypeError("Expected a Manifest instance or dictionary, got %r" % (type(manifest),))


def _coerce_receipt(receipt):
    if receipt is None or isinstance(receipt, Receipt):
        return receipt
    if isinstance(receipt, dict):
        return Receipt.from_dict(receipt)
    raise TypeError("Expected a Receipt instance or dictionary, got %r" % (type(receipt),))


@dataclass(frozen=True)
class Manifest:
    """An immutable manifest selected by a publication command."""

    partition: str
    version: int
    command_id: str
    candidates: tuple = ()
    payload: object = None
    metadata: object = None
    digest: str = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "version", int(self.version))
        object.__setattr__(self, "candidates", tuple(self.candidates or ()))
        object.__setattr__(self, "digest", _digest(self._content()))

    def _content(self):
        return {
            "partition": self.partition,
            "version": self.version,
            "candidates": list(self.candidates),
            "payload": self.payload,
            "metadata": self.metadata,
        }

    @classmethod
    def create(cls, partition, version, command_id, candidates=None, payload=None, metadata=None):
        """Create a manifest from the final, accepted candidates."""
        return cls(partition=partition, version=version, command_id=command_id,
                   candidates=tuple(candidates or ()), payload=payload, metadata=metadata)

    def as_dict(self):
        """Return a JSON serializable representation including the digest."""
        result = self._content()
        result["command_id"] = self.command_id
        result["digest"] = self.digest
        return result

    to_dict = as_dict

    @classmethod
    def from_dict(cls, value):
        """Recreate a manifest from :meth:`as_dict` output."""
        manifest = cls(partition=value["partition"], version=value["version"],
                       command_id=value["command_id"], candidates=tuple(value.get("candidates") or ()),
                       payload=value.get("payload"), metadata=value.get("metadata"))
        expected = value.get("digest")
        if expected is not None and expected != manifest.digest:
            raise PublishingError("Manifest digest does not match its content")
        return manifest


@dataclass(frozen=True)
class Receipt:
    """The warehouse acknowledgement for one accepted manifest."""

    partition: str
    version: int
    command_id: object
    manifest_digest: str
    previous_digest: object
    token: str

    @classmethod
    def create(cls, manifest, previous_receipt=None):
        """Create the acknowledgement for ``manifest`` after ``previous_receipt``."""
        previous = _coerce_receipt(previous_receipt)
        fields = {
            "partition": manifest.partition,
            "version": manifest.version,
            "command_id": manifest.command_id,
            "manifest_digest": manifest.digest,
            "previous_digest": previous.token if previous is not None else None,
        }
        return cls(token=_digest(fields), **fields)

    def as_dict(self):
        """Return a JSON serializable representation."""
        return {
            "partition": self.partition,
            "version": self.version,
            "command_id": self.command_id,
            "manifest_digest": self.manifest_digest,
            "previous_digest": self.previous_digest,
            "token": self.token,
        }

    to_dict = as_dict

    @classmethod
    def from_dict(cls, value):
        """Recreate a receipt from :meth:`as_dict` output."""
        return cls(partition=value["partition"], version=value["version"],
                   command_id=value.get("command_id"), manifest_digest=value["manifest_digest"],
                   previous_digest=value.get("previous_digest"), token=value["token"])


@dataclass(frozen=True)
class StoredCommand:
    """A command as persisted in SQLite."""

    partition: str
    command_id: str
    request_digest: str
    state: str
    manifest: Manifest
    predecessor: object
    receipt: object
    trial: tuple = ()

    @property
    def pending(self):
        """Whether this command still waits for a confirmation."""
        return self.state in PENDING_STATES


@dataclass(frozen=True)
class CommandResult:
    """Outcome of preparing, sending, resuming or replaying a command."""

    command_id: str
    partition: str
    manifest: object
    receipt: object = None
    confirmed_version: int = None
    pending: bool = False
    response: object = None

    @property
    def version(self):
        """The version carried by the returned receipt, if any."""
        return self.receipt.version if self.receipt is not None else None

    @property
    def status(self):
        """Either ``pending`` or ``confirmed``."""
        return "pending" if self.pending else "confirmed"


class DeferredResponse:
    """A response handle which can be released after the warehouse accepted a manifest."""

    def __init__(self, command_id=None, partition=None):
        self.command_id = command_id
        self.partition = partition
        self._receipt = None
        self._ready = False
        self._lock = threading.RLock()

    def hold(self, receipt):
        """Store the original receipt without making it available yet."""
        with self._lock:
            self._receipt = _coerce_receipt(receipt)
        return self

    def resolve(self, receipt=None):
        """Make the held receipt available."""
        with self._lock:
            if receipt is not None:
                self._receipt = _coerce_receipt(receipt)
            if self._receipt is None:
                raise ResponseNotReady("No receipt has been attached to this response")
            self._ready = True
        return self

    release = resolve

    @property
    def ready(self):
        """Whether the response can be delivered."""
        with self._lock:
            return self._ready

    @property
    def held_receipt(self):
        """The receipt held by this response, if one has been attached."""
        with self._lock:
            return self._receipt

    def wait(self):
        """Return the original receipt or raise :class:`ResponseNotReady`."""
        with self._lock:
            if not self._ready or self._receipt is None:
                raise ResponseNotReady("The warehouse response is still pending")
            return self._receipt


class Warehouse:
    """Authoritative, receipt ordered publication log.

    A warehouse accepts at most one manifest per partition and version.  It
    returns the first receipt for a repeated ``(partition, command_id,
    manifest content)`` submission even if the partition has advanced since
    then.  Any other reuse of the same command id is rejected.
    """

    def __init__(self):
        self._current = {}
        self._manifests = {}
        self._commands = {}
        self._lock = threading.RLock()

    def seed(self, partition, version, command_id=None, candidates=None, payload=None):
        """Create an initial receipt for a partition.

        This is useful when a partition has already been confirmed before the
        process using this warehouse starts.
        """
        with self._lock:
            current = self._current.get(partition)
            if current is not None:
                if current.version == int(version):
                    return current
                raise VersionConflict("Partition '%s' is already at version %s"
                                      % (partition, current.version))
            manifest = Manifest.create(partition=partition, version=int(version),
                                       command_id=command_id or "__seed__",
                                       candidates=candidates, payload=payload)
            receipt = Receipt.create(manifest)
            self._manifests[(partition, manifest.version)] = manifest
            self._current[partition] = receipt
            if command_id is not None:
                self._commands[(partition, command_id)] = receipt
            return receipt

    bootstrap = seed

    def current_receipt(self, partition):
        """Return the receipt of the currently published version."""
        with self._lock:
            return self._current.get(partition)

    def manifest_for(self, partition, version):
        """Return the manifest accepted for ``version``."""
        with self._lock:
            return self._manifests.get((partition, int(version)))

    def manifests(self, partition):
        """Return all manifests of a partition ordered by version."""
        with self._lock:
            return [manifest for (part, _), manifest in sorted(self._manifests.items())
                    if part == partition]

    def receipt_for_command(self, partition, command_id):
        """Return the original receipt of a command, if it has been accepted."""
        with self._lock:
            return self._commands.get((partition, command_id))

    def receive(self, manifest, previous_receipt=None):
        """Accept a manifest and return its receipt.

        The method is idempotent for repeated submissions of the same command
        and manifest.  It rejects stale predecessors and content changes under
        an already used command id.
        """
        manifest = _coerce_manifest(manifest)
        previous = _coerce_receipt(previous_receipt)
        with self._lock:
            return self._receive(manifest, previous)

    publish = receive

    def _receive(self, manifest, previous):
        key = (manifest.partition, manifest.command_id)
        existing = self._commands.get(key)
        if existing is not None:
            if existing.manifest_digest != manifest.digest:
                raise CommandConflict("Command '%s' was already used for different content"
                                      % manifest.command_id)
            return existing

        version_key = (manifest.partition, manifest.version)
        if version_key in self._manifests:
            raise VersionConflict("Partition '%s' already accepted a manifest for version %s"
                                  % (manifest.partition, manifest.version))

        current = self._current.get(manifest.partition)
        if previous is None:
            if current is not None:
                raise StaleReceiptError("A previous receipt is required for partition '%s'"
                                        % manifest.partition)
        else:
            if previous.partition != manifest.partition:
                raise StaleReceiptError("Receipt belongs to a different partition")
            if current is None:
                self._current[manifest.partition] = previous
                current = previous
            elif previous.token != current.token or previous.version != current.version:
                raise StaleReceiptError(
                    "Manifest must build on the current receipt (version %s)" % current.version)

        receipt = Receipt.create(manifest, current)
        self._manifests[version_key] = manifest
        self._current[manifest.partition] = receipt
        self._commands[key] = receipt
        return receipt


class DeferredWarehouse(Warehouse):
    """A warehouse which withholds the first response.

    The first accepted manifest is stored and a :class:`DeferredResponse` is
    returned instead of its receipt.  Later submissions behave normally.  This
    models the gap between warehouse acceptance and the application receiving
    the acknowledgement.
    """

    def __init__(self, defer_first=True):
        super(DeferredWarehouse, self).__init__()
        self.defer_first = defer_first
        self._deferred = False
        self.deferred_responses = []

    def publish(self, manifest, previous_receipt=None):
        receipt = self.receive(manifest, previous_receipt)
        if self.defer_first and not self._deferred:
            self._deferred = True
            response = DeferredResponse(manifest.command_id, manifest.partition).hold(receipt)
            self.deferred_responses.append(response)
            return response
        return receipt

    @property
    def first_response(self):
        """The first deferred response, if one has been created."""
        return self.deferred_responses[0] if self.deferred_responses else None

    def release_first_response(self, receipt=None):
        """Release the first withheld response."""
        response = self.first_response
        if response is None:
            raise ResponseNotReady("No response has been deferred")
        return response.resolve(receipt)

    release = release_first_response

    receive = Warehouse.receive


class SQLiteCommandStore:
    """SQLite journal for publication commands and local confirmations."""

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS publish_confirmations (
        partition TEXT PRIMARY KEY,
        version INTEGER NOT NULL,
        receipt TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS publish_commands (
        partition TEXT NOT NULL,
        command_id TEXT NOT NULL,
        request_digest TEXT NOT NULL,
        state TEXT NOT NULL,
        manifest TEXT NOT NULL,
        predecessor TEXT,
        receipt TEXT,
        updated REAL NOT NULL,
        PRIMARY KEY (partition, command_id)
    );
    CREATE TABLE IF NOT EXISTS publish_trials (
        partition TEXT NOT NULL,
        command_id TEXT NOT NULL,
        candidate_index INTEGER NOT NULL,
        raw TEXT,
        cleaned TEXT,
        accepted INTEGER NOT NULL,
        error TEXT,
        PRIMARY KEY (partition, command_id, candidate_index)
    );
    """

    def __init__(self, path=":memory:", connection=None):
        self.path = str(path)
        self._lock = threading.RLock()
        self._connection = connection or sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        with self._connection:
            self._connection.executescript(self._SCHEMA)

    def close(self):
        """Close the underlying SQLite connection."""
        with self._lock:
            self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _receipt_from_row(self, row):
        return Receipt.from_dict(json.loads(row["receipt"]))

    def get_confirmation(self, partition):
        """Return the locally confirmed receipt of a partition."""
        with self._lock:
            row = self._connection.execute(
                "SELECT receipt FROM publish_confirmations WHERE partition = ?", (partition,)).fetchone()
            return self._receipt_from_row(row) if row is not None else None

    def confirmed_version(self, partition):
        """Return the locally confirmed version or ``None``."""
        receipt = self.get_confirmation(partition)
        return receipt.version if receipt is not None else None

    def seed_confirmation(self, partition, receipt):
        """Set the initial confirmation if the partition has none yet."""
        receipt = _coerce_receipt(receipt)
        with self._lock, self._connection:
            row = self._connection.execute(
                "SELECT receipt FROM publish_confirmations WHERE partition = ?", (partition,)).fetchone()
            if row is None:
                self._connection.execute(
                    "INSERT INTO publish_confirmations (partition, version, receipt) VALUES (?, ?, ?)",
                    (partition, receipt.version, json.dumps(receipt.as_dict(), sort_keys=True)))
                return receipt
            current = self._receipt_from_row(row)
            if current.version != receipt.version or current.token != receipt.token:
                raise ReceiptMismatch("Partition '%s' already has a different confirmation" % partition)
            return current

    def set_confirmation(self, partition, receipt):
        """Advance the confirmation monotonically.

        Lower versions are ignored so a delayed acknowledgement cannot roll
        back a process which has already confirmed a later version.
        """
        receipt = _coerce_receipt(receipt)
        if receipt.partition != partition:
            raise ReceiptMismatch("Receipt belongs to a different partition")
        with self._lock, self._connection:
            row = self._connection.execute(
                "SELECT receipt FROM publish_confirmations WHERE partition = ?", (partition,)).fetchone()
            if row is None:
                self._connection.execute(
                    "INSERT INTO publish_confirmations (partition, version, receipt) VALUES (?, ?, ?)",
                    (partition, receipt.version, json.dumps(receipt.as_dict(), sort_keys=True)))
                return receipt
            current = self._receipt_from_row(row)
            if receipt.version < current.version:
                return current
            if receipt.version == current.version:
                if receipt.token != current.token:
                    raise ReceiptMismatch("Version %s already has a different receipt" % receipt.version)
                return current
            if receipt.previous_digest != current.token:
                raise ReceiptMismatch("Receipt does not extend the confirmed receipt")
            self._connection.execute(
                "UPDATE publish_confirmations SET version = ?, receipt = ? WHERE partition = ?",
                (receipt.version, json.dumps(receipt.as_dict(), sort_keys=True), partition))
            return receipt

    def save_prepared(self, partition, command_id, request_digest, manifest, predecessor=None, trial=None):
        """Persist a selected manifest and its trial results before sending."""
        manifest = _coerce_manifest(manifest)
        predecessor = _coerce_receipt(predecessor)
        trial = list(trial or ())
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO publish_commands "
                "(partition, command_id, request_digest, state, manifest, predecessor, receipt, updated) "
                "VALUES (?, ?, ?, 'prepared', ?, ?, NULL, ?)",
                (partition, command_id, request_digest,
                 json.dumps(manifest.as_dict(), sort_keys=True),
                 json.dumps(predecessor.as_dict(), sort_keys=True) if predecessor is not None else None,
                 time.time()))
            self._connection.execute(
                "DELETE FROM publish_trials WHERE partition = ? AND command_id = ?",
                (partition, command_id))
            for index, record in enumerate(trial):
                self._connection.execute(
                    "INSERT INTO publish_trials "
                    "(partition, command_id, candidate_index, raw, cleaned, accepted, error) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (partition, command_id, index,
                     json.dumps(record.get("raw"), sort_keys=True, default=repr),
                     json.dumps(record.get("cleaned"), sort_keys=True, default=repr),
                     1 if record.get("accepted") else 0,
                     record.get("error")))

    def load_trials(self, partition, command_id):
        """Return the persisted trial records of a command."""
        with self._lock:
            rows = self._connection.execute(
                "SELECT raw, cleaned, accepted, error FROM publish_trials "
                "WHERE partition = ? AND command_id = ? ORDER BY candidate_index",
                (partition, command_id)).fetchall()
            return tuple({
                "raw": json.loads(row["raw"]) if row["raw"] is not None else None,
                "cleaned": json.loads(row["cleaned"]) if row["cleaned"] is not None else None,
                "accepted": bool(row["accepted"]),
                "error": row["error"],
            } for row in rows)

    def load_command(self, partition, command_id):
        """Return a stored command or ``None``."""
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM publish_commands WHERE partition = ? AND command_id = ?",
                (partition, command_id)).fetchone()
            return self._row_to_command(row) if row is not None else None

    def _row_to_command(self, row):
        return StoredCommand(
            partition=row["partition"],
            command_id=row["command_id"],
            request_digest=row["request_digest"],
            state=row["state"],
            manifest=Manifest.from_dict(json.loads(row["manifest"])),
            predecessor=Receipt.from_dict(json.loads(row["predecessor"])) if row["predecessor"] else None,
            receipt=Receipt.from_dict(json.loads(row["receipt"])) if row["receipt"] else None,
            trial=self.load_trials(row["partition"], row["command_id"]))

    def pending_commands(self, partition):
        """Return prepared and sent commands in insertion order."""
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM publish_commands WHERE partition = ? AND state IN (?, ?) ORDER BY rowid",
                (partition,) + PENDING_STATES).fetchall()
            return [self._row_to_command(row) for row in rows]

    def mark_sent(self, partition, command_id):
        """Mark a command as sent before contacting the warehouse."""
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE publish_commands SET state = 'sent', updated = ? "
                "WHERE partition = ? AND command_id = ? AND state IN ('prepared', 'sent')",
                (time.time(), partition, command_id))

    def mark_confirmed(self, partition, command_id, receipt):
        """Store the warehouse receipt on a command."""
        receipt = _coerce_receipt(receipt)
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE publish_commands SET state = 'confirmed', receipt = ?, updated = ? "
                "WHERE partition = ? AND command_id = ?",
                (json.dumps(receipt.as_dict(), sort_keys=True), time.time(), partition, command_id))

    def mark_failed(self, partition, command_id):
        """Mark a command as failed so it no longer blocks the partition."""
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE publish_commands SET state = 'failed', updated = ? "
                "WHERE partition = ? AND command_id = ?",
                (time.time(), partition, command_id))

    def delete_command(self, partition, command_id):
        """Remove a command and its trial records."""
        with self._lock, self._connection:
            self._connection.execute(
                "DELETE FROM publish_trials WHERE partition = ? AND command_id = ?",
                (partition, command_id))
            self._connection.execute(
                "DELETE FROM publish_commands WHERE partition = ? AND command_id = ?",
                (partition, command_id))


class PublishingMachine(Machine):
    """A ``Machine`` with an opt-in recoverable publication command path.

    The configured ``publish_trigger`` keeps its normal behaviour when called
    without ``command_id``.  With a command id, the trigger prepares or
    resumes the command through ``publish_command``.
    """

    def __init__(self, model=Machine.self_literal, states=None, initial='initial', transitions=None,
                 send_event=False, auto_transitions=True, ordered_transitions=False,
                 ignore_invalid_triggers=None, before_state_change=None, after_state_change=None,
                 name=None, queued=False, prepare_event=None, finalize_event=None,
                 model_attribute='state', model_override=False, on_exception=None, on_final=None,
                 warehouse=None, command_store=None, db_path=None, publish_trigger='publish',
                 clean=None, trial=None, partition=None, **kwargs):
        clean = clean if clean is not None else kwargs.pop("cleaner", None)
        trial = trial if trial is not None else kwargs.pop("candidate_condition", None)
        if trial is None:
            trial = kwargs.pop("accept_candidate", None)
        partition = partition if partition is not None else kwargs.pop("partition_id", None)
        db_path = db_path if db_path is not None else kwargs.pop("command_db", None)
        if db_path is None:
            db_path = kwargs.pop("journal_path", None)
        publish_trigger = kwargs.pop("publish_event", publish_trigger)
        self.warehouse = warehouse if warehouse is not None else Warehouse()
        if command_store is not None and db_path is not None:
            raise ValueError("Pass either command_store or db_path, not both")
        self.command_store = command_store if command_store is not None \
            else SQLiteCommandStore(db_path if db_path is not None else ":memory:")
        self.publish_trigger = publish_trigger
        self.clean = clean
        self.trial = trial
        self.partition = partition
        self._responses = {}
        super(PublishingMachine, self).__init__(
            model=model, states=states, initial=initial, transitions=transitions,
            send_event=send_event, auto_transitions=auto_transitions,
            ordered_transitions=ordered_transitions, ignore_invalid_triggers=ignore_invalid_triggers,
            before_state_change=before_state_change, after_state_change=after_state_change,
            name=name, queued=queued, prepare_event=prepare_event, finalize_event=finalize_event,
            model_attribute=model_attribute, model_override=model_override,
            on_exception=on_exception, on_final=on_final, **kwargs)
        for mod in list(self.models):
            self._bind_commands(mod)
            self._refresh_model_state(mod)

    def add_model(self, model, initial=None):
        super(PublishingMachine, self).add_model(model, initial=initial)
        for mod in listify(model):
            mod = self if mod is self.self_literal else mod
            self._bind_commands(mod)
            self._refresh_model_state(mod)

    def add_transition(self, trigger, source, dest, conditions=None,
                       unless=None, before=None, after=None, prepare=None, **kwargs):
        super(PublishingMachine, self).add_transition(trigger, source, dest, conditions=conditions,
                                                      unless=unless, before=before, after=after,
                                                      prepare=prepare, **kwargs)
        if trigger == self.publish_trigger:
            for mod in list(self.models):
                self._bind_commands(mod)

    def _bind_commands(self, model):
        if model is not self:
            setattr(model, "publish_command", partial(self.publish_command, model=model))
            setattr(model, "prepare_command", partial(self.prepare_command, model=model))
            setattr(model, "resume_command", partial(self.resume_command, model=model))
            setattr(model, "deliver_response", partial(self.deliver, model=model))
            setattr(model, "release_response", partial(self.release_response, model=model))
        event = self.events.get(self.publish_trigger)
        if event is not None:
            setattr(model, self.publish_trigger,
                    partial(self._dispatch_publish, model, self.publish_trigger, event))

    def _dispatch_publish(self, model, trigger_name, event, *args, **kwargs):
        command_id = kwargs.pop("command_id", None)
        if command_id is None:
            command_id = kwargs.pop("command", None)
        if command_id is None:
            return event.trigger(model, *args, **kwargs)
        if args and "candidates" not in kwargs and "inputs" not in kwargs:
            kwargs["candidates"] = args[0] if len(args) == 1 else list(args)
        partition = self._partition(kwargs.get("partition"))
        existing = self.command_store.load_command(partition, command_id)
        result = self.publish_command(command_id, model=model, **kwargs)
        if existing is None or existing.state != "confirmed":
            accepted = list(result.manifest.candidates) if result.manifest is not None else []
            candidate = accepted[0] if accepted else None
            event_data = self._event_data(model, command_id, partition,
                                          result.manifest.version if result.manifest is not None else None,
                                          candidate=candidate)
            event_data.cleaned = candidate
            self._execute_prechecked(event, event_data)
        return result

    def _execute_prechecked(self, event, event_data):
        """Execute the first applicable transition for the selected candidate."""
        for transition in event.transitions[event_data.state.name]:
            event_data.transition = transition
            self.callbacks(transition.prepare, event_data)
            if not transition._eval_conditions(event_data):
                continue
            self.callbacks(itertools.chain(self.before_state_change, transition.before), event_data)
            if transition.dest is not None:
                transition._change_state(event_data)
            self.callbacks(itertools.chain(transition.after, self.after_state_change), event_data)
            event_data.result = True
            return True
        return False

    def _model(self, model=None):
        if model is not None:
            return model
        if len(self.models) == 1:
            return self.models[0]
        return self

    def _partition(self, partition=None):
        result = partition if partition is not None else self.partition
        if result is None:
            raise ValueError("A partition is required for publication commands")
        return result

    def _event_data(self, model, command_id, partition, version, candidate=None):
        event_data = EventData(self.get_model_state(model), Event(self.publish_trigger, self),
                               self, model, args=(), kwargs={})
        event_data.command_id = command_id
        event_data.command = command_id
        event_data.partition = partition
        event_data.version = version
        event_data.candidate = candidate
        event_data.store = self.command_store
        event_data.journal = self.command_store
        event_data.warehouse = self.warehouse
        return event_data

    def _invoke(self, callback, event_data, value):
        if callback is None:
            return value
        func = self.resolve_callable(callback, event_data)
        event_data.cleaned = value
        if self.send_event:
            return func(event_data)
        return func(value)

    def _trial(self, callback, event_data, value):
        if callback is None:
            return True
        callbacks = listify(callback)
        event_data.cleaned = value
        for item in callbacks:
            func = self.resolve_callable(item, event_data)
            passed = func(event_data) if self.send_event else func(value)
            if not passed:
                return False
        return True

    def _event_conditions(self, event, event_data):
        state = event_data.state
        for transition in event.transitions.get(state.name, []):
            if not transition.conditions:
                return True
            event_data.transition = transition
            if transition._eval_conditions(event_data):
                return True
        return False

    def _run_candidates(self, model, command_id, partition, version, candidates):
        accepted = []
        records = []
        event = self.events.get(self.publish_trigger)
        for raw in candidates:
            event_data = self._event_data(model, command_id, partition, version, candidate=raw)
            record = {"raw": raw, "cleaned": None, "accepted": False, "error": None}
            try:
                cleaned = self._invoke(self.clean, event_data, raw)
                record["cleaned"] = cleaned
                event_data.cleaned = cleaned
                record["accepted"] = bool(self._trial(self.trial, event_data, cleaned))
                if record["accepted"] and event is not None:
                    record["accepted"] = self._event_conditions(event, event_data)
                if record["accepted"]:
                    accepted.append(cleaned)
            except Exception as err:
                record["error"] = "%s: %s" % (type(err).__name__, err)
            records.append(record)
        if not accepted:
            raise NoAcceptedCandidate("No candidate passed the trial for command '%s'" % command_id)
        return accepted, records

    def prepare_command(self, command_id, partition=None, version=None, candidates=None, inputs=None,
                        payload=None, manifest=None, model=None, **options):
        """Prepare and persist a command without sending it.

        Reusing a command id with a different request raises
        :class:`CommandConflict` before changing local or warehouse state.
        Repeating the same request returns the already persisted manifest and
        does not run cleaning or trial callbacks again.
        """
        model = self._model(model)
        partition = self._model_partition(model, partition)
        raw_candidates = candidates if candidates is not None else inputs
        request_options = dict(options)
        for key in ("deliver_response", "response", "simulate_lost_response", "model"):
            request_options.pop(key, None)
        request = {
            "partition": partition,
            "command_id": command_id,
            "candidates": _freeze(list(raw_candidates) if raw_candidates is not None else None),
            "payload": _freeze(payload),
            "options": _freeze(request_options),
        }
        if version is not None:
            request["version"] = int(version)
        if manifest is not None:
            request["manifest"] = _coerce_manifest(manifest).digest
        request_digest = _digest(request)

        stored = self.command_store.load_command(partition, command_id)
        if stored is not None and stored.request_digest != request_digest:
            raise CommandConflict("Command '%s' was already used for different content" % command_id)
        if stored is not None:
            self._refresh_model_state(model)
            return stored

        pending = self.command_store.pending_commands(partition)
        if pending:
            current = self.command_store.get_confirmation(partition)
            raise PendingCommandError(pending[0].command_id, current.version if current else None)

        current = self.command_store.get_confirmation(partition)
        if manifest is not None:
            selected = _coerce_manifest(manifest)
            if selected.partition != partition:
                raise ValueError("Manifest partition does not match the command partition")
            if selected.command_id != command_id:
                raise ValueError("Manifest command id does not match the command id")
            if version is not None and selected.version != int(version):
                raise ValueError("Manifest version does not match the requested version")
            trial = self.command_store.load_trials(partition, command_id)
        else:
            if version is None:
                version = current.version + 1 if current is not None else 1
            version = int(version)
            candidates = listify(raw_candidates) if raw_candidates is not None else []
            accepted, trial = self._run_candidates(model, command_id, partition, version, candidates)
            selected = Manifest.create(partition=partition, version=version, command_id=command_id,
                                       candidates=accepted, payload=payload)

        self.command_store.save_prepared(partition, command_id, request_digest, selected,
                                         predecessor=current, trial=trial)
        self._refresh_model_state(model)
        return self.command_store.load_command(partition, command_id)

    def publish_command(self, command_id, partition=None, version=None, candidates=None, inputs=None,
                        payload=None, manifest=None, model=None, **options):
        """Prepare if necessary, send, and return a :class:`CommandResult`."""
        model = self._model(model)
        partition = self._model_partition(model, partition)
        stored = self.prepare_command(command_id, partition=partition, version=version,
                                      candidates=candidates, inputs=inputs, payload=payload,
                                      manifest=manifest, model=model, **options)
        if stored.state == "confirmed" and stored.receipt is not None:
            self._refresh_model_state(model)
            current = self.command_store.get_confirmation(partition)
            return CommandResult(command_id, partition, stored.manifest, receipt=stored.receipt,
                                 confirmed_version=current.version if current else None)
        return self._send(stored, model, **options)

    run_command = publish_command

    def resume_command(self, command_id, partition=None, model=None, **options):
        """Continue a command which was persisted by an earlier process."""
        model = self._model(model)
        partition = self._model_partition(model, partition)
        stored = self.command_store.load_command(partition, command_id)
        if stored is None:
            raise PublishingError("Unknown command '%s' for partition '%s'" % (command_id, partition))
        if stored.state == "confirmed" and stored.receipt is not None:
            self._refresh_model_state(model)
            current = self.command_store.get_confirmation(partition)
            return CommandResult(command_id, partition, stored.manifest, receipt=stored.receipt,
                                 confirmed_version=current.version if current else None)
        return self._send(stored, model, **options)

    resume = resume_command

    def _send(self, stored, model, simulate_lost_response=False, **options):
        partition = stored.partition
        command_id = stored.command_id
        current = self.command_store.get_confirmation(partition)
        previous = current if current is not None else stored.predecessor
        self.command_store.mark_sent(partition, command_id)
        self._responses.pop((partition, command_id), None)
        accepted = self.warehouse.receipt_for_command(partition, command_id)
        if accepted is not None:
            if simulate_lost_response:
                self._refresh_model_state(model)
                return CommandResult(command_id, partition, stored.manifest, pending=True,
                                     confirmed_version=current.version if current else None,
                                     receipt=accepted)
            return self._complete(stored, accepted, model)
        try:
            response = self.warehouse.publish(stored.manifest, previous_receipt=previous)
        except ResponseLost as err:
            self._refresh_model_state(model)
            if isinstance(err.response, DeferredResponse):
                self._register_response(err.response)
            return CommandResult(command_id, partition, stored.manifest, pending=True,
                                 confirmed_version=current.version if current else None,
                                 response=err.response)
        except Exception:
            self.command_store.mark_failed(partition, command_id)
            self._refresh_model_state(model)
            raise

        if isinstance(response, DeferredResponse):
            if response.command_id is None:
                response.command_id = command_id
            if response.partition is None:
                response.partition = partition
            self._register_response(response)
            if not response.ready:
                self._refresh_model_state(model)
                return CommandResult(command_id, partition, stored.manifest, pending=True,
                                     confirmed_version=current.version if current else None,
                                     response=response)
            response = response.wait()

        if simulate_lost_response:
            self._refresh_model_state(model)
            return CommandResult(command_id, partition, stored.manifest, pending=True,
                                 confirmed_version=current.version if current else None,
                                 receipt=_coerce_receipt(response))

        return self._complete(stored, _coerce_receipt(response), model)

    def _register_response(self, response):
        if response.command_id is not None and response.partition is not None:
            self._responses[(response.partition, response.command_id)] = response

    def _complete(self, stored, receipt, model):
        if receipt.partition != stored.partition:
            raise ReceiptMismatch("Receipt belongs to a different partition")
        if receipt.manifest_digest != stored.manifest.digest:
            raise ReceiptMismatch("Receipt does not acknowledge the command manifest")
        self.command_store.mark_confirmed(stored.partition, stored.command_id, receipt)
        self.command_store.set_confirmation(stored.partition, receipt)
        self._refresh_model_state(model)
        current = self.command_store.get_confirmation(stored.partition)
        return CommandResult(stored.command_id, stored.partition, stored.manifest, receipt=receipt,
                             confirmed_version=current.version if current else None)

    def deliver(self, response, receipt=None, model=None, partition=None):
        """Deliver a deferred or late warehouse response.

        Delivering an older receipt after a newer version has been confirmed
        is harmless: the returned receipt is preserved but the local
        confirmation stays at the newer version.  A deferred response is
        released by this call unless an explicit ``receipt`` is passed.
        """
        model = self._model(model)
        command_id = getattr(response, "command_id", None)
        partition = partition if partition is not None else getattr(response, "partition", None)
        if isinstance(response, str):
            command_id = response
            partition = self._model_partition(model, partition)
            stored = self.command_store.load_command(partition, command_id)
            if stored is None:
                raise PublishingError("Unknown command '%s' for partition '%s'" % (command_id, partition))
            if receipt is None:
                receipt = stored.receipt or self.warehouse.receipt_for_command(partition, command_id)
                if receipt is None:
                    raise PublishingError("There is no warehouse receipt for command '%s'" % command_id)
            return self._complete(stored, _coerce_receipt(receipt), model)
        if receipt is not None:
            response.resolve(receipt)
        elif not response.ready and response.held_receipt is not None:
            response.resolve()
        receipt = response.wait()
        partition = partition if partition is not None else receipt.partition
        stored = self.command_store.load_command(partition, command_id)
        if stored is None:
            raise PublishingError("Unknown command '%s' for partition '%s'" % (command_id, partition))
        return self._complete(stored, _coerce_receipt(receipt), model)

    deliver_response = deliver

    note_receipt = deliver

    def release_response(self, response=None, partition=None, command_id=None, receipt=None, model=None):
        """Release a deferred warehouse response and deliver its receipt."""
        if response is None:
            model = self._model(model)
            partition = self._model_partition(model, partition)
            response = self._responses.get((partition, command_id))
            if response is None and isinstance(self.warehouse, DeferredWarehouse):
                response = self.warehouse.first_response
            if response is None:
                raise ResponseNotReady("No deferred response is available")
        return self.deliver(response, receipt=receipt, model=model)

    def get_confirmation(self, partition=None):
        """Return the locally confirmed receipt."""
        return self.command_store.get_confirmation(self._partition(partition))

    def get_confirmed_version(self, partition=None):
        """Return the locally confirmed version."""
        return self.command_store.confirmed_version(self._partition(partition))

    def get_pending_command_id(self, partition=None):
        """Return the first unconfirmed command id, if any."""
        pending = self.command_store.pending_commands(self._partition(partition))
        return pending[0].command_id if pending else None

    @property
    def confirmed_version(self):
        """The locally confirmed version of the machine's default partition."""
        return self.get_confirmed_version()

    @property
    def pending_command_id(self):
        """The first unconfirmed command of the machine's default partition."""
        return self.get_pending_command_id()

    def _refresh_model_state(self, model):
        partition = getattr(model, "_publishing_partition", None) or self.partition
        receipt = self.command_store.get_confirmation(partition) if partition is not None else None
        pending = self.command_store.pending_commands(partition) if partition is not None else []
        if model is not self:
            setattr(model, "confirmed_version", receipt.version if receipt is not None else None)
            setattr(model, "confirmed_receipt", receipt)
            setattr(model, "pending_command_id", pending[0].command_id if pending else None)

    def _model_partition(self, model, partition=None):
        result = partition if partition is not None else self._partition()
        if model is not self:
            setattr(model, "_publishing_partition", result)
        return result

    def seed(self, version, partition=None, candidates=None, payload=None):
        """Seed the local confirmation and warehouse with an initial version."""
        partition = self._partition(partition)
        receipt = self.warehouse.seed(partition, version, candidates=candidates, payload=payload)
        self.command_store.seed_confirmation(partition, receipt)
        for mod in self.models:
            self._refresh_model_state(mod)
        return receipt

    bootstrap = seed


# Readable aliases for callers which use the nouns from the workflow description.
Publication = Manifest
Acknowledgement = Receipt
WarehouseReceipt = Receipt
CommandJournal = SQLiteCommandStore
CommandStore = SQLiteCommandStore
PublishMachine = PublishingMachine
PublicationMachine = PublishingMachine
RecoverablePublishingMachine = PublishingMachine
CommandIdConflict = CommandConflict
CommandPending = PendingCommandError
PublicationResult = CommandResult
