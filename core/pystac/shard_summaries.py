from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Iterable, Optional

#: Cloud cover is not known for this record. The detail data must be consulted.
UNKNOWN: str = "unknown"

#: Cloud cover is explicitly not applicable to the record. Such records can be
#: excluded when deciding whether a device may produce matching imagery.
NOT_APPLICABLE: str = "not_applicable"

CloudCoverState = int | str
"""A cloud cover state is either an integer percentage in the range 0-100,
:data:`UNKNOWN`, or :data:`NOT_APPLICABLE`."""


def _validate_state(state: Any) -> CloudCoverState:
    if isinstance(state, bool) or not isinstance(state, (int, str)):
        raise ValueError(
            f"Cloud cover state must be an int 0-100, {UNKNOWN!r} or "
            f"{NOT_APPLICABLE!r}, got {state!r}"
        )
    if isinstance(state, str):
        if state not in (UNKNOWN, NOT_APPLICABLE):
            raise ValueError(
                f"Unknown cloud cover state {state!r}; expected an int 0-100, "
                f"{UNKNOWN!r} or {NOT_APPLICABLE!r}"
            )
    elif not 0 <= state <= 100:
        raise ValueError(f"Cloud cover percentage must be within 0-100, got {state}")
    return state


def _canonical(payload: Any) -> str:
    return json_dumps(payload)


def _decode_state(value: Any) -> CloudCoverState:
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return _validate_state(value)


def json_dumps(payload: Any) -> str:
    import json

    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class ShardCorrectionEntry:
    """A single record inside a :class:`ShardCorrection`.

    A correction moves a record from ``before_device``/``before_state`` to
    ``after_device``/``after_state``. A withdrawal keeps the original state and
    leaves the after fields as ``None``.
    """

    before_device: str
    before_state: CloudCoverState
    after_device: Optional[str]
    after_state: Optional[CloudCoverState]

    def __post_init__(self) -> None:
        _validate_state(self.before_state)
        if not self.is_withdrawal:
            if self.after_device is None:
                raise ValueError(
                    "after_device is required unless the entry is a withdrawal"
                )
            _validate_state(self.after_state)

    @classmethod
    def withdraw(cls, device: str, state: CloudCoverState) -> ShardCorrectionEntry:
        """Build an entry that withdraws a record previously seen on ``device``."""
        return cls(
            before_device=device,
            before_state=state,
            after_device=None,
            after_state=None,
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ShardCorrectionEntry:
        return cls(
            before_device=d["before_device"],
            before_state=d["before_state"],
            after_device=d.get("after_device"),
            after_state=d.get("after_state"),
        )

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "before_device": self.before_device,
            "before_state": self.before_state,
        }
        if not self.is_withdrawal:
            d["after_device"] = self.after_device
            d["after_state"] = self.after_state
        return d

    @property
    def is_withdrawal(self) -> bool:
        return self.after_device is None and self.after_state is None


@dataclass(frozen=True)
class FullShardSummary:
    """A complete vendor summary for one delivery partition up to ``sequence``.

    A full summary independently replaces every older correction whose sequence
    it covers. ``records`` is an iterable of ``(device, cloud_cover_state)``
    pairs; one pair per sealed record.
    """

    provider: str
    partition: str
    sequence: int
    records: tuple[tuple[str, CloudCoverState], ...]

    def __init__(
        self,
        provider: str,
        partition: str,
        sequence: int,
        records: Iterable[tuple[str, CloudCoverState]],
    ) -> None:
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "partition", partition)
        object.__setattr__(self, "sequence", sequence)
        normalized = tuple(
            (device, _validate_state(state)) for device, state in records
        )
        object.__setattr__(self, "records", normalized)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> FullShardSummary:
        return cls(
            provider=d["provider"],
            partition=d["partition"],
            sequence=d["sequence"],
            records=[(record[0], record[1]) for record in d["records"]],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "partition": self.partition,
            "sequence": self.sequence,
            "records": [[device, state] for device, state in self.records],
        }

    def canonical(self) -> str:
        return _canonical(self.to_dict())


@dataclass(frozen=True)
class ShardCorrection:
    """A numbered correction that follows a full summary.

    Each entry describes the device and cloud cover state before and after the
    correction. Withdrawals use :meth:`ShardCorrectionEntry.withdraw` and still
    record the original state.
    """

    provider: str
    partition: str
    sequence: int
    entries: tuple[ShardCorrectionEntry, ...]

    def __init__(
        self,
        provider: str,
        partition: str,
        sequence: int,
        entries: Iterable[ShardCorrectionEntry | tuple[Any, ...]],
    ) -> None:
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "partition", partition)
        object.__setattr__(self, "sequence", sequence)
        normalized = tuple(
            entry
            if isinstance(entry, ShardCorrectionEntry)
            else ShardCorrectionEntry(
                before_device=entry[0],
                before_state=entry[1],
                after_device=entry[2],
                after_state=entry[3],
            )
            for entry in entries
        )
        object.__setattr__(self, "entries", normalized)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ShardCorrection:
        return cls(
            provider=d["provider"],
            partition=d["partition"],
            sequence=d["sequence"],
            entries=[ShardCorrectionEntry.from_dict(entry) for entry in d["entries"]],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "partition": self.partition,
            "sequence": self.sequence,
            "entries": [entry.to_dict() for entry in self.entries],
        }

    def canonical(self) -> str:
        return _canonical(self.to_dict())


class _PartitionState:
    """Mutable bookkeeping for one (provider, delivery partition) shard."""

    def __init__(self, devices: Iterable[str]) -> None:
        self.devices: set[str] = set(devices)
        self.full_sequence: int | None = None
        # sequence -> content hashes of every full summary version seen. The
        # hashes are constant-size, so same-sequence divergence stays
        # detectable even after the per-scene payloads were folded away.
        self.full_hashes: dict[int, set[str]] = {}
        # sequence -> content hash -> full summary. Payloads are retained only
        # while a sequence is conflicted; an uncontested full summary is
        # folded into the histogram immediately.
        self.full_versions: dict[int, dict[str, FullShardSummary]] = {}
        # device -> state -> count. Sealed data is a histogram, never a
        # per-scene record list.
        self.sealed: dict[str, dict[CloudCoverState, int]] = {}
        # sequence -> content hash -> correction. Only corrections newer than
        # the newest full summary survive; covered ones are dropped.
        self.correction_versions: dict[int, dict[str, ShardCorrection]] = {}

    def histogram_from_records(
        self, records: Iterable[tuple[str, CloudCoverState]]
    ) -> dict[str, dict[CloudCoverState, int]]:
        histogram: dict[str, dict[CloudCoverState, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        for device, state in records:
            if device not in self.devices:
                raise ValueError(
                    f"Device {device!r} is not registered to this partition"
                )
            histogram[device][state] += 1
        return {device: dict(states) for device, states in histogram.items()}


class ShardSummaries:
    """Optional per-partition cloud cover summaries attached to a Collection.

    Shards are keyed by ``(provider, delivery_partition)`` and the device range
    of every partition must be registered up front. Vendors publish full
    summaries up to a sequence number and numbered corrections that continue
    from it. Files may arrive through different mirrors, out of order,
    duplicated or with divergent content for the same sequence.

    A partition can only be used to exclude devices from a query while its
    sequence stream is contiguous and free of unresolved conflicts. A full
    summary independently replaces every correction whose sequence it covers.
    """

    def __init__(self) -> None:
        self._partitions: dict[tuple[str, str], _PartitionState] = {}

    def is_empty(self) -> bool:
        return not self._partitions

    def clone(self) -> ShardSummaries:
        clone = ShardSummaries()
        clone._partitions = deepcopy(self._partitions)
        return clone

    def register_partition(
        self, provider: str, partition: str, devices: Iterable[str]
    ) -> None:
        """Register the device range governed by a delivery partition."""
        key = (provider, partition)
        state = self._partitions.get(key)
        if state is None:
            self._partitions[key] = _PartitionState(devices)
        else:
            state.devices.update(devices)

    def partitions(self) -> list[tuple[str, str]]:
        return list(self._partitions)

    def partition_devices(self, provider: str, partition: str) -> frozenset[str]:
        state = self._partitions.get((provider, partition))
        if state is None:
            raise KeyError(f"Unknown partition {(provider, partition)!r}")
        return frozenset(state.devices)

    def partitions_for_device(self, device: str) -> list[tuple[str, str]]:
        return [
            key for key, state in self._partitions.items() if device in state.devices
        ]

    def apply_message(self, message: FullShardSummary | ShardCorrection) -> None:
        """Apply a parsed summary file, regardless of which mirror delivered it."""
        if isinstance(message, FullShardSummary):
            self.apply_full(message)
        elif isinstance(message, ShardCorrection):
            self.apply_correction(message)
        else:
            raise TypeError(f"Unsupported shard summary message {message!r}")

    def apply_full(self, summary: FullShardSummary) -> None:
        state = self._require_partition(summary.provider, summary.partition)
        for device, _ in summary.records:
            if device not in state.devices:
                raise ValueError(
                    f"Device {device!r} is not registered to partition "
                    f"{summary.partition!r} of provider {summary.provider!r}"
                )
        if state.full_sequence is not None and summary.sequence < state.full_sequence:
            # An older full summary cannot reopen already sealed data.
            return
        canonical = summary.canonical()
        if state.full_sequence == summary.sequence:
            state.full_hashes.setdefault(summary.sequence, set()).add(canonical)
            versions = state.full_versions.setdefault(summary.sequence, {})
            versions.setdefault(canonical, summary)
            return
        state.full_hashes = {summary.sequence: {canonical}}
        state.full_versions = {summary.sequence: {canonical: summary}}
        state.full_sequence = summary.sequence
        state.sealed = state.histogram_from_records(summary.records)
        # The full summary independently replaces every correction it covers.
        for covered in [
            sequence
            for sequence in state.correction_versions
            if sequence <= summary.sequence
        ]:
            del state.correction_versions[covered]

    def apply_correction(self, correction: ShardCorrection) -> None:
        state = self._require_partition(correction.provider, correction.partition)
        for entry in correction.entries:
            for device in (entry.before_device, entry.after_device):
                if device is not None and device not in state.devices:
                    raise ValueError(
                        f"Device {device!r} is not registered to partition "
                        f"{correction.partition!r} of provider "
                        f"{correction.provider!r}"
                    )
        if (
            state.full_sequence is not None
            and correction.sequence <= state.full_sequence
        ):
            # Already sealed by a covering full summary; replay must not
            # reintroduce the correction or its conflict.
            return
        versions = state.correction_versions.setdefault(correction.sequence, {})
        versions.setdefault(correction.canonical(), correction)

    def _require_partition(self, provider: str, partition: str) -> _PartitionState:
        state = self._partitions.get((provider, partition))
        if state is None:
            raise KeyError(
                f"Partition {partition!r} for provider {provider!r} must be "
                "registered before applying shard summary data"
            )
        return state

    def is_partition_usable(self, provider: str, partition: str) -> bool:
        """Return whether the partition may be used to exclude devices.

        A partition is unusable while it has no sealed baseline, has a gap in
        its correction sequence, or holds two unresolved versions of the same
        sequence.
        """
        state = self._partitions.get((provider, partition))
        if state is None or state.full_sequence is None:
            return False
        if self._has_conflict(state) or self._has_gap(state):
            return False
        return True

    @staticmethod
    def _has_conflict(state: _PartitionState) -> bool:
        if any(len(hashes) > 1 for hashes in state.full_hashes.values()):
            return True
        return any(len(versions) > 1 for versions in state.correction_versions.values())

    @staticmethod
    def _has_gap(state: _PartitionState) -> bool:
        assert state.full_sequence is not None
        sequences = sorted(state.correction_versions)
        if not sequences:
            return False
        expected = list(range(state.full_sequence + 1, sequences[-1] + 1))
        return sequences != expected

    def conflicted_sequences(self, provider: str, partition: str) -> list[int]:
        state = self._require_partition(provider, partition)
        conflicted = [
            sequence
            for sequence, versions in state.correction_versions.items()
            if len(versions) > 1
        ]
        conflicted.extend(
            sequence
            for sequence, hashes in state.full_hashes.items()
            if len(hashes) > 1
        )
        return sorted(conflicted)

    def can_skip_device(
        self,
        provider: str,
        partition: str,
        device: str,
        min_cloud_cover: int,
    ) -> bool:
        """Return whether ``device`` cannot produce imagery with cloud cover at
        least ``min_cloud_cover``.

        Unknown cloud cover forces a query, as does any flagged partition or
        unregistered device. Explicitly not-applicable records are ignored.
        """
        if not isinstance(min_cloud_cover, int) or isinstance(min_cloud_cover, bool):
            raise ValueError("min_cloud_cover must be an int 0-100")
        if not 0 <= min_cloud_cover <= 100:
            raise ValueError(
                f"min_cloud_cover must be within 0-100, got {min_cloud_cover}"
            )
        if not self.is_partition_usable(provider, partition):
            return False
        state = self._require_partition(provider, partition)
        if device not in state.devices:
            return False
        histogram = self._working_histogram(state)
        states = histogram.get(device, {})
        if states.get(UNKNOWN, 0) > 0:
            return False
        return not any(
            isinstance(state_value, int) and state_value >= min_cloud_cover
            for state_value in states
        )

    def devices_to_query(
        self, provider: str, partition: str, min_cloud_cover: int
    ) -> frozenset[str]:
        """Registered devices that still need detail queries for the threshold."""
        state = self._require_partition(provider, partition)
        return frozenset(
            device
            for device in state.devices
            if not self.can_skip_device(provider, partition, device, min_cloud_cover)
        )

    def _working_histogram(
        self, state: _PartitionState
    ) -> dict[str, dict[CloudCoverState, int]]:
        histogram: dict[str, dict[CloudCoverState, int]] = deepcopy(state.sealed)
        for sequence in sorted(state.correction_versions):
            versions = state.correction_versions[sequence]
            # Usable partitions have exactly one version per sequence.
            correction = next(iter(versions.values()))
            for entry in correction.entries:
                before = histogram.setdefault(entry.before_device, {})
                count = before.get(entry.before_state, 0)
                if count <= 0:
                    raise ValueError(
                        f"Correction {correction.sequence} references a record "
                        f"in state {entry.before_state!r} on device "
                        f"{entry.before_device!r} that is not present"
                    )
                before[entry.before_state] = count - 1
                if before[entry.before_state] == 0:
                    del before[entry.before_state]
                if not entry.is_withdrawal:
                    after = histogram.setdefault(entry.after_device, {})
                    after[entry.after_state] = after.get(entry.after_state, 0) + 1
        return histogram

    def merge(self, other: ShardSummaries) -> ShardSummaries:
        """Merge another replica's shard data into this one in place.

        The result depends only on the set of files each replica saw, not on
        the merge order or on whether replicas were saved and reloaded.
        """
        for (provider, partition), other_state in other._partitions.items():
            self.register_partition(provider, partition, other_state.devices)
            state = self._require_partition(provider, partition)
            # Replicas that were saved and reloaded retain only the sealed
            # histogram and the newest full sequence, so adopt the newest
            # complete snapshot directly rather than replaying per-scene data.
            if other_state.full_sequence is not None and (
                state.full_sequence is None
                or other_state.full_sequence > state.full_sequence
            ):
                state.full_sequence = other_state.full_sequence
                state.full_hashes = deepcopy(other_state.full_hashes)
                state.full_versions = deepcopy(other_state.full_versions)
                state.sealed = deepcopy(other_state.sealed)
                state.correction_versions = {
                    sequence: versions
                    for sequence, versions in state.correction_versions.items()
                    if sequence > state.full_sequence
                }
            elif other_state.full_sequence == state.full_sequence:
                # Same-sequence full summaries seen by different replicas may
                # diverge; merge their constant-size content hashes.
                for sequence, hashes in other_state.full_hashes.items():
                    state.full_hashes.setdefault(sequence, set()).update(hashes)
            for versions in other_state.full_versions.values():
                for summary in versions.values():
                    self.apply_full(summary)
            for versions in other_state.correction_versions.values():
                for correction in versions.values():
                    self.apply_correction(correction)
        return self

    def to_dict(self) -> dict[str, Any]:
        partitions: list[dict[str, Any]] = []
        for (provider, partition), state in sorted(self._partitions.items()):
            sealed = [
                {
                    "device": device,
                    "cloud_cover": [
                        {"state": state_value, "count": count}
                        for state_value, count in sorted(
                            states.items(), key=lambda item: str(item[0])
                        )
                    ],
                }
                for device, states in sorted(state.sealed.items())
            ]
            partition_dict: dict[str, Any] = {
                "provider": provider,
                "partition": partition,
                "devices": sorted(state.devices),
                "sealed": sealed,
                "corrections": [
                    {
                        "sequence": sequence,
                        "versions": [
                            correction.to_dict()
                            for correction in sorted(
                                versions.values(), key=lambda c: c.canonical()
                            )
                        ],
                    }
                    for sequence, versions in sorted(state.correction_versions.items())
                ],
            }
            if state.full_sequence is not None:
                partition_dict["full_sequence"] = state.full_sequence
                partition_dict["full_hashes"] = [
                    {
                        "sequence": sequence,
                        "hashes": sorted(hashes),
                    }
                    for sequence, hashes in sorted(state.full_hashes.items())
                ]
                # Uncontested full summaries are folded into the histogram and
                # need no per-scene retention. Divergent versions are retained
                # so the conflicting payloads can still be inspected.
                if any(len(hashes) > 1 for hashes in state.full_hashes.values()):
                    partition_dict["full_versions"] = [
                        {
                            "sequence": sequence,
                            "versions": [
                                summary.to_dict()
                                for summary in sorted(
                                    versions.values(),
                                    key=lambda s: s.canonical(),
                                )
                            ],
                        }
                        for sequence, versions in sorted(state.full_versions.items())
                    ]
            partitions.append(partition_dict)
        return {"partitions": partitions}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ShardSummaries:
        summaries = cls()
        for partition_data in d.get("partitions", []):
            provider = partition_data["provider"]
            partition = partition_data["partition"]
            summaries.register_partition(provider, partition, partition_data["devices"])
            state = summaries._require_partition(provider, partition)
            full_versions = partition_data.get("full_versions", [])
            if full_versions:
                # Conflicting full summaries are retained verbatim and replay
                # through the normal merge logic, rebuilding content hashes.
                for full in sorted(full_versions, key=lambda item: item["sequence"]):
                    for version in full["versions"]:
                        summaries.apply_full(FullShardSummary.from_dict(version))
            elif "full_sequence" in partition_data:
                # Sealed data persists as a histogram; no per-scene records.
                state.full_sequence = partition_data["full_sequence"]
                state.full_versions = {}
                state.full_hashes = {
                    item["sequence"]: set(item["hashes"])
                    for item in partition_data.get("full_hashes", [])
                }
            for device_data in partition_data.get("sealed", []):
                states = {
                    _decode_state(entry["state"]): entry["count"]
                    for entry in device_data["cloud_cover"]
                }
                state.sealed[device_data["device"]] = states
            for correction in sorted(
                partition_data.get("corrections", []),
                key=lambda item: item["sequence"],
            ):
                for version in correction["versions"]:
                    summaries.apply_correction(ShardCorrection.from_dict(version))
        return summaries

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ShardSummaries):
            return NotImplemented
        return self.to_dict() == other.to_dict()
