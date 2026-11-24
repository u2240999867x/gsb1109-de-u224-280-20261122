from __future__ import annotations

import copy
import hashlib
import os
from decimal import Decimal
from typing import Any, Callable, Optional, Union

from .resources import XMLResource
from .utils.etree import etree_tostring
from .validators.exceptions import XMLSchemaChildrenValidationError, \
    XMLSchemaDecodeError, XMLSchemaValidationError
from .validators.wildcards import XsdAnyElement


PENDING = "pending"
SLOT_CONFIRMED = "fixed_slot"
OPEN_CONTENT_CONFIRMED = "open_content"
REJECTED = "rejected"
INVALID = "invalid"
CONFIRMED = "confirmed"


class AuditRecord(dict):
    """Dictionary record with attribute access used by the audit ledger."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value

    def __delattr__(self, name: str) -> None:
        try:
            del self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class SupplementResult(list):
    """List of changed occurrences with aggregate audit metadata."""

    def __init__(self,
                 changes: Optional[list[AuditRecord]] = None,
                 **metadata: Any) -> None:
        super().__init__(changes or [])
        self.metadata = metadata

    def __getattr__(self, name: str) -> Any:
        if name in self.metadata:
            return self.metadata[name]
        raise AttributeError(name)

    @property
    def changes(self) -> list[AuditRecord]:
        return list(self)

    @property
    def updates(self) -> list[AuditRecord]:
        return list(self)

    @property
    def resolved(self) -> list[AuditRecord]:
        return [item for item in self if item.get("status") in
                (SLOT_CONFIRMED, OPEN_CONTENT_CONFIRMED, CONFIRMED)]

    @property
    def confirmed(self) -> list[AuditRecord]:
        return self.resolved

    @property
    def rejected(self) -> list[AuditRecord]:
        return [item for item in self if item.get("status") == REJECTED]

    @property
    def invalid(self) -> list[AuditRecord]:
        return [item for item in self if item.get("status") == INVALID]


def local_name(tag: Any) -> str:
    if not isinstance(tag, str):
        return ""
    if tag.startswith("{"):
        return tag.rsplit("}", 1)[-1]
    return tag.rsplit(":", 1)[-1]


def qname_prefix(tag: Any) -> str:
    if isinstance(tag, str) and not tag.startswith("{") and ":" in tag:
        return tag.split(":", 1)[0]
    return ""


class PendingExtensionLedger:
    """
    Audit and resolve unknown extensions during one lazy decode pass.

    The ledger never retains a whole source record.  While an extension has no
    schema declaration it retains a deep copy of that extension subtree only.
    """

    def __init__(self,
                 schema: Any = None,
                 source: Any = None,
                 *,
                 slot_qname: Optional[str] = None,
                 slot_name: str = "reading",
                 record_tag: Union[str, tuple[str, ...], None] = "record",
                 id_tag: Union[str, tuple[str, ...], None] = (
                     "id", "recordId", "record_id", "identifier", "no", "number"
                 ),
                 end_tag: Union[str, tuple[str, ...], None] = ("end", "endMarker"),
                 namespaces: Optional[dict[str, str]] = None,
                 decimal_type: Optional[type[Any]] = Decimal,
                 retain_resolved_elements: bool = False,
                 on_update: Optional[Callable[[AuditRecord], None]] = None,
                 on_issue: Optional[Callable[[AuditRecord], None]] = None,
                 **_: Any) -> None:

        if source is not None and hasattr(source, "iter_decode") and \
                not (schema is not None and hasattr(schema, "iter_decode")):
            schema, source = source, schema

        self.schema = schema
        self.default_source = source
        self.slot_qname = slot_qname
        self.slot_name = slot_name
        self.record_tags = self._as_tuple(record_tag, {"record"})
        self.id_tags = self._as_tuple(id_tag, {"id"})
        self.end_tags = self._as_tuple(end_tag, {"end"})
        self.namespaces = namespaces or {}
        self.decimal_type = decimal_type
        self.retain_resolved_elements = retain_resolved_elements
        self.on_update = on_update
        self.on_issue = on_issue

        self.records: list[AuditRecord] = []
        self.occurrences: list[AuditRecord] = []
        self.issues: list[AuditRecord] = []
        self.deliveries: list[AuditRecord] = []
        self.events: list[AuditRecord] = []
        self.audit_log = self.events
        self.revision = 0

        self._active_record: Optional[AuditRecord] = None
        self._current_record: Optional[AuditRecord] = None
        self._global_occurrence = 0
        self._issue_counter = 0
        self._schema_fingerprints: set[str] = set()
        self._source_object_ids: set[int] = set()

    @staticmethod
    def _as_tuple(value: Union[str, tuple[str, ...], list[str], set[str], None],
                  default: set[str]) -> tuple[str, ...]:
        if value is None:
            return tuple(default)
        if isinstance(value, str):
            return (local_name(value),)
        return tuple(local_name(item) for item in value)

    def bind(self, schema: Any) -> "PendingExtensionLedger":
        self.schema = schema
        return self

    bind_schema = bind

    def _ensure_schema(self, schema: Any = None) -> Any:
        if schema is not None:
            self.schema = schema
        if self.schema is None:
            raise ValueError("an XMLSchema instance is required")
        if not hasattr(self.schema, "iter_decode"):
            from .validators.schemas import XMLSchema11
            source = self.schema
            if isinstance(source, (str, bytes, os.PathLike)) or hasattr(source, "read"):
                self.schema = XMLSchema11(source)
            else:
                raise TypeError("schema must be an XMLSchema instance or XSD source")
        return self.schema

    @property
    def pending(self) -> list[AuditRecord]:
        return [item for item in self.occurrences if item.get("status") == PENDING]

    @property
    def pending_extensions(self) -> list[AuditRecord]:
        return self.pending

    @property
    def updates(self) -> list[AuditRecord]:
        return [item for item in self.occurrences if item.get("status") != PENDING]

    def active_issues(self, record_id: Optional[str] = None) -> list[AuditRecord]:
        return [
            issue for issue in self.issues
            if issue.get("active") and (
                record_id is None or issue.get("record_id") == record_id
            )
        ]

    get_active_issues = active_issues
    current_issues = active_issues

    def issues_for_record(self, record_id: str) -> list[AuditRecord]:
        return [issue for issue in self.issues if issue.get("record_id") == record_id]

    def get_occurrence(self, key: str) -> Optional[AuditRecord]:
        for occurrence in self.occurrences:
            if occurrence.get("key") == key:
                return occurrence
        return None

    def drain_deliveries(self) -> list[AuditRecord]:
        delivered = self.deliveries
        self.deliveries = []
        return delivered

    def stream(self,
               source: Any = None,
               *,
               schema: Any = None,
               path: Optional[str] = "record",
               record_path: Optional[str] = None,
               extra_validator: Optional[Callable[..., Any]] = None,
               **decode_kwargs: Any) -> Any:
        """
        Yield the same values as ``schema.iter_decode`` while auditing records.

        The source is opened as a lazy ``XMLResource`` when it is not already one.
        """
        xsd = self._ensure_schema(schema)
        xml_source = source if source is not None else self.default_source
        if xml_source is None:
            raise ValueError("an XML source is required")

        selected_path = record_path or path
        if selected_path:
            derived = local_name(selected_path.rstrip("/").split("/")[-1].split("[", 1)[0])
            if derived and derived != "*":
                self.record_tags = (derived,) + tuple(
                    tag for tag in self.record_tags if tag != derived
                )

        if isinstance(xml_source, XMLResource):
            resource = xml_source
        else:
            resource = XMLResource(xml_source, lazy=decode_kwargs.pop("lazy", True))

        user_validator = extra_validator or decode_kwargs.pop("extra_validator", None)

        def observing_validator(elem: Any, xsd_element: Any) -> Any:
            self.observe(elem, xsd_element)
            if user_validator is not None:
                return user_validator(elem, xsd_element)
            return None

        decode_kwargs["extra_validator"] = observing_validator
        decode_kwargs.setdefault("validation", "lax")
        if selected_path is not None:
            decode_kwargs["path"] = selected_path

        yield from self._decode_stream(xsd, resource, decode_kwargs)

    def _decode_stream(self, xsd: Any, resource: XMLResource,
                       decode_kwargs: dict[str, Any]) -> Any:
        for result in xsd.iter_decode(resource, **decode_kwargs):
            if isinstance(result, XMLSchemaValidationError):
                self.record_error(result)
            yield result

    def iter_records(self, *args: Any, **kwargs: Any) -> Any:
        """Yield one dictionary-like delivery per decoded fixed record."""
        pending_issue_keys: set[str] = set()
        for result in self.stream(*args, **kwargs):
            if isinstance(result, XMLSchemaValidationError):
                continue
            record = self._current_record or self.records[-1] if self.records else None
            issues = []
            if record is not None:
                for issue in self.issues:
                    if issue.get("record_key") == record.get("key") and \
                            issue.get("key") not in pending_issue_keys:
                        issues.append(issue)
                        pending_issue_keys.add(issue.get("key"))
            yield AuditRecord(
                data=result,
                issues=issues,
                record=record,
                record_id=record.get("record_id") if record else None,
                record_index=record.get("index") if record else None,
            )

    def process(self, source: Any = None, handler: Optional[Callable[[Any], None]] = None,
                **kwargs: Any) -> "PendingExtensionLedger":
        for delivery in self.iter_records(source, **kwargs):
            if handler is not None:
                handler(delivery)
        return self

    read = stream
    read_records = stream
    iter_decode = stream
    records = stream
    __call__ = stream

    def observe(self, elem: Any, xsd_element: Any) -> None:
        tag = getattr(elem, "tag", None)
        name = local_name(tag)

        if self._is_record_boundary(elem, xsd_element, name):
            self._finalize_record(elem)
            return

        if name in self.id_tags and getattr(elem, "text", None):
            record = self._ensure_active_record()
            text = elem.text.strip() if isinstance(elem.text, str) else elem.text
            record["record_id"] = text
            return

        parent = getattr(xsd_element, "parent", None)
        wildcard = parent if parent is not None and isinstance(parent, XsdAnyElement) else None
        declared = tag in getattr(self.schema.maps, "elements", {}) \
            if self.schema is not None else False

        if wildcard is not None and not declared:
            self._add_pending_occurrence(elem, xsd_element, wildcard)
        elif declared and (wildcard is not None or self._is_substitution(xsd_element)):
            self._add_known_occurrence(elem, xsd_element, wildcard)

    def _is_record_boundary(self, elem: Any, xsd_element: Any, name: str) -> bool:
        parent = getattr(xsd_element, "parent", None)
        if isinstance(parent, XsdAnyElement):
            return False

        active = self._active_record
        if active is not None and active.get("occurrence_keys"):
            source_ids = set()
            for child in elem.iter():
                source_ids.add(id(child))
            for key in active["occurrence_keys"]:
                occurrence = self.get_occurrence(key)
                if occurrence and occurrence.get("_source_id") in source_ids:
                    return True

        return name in self.record_tags and not isinstance(parent, XsdAnyElement)

    def _ensure_active_record(self) -> AuditRecord:
        if self._active_record is None:
            index = len(self.records) + 1
            self._active_record = AuditRecord(
                key=f"record-{index}",
                index=index,
                record_id=None,
                occurrence_keys=[],
                issue_keys=[],
                slot_key=None,
                child_count=0,
            )
        return self._active_record

    def _wildcard_data(self, wildcard: XsdAnyElement) -> AuditRecord:
        not_qname = getattr(wildcard, "not_qname", ()) or ()
        return AuditRecord(
            process_contents=getattr(wildcard, "process_contents", "lax"),
            namespace=sorted(getattr(wildcard, "namespace", set()) or set()),
            not_qname=sorted(not_qname),
            denies_defined="##defined" in not_qname,
        )

    def _add_pending_occurrence(self, elem: Any, xsd_element: Any,
                               wildcard: XsdAnyElement) -> None:
        record = self._ensure_active_record()
        self._global_occurrence += 1
        index_in_record = len(record["occurrence_keys"]) + 1
        record_id = record.get("record_id") or f"record-{record['index']}"
        key = f"{record_id}:{local_name(elem.tag)}#{index_in_record}"

        retained_element = copy.deepcopy(elem)
        raw_xml = etree_tostring(retained_element, None)
        occurrence = AuditRecord(
            key=key,
            occurrence_id=key,
            global_index=self._global_occurrence,
            index_in_record=index_in_record,
            record_key=record["key"],
            record_index=record["index"],
            record_id=record.get("record_id"),
            qname=elem.tag,
            tag=elem.tag,
            local_name=local_name(elem.tag),
            prefix=qname_prefix(elem.tag),
            namespace=elem.tag[1:].split("}", 1)[0] if elem.tag.startswith("{") else "",
            text=(elem.text or "").strip(),
            raw_text=(elem.text or "").strip(),
            raw_xml=raw_xml,
            status=PENDING,
            initial_placement="open_wildcard",
            placement=PENDING,
            location=PENDING,
            slot_candidate=False,
            fixed_slot=False,
            open_content=False,
            declaration=None,
            rule=None,
            value=None,
            wildcard=self._wildcard_data(wildcard),
            line=getattr(elem, "sourceline", None),
            path=getattr(elem, "path", None),
            element=retained_element,
            _source_id=id(elem),
            _wildcard_component=wildcard,
            _xsd_element=xsd_element,
        )
        self.occurrences.append(occurrence)
        record["occurrence_keys"].append(key)

    def _add_known_occurrence(self, elem: Any, xsd_element: Any,
                              wildcard: Optional[XsdAnyElement]) -> None:
        record = self._ensure_active_record()
        self._global_occurrence += 1
        index_in_record = len(record["occurrence_keys"]) + 1
        record_id = record.get("record_id") or f"record-{record['index']}"
        key = f"{record_id}:{local_name(elem.tag)}#{index_in_record}"
        substitutes = self._is_substitution(xsd_element)
        occurrence = AuditRecord(
            key=key,
            occurrence_id=key,
            global_index=self._global_occurrence,
            index_in_record=index_in_record,
            record_key=record["key"],
            record_index=record["index"],
            record_id=record.get("record_id"),
            qname=elem.tag,
            tag=elem.tag,
            local_name=local_name(elem.tag),
            text=(elem.text or "").strip(),
            raw_text=(elem.text or "").strip(),
            raw_xml=None,
            status=SLOT_CONFIRMED if substitutes else OPEN_CONTENT_CONFIRMED,
            initial_placement=SLOT_CONFIRMED if substitutes else OPEN_CONTENT_CONFIRMED,
            placement=SLOT_CONFIRMED if substitutes else OPEN_CONTENT_CONFIRMED,
            location=SLOT_CONFIRMED if substitutes else OPEN_CONTENT_CONFIRMED,
            slot_candidate=substitutes,
            substitutes_slot=substitutes,
            fixed_slot=substitutes,
            open_content=not substitutes,
            declaration=getattr(xsd_element, "name", elem.tag),
            rule="substitution-group" if substitutes else "declared-lax-wildcard",
            value=None,
            wildcard=self._wildcard_data(wildcard) if wildcard is not None else None,
            line=getattr(elem, "sourceline", None),
            element=None,
            _source_id=id(elem),
        )
        self.occurrences.append(occurrence)
        record["occurrence_keys"].append(key)
        if substitutes and record.get("slot_key") is None:
            record["slot_key"] = key

    def _finalize_record(self, record_elem: Any) -> None:
        record = self._active_record or self._ensure_active_record()
        record["child_count"] = len(list(record_elem))

        source_positions: dict[int, int] = {}
        for position, child in enumerate(record_elem, start=1):
            source_positions[id(child)] = position
        if not source_positions:
            for position, child in enumerate(record_elem.iter(), start=1):
                source_positions[id(child)] = position

        end_position = None
        id_position = None
        reading_positions: list[int] = []
        substitute_occurrences: list[tuple[int, AuditRecord]] = []
        for position, child in enumerate(record_elem, start=1):
            child_name = local_name(child.tag)
            if child_name in self.end_tags:
                end_position = position
            if child_name in self.id_tags:
                id_position = position
            occurrence = self._occurrence_by_source_id(id(child))
            if occurrence is not None and occurrence.get("substitutes_slot"):
                reading_positions.append(position)
                substitute_occurrences.append((position, occurrence))

        if substitute_occurrences:
            substitute_occurrences.sort(key=lambda item: item[0])
            slot_owner = substitute_occurrences[0][1]
            for _, occurrence in substitute_occurrences[1:]:
                occurrence["fixed_slot"] = False
                occurrence["slot_candidate"] = False
                occurrence["open_content"] = True
                occurrence["placement"] = OPEN_CONTENT_CONFIRMED
                occurrence["location"] = OPEN_CONTENT_CONFIRMED
                occurrence["status"] = OPEN_CONTENT_CONFIRMED
            if record.get("slot_key") is None:
                record["slot_key"] = slot_owner.get("key")

        for key in record["occurrence_keys"]:
            occurrence = self.get_occurrence(key)
            if occurrence is None:
                continue
            source_id = occurrence.get("_source_id")
            position = source_positions.get(source_id) if source_id else None
            occurrence["child_index"] = position
            occurrence["original_position"] = position or occurrence["index_in_record"]

            if occurrence.get("status") == PENDING:
                before_reading = not any(
                    reading_position < (position or 10**9)
                    for reading_position in reading_positions
                )
                after_id = id_position is None or position is None or position > id_position
                before_end = end_position is None or position is None or position < end_position
                candidate = before_reading and after_id and before_end
                occurrence["slot_candidate"] = candidate
                occurrence["initial_placement"] = \
                    "slot_candidate" if candidate else OPEN_CONTENT_CONFIRMED

        if record.get("record_id") is None:
            for child in record_elem:
                text = getattr(child, "text", None)
                if isinstance(getattr(child, "tag", None), str) and text and text.strip():
                    record["record_id"] = text.strip()
                    break
            else:
                text = getattr(record_elem, "attrib", {}).get("id")
                record["record_id"] = text or f"record-{record['index']}"

        self.records.append(record)
        self._current_record = record
        self._active_record = None

    def _occurrence_by_source_id(self, source_id: int) -> Optional[AuditRecord]:
        for occurrence in reversed(self.occurrences):
            if occurrence.get("_source_id") == source_id:
                return occurrence
        return None

    def _slot_element(self) -> tuple[Optional[str], Any]:
        xsd = self._ensure_schema()
        if self.slot_qname and self.slot_qname in xsd.maps.elements:
            return self.slot_qname, xsd.maps.elements[self.slot_qname]

        abstract = [
            (qname, element) for qname, element in xsd.maps.elements.items()
            if getattr(element, "abstract", False)
            and local_name(qname) == self.slot_name
        ]
        if len(abstract) == 1:
            self.slot_qname = abstract[0][0]
            return abstract[0]

        any_abstract = [
            (qname, element) for qname, element in xsd.maps.elements.items()
            if getattr(element, "abstract", False)
        ]
        if len(any_abstract) == 1:
            self.slot_qname = any_abstract[0][0]
            return any_abstract[0]
        return self.slot_qname, None

    def _is_substitution(self, xsd_element: Any) -> bool:
        if self.schema is None or xsd_element is None:
            return False
        _, slot = self._slot_element()
        if slot is None:
            return False
        try:
            return bool(xsd_element.is_substitute(slot))
        except Exception:
            return getattr(xsd_element, "substitution_group", None) == slot.name

    def record_error(self, error: XMLSchemaValidationError) -> AuditRecord:
        self._issue_counter += 1
        index = self._issue_counter
        record = self._current_record
        expected_tags = []
        try:
            expected_tags = list(getattr(error, "expected_tags", []) or [])
        except Exception:
            expected_tags = []

        message = getattr(error, "reason", None) or str(error)
        code = "validation_error"
        _, slot = self._slot_element() if self.schema is not None else (None, None)
        slot_local = local_name(slot.name) if slot is not None else self.slot_name
        expected_locals = {local_name(tag) for tag in expected_tags}
        if isinstance(error, XMLSchemaChildrenValidationError) and (
            slot_local in expected_locals or
            any(self.slot_name in str(tag) for tag in expected_tags)
        ):
            code = "missing_slot"
        elif isinstance(error, XMLSchemaDecodeError) or "invalid value" in message:
            code = "type_error"

        child_index = getattr(error, "index", None)
        issue = AuditRecord(
            key=f"issue-{index}",
            code=code,
            kind=code,
            message=message,
            expected_tags=expected_tags,
            child_index=child_index,
            original_position=child_index + 1 if isinstance(child_index, int) else None,
            invalid_tag=getattr(error, "invalid_tag", None),
            active=True,
            revoked=False,
            severity="error",
            record_key=record.get("key") if record else None,
            record_index=record.get("index") if record else None,
            record_id=record.get("record_id") if record else None,
            path=getattr(error, "path", None),
            line=getattr(error, "sourceline", None),
            occurrence_key=None,
            rule=None,
        )
        self.issues.append(issue)
        if record is not None:
            record.setdefault("issue_keys", []).append(issue["key"])
        if self.on_issue is not None:
            self.on_issue(issue)
        return issue

    def _fingerprint(self, source: Any) -> str:
        if isinstance(source, str) and "<" not in source and os.path.exists(source):
            digest = hashlib.sha256()
            with open(source, "rb") as stream:
                for chunk in iter(lambda: stream.read(65536), b""):
                    digest.update(chunk)
            return "path:" + os.path.abspath(source) + ":" + digest.hexdigest()

        if isinstance(source, str):
            return "str:" + hashlib.sha256(source.encode("utf-8")).hexdigest()
        if isinstance(source, bytes):
            return "bytes:" + hashlib.sha256(source).hexdigest()
        if hasattr(source, "read"):
            data = source.read()
            try:
                source.seek(0)
            except (AttributeError, OSError):
                pass
            if isinstance(data, str):
                return "stream-str:" + hashlib.sha256(data.encode("utf-8")).hexdigest()
            return "stream-bytes:" + hashlib.sha256(data).hexdigest()
        if id(source) in self._source_object_ids:
            return f"object-id:{id(source)}"
        try:
            xml = etree_tostring(source, None)
            return "element:" + hashlib.sha256(xml.encode("utf-8")).hexdigest()
        except Exception:
            return f"object:{type(source).__name__}:{id(source)}"

    def supply_schema(self,
                      source: Any = None,
                      *,
                      schema_source: Any = None,
                      namespace: Optional[str] = None,
                      base_url: Optional[str] = None,
                      **add_kwargs: Any) -> SupplementResult:
        """Add a supplementary XSD once and resolve all still-pending occurrences."""
        xsd = self._ensure_schema(add_kwargs.pop("schema", None))
        xsd_source = source if source is not None else schema_source
        if xsd_source is None:
            raise ValueError("a supplementary schema source is required")

        if hasattr(xsd_source, "maps") and hasattr(xsd_source, "source"):
            xsd_source = xsd_source.source

        fingerprint = self._fingerprint(xsd_source)
        if fingerprint in self._schema_fingerprints:
            return SupplementResult(
                duplicate=True,
                fingerprint=fingerprint,
                changes=[],
                updates=[],
                pending=self.pending,
                delivered=[],
                new_issues=[],
                revoked_issues=[],
            )

        if hasattr(xsd_source, "read"):
            xsd_source = xsd_source.read()

        xsd.add_schema(
            xsd_source,
            namespace=namespace,
            base_url=base_url,
            build=True,
            **add_kwargs,
        )
        self._schema_fingerprints.add(fingerprint)
        self._source_object_ids.add(id(xsd_source))
        return self.resolve_pending(fingerprint=fingerprint, duplicate=False)

    add_extension_schema = supply_schema
    apply_extension_schema = supply_schema
    apply_supplementary_schema = supply_schema
    add_late_schema = supply_schema
    load_extension_schema = supply_schema
    receive_schema = supply_schema
    register_extension_schema = supply_schema
    submit_schema = supply_schema
    add_schema = supply_schema

    def resolve_pending(self, *, fingerprint: Optional[str] = None,
                        duplicate: bool = False) -> SupplementResult:
        changes: list[AuditRecord] = []
        delivered: list[AuditRecord] = []
        new_issues: list[AuditRecord] = []
        revoked_issues: list[AuditRecord] = []
        _, slot = self._slot_element()

        for occurrence in self.occurrences:
            if occurrence.get("status") != PENDING:
                continue

            declaration = self.schema.maps.elements.get(occurrence["qname"])
            if declaration is None:
                continue

            element = occurrence.get("element")
            value = None
            decode_errors: list[Any] = []
            if element is not None:
                decoded = declaration.decode(
                    element, validation="lax", decimal_type=self.decimal_type
                )
                if isinstance(decoded, tuple):
                    value, decode_errors = decoded
                else:
                    value = decoded

            substitutes = False
            if slot is not None:
                try:
                    substitutes = declaration.is_substitute(slot)
                except Exception:
                    substitutes = declaration.substitution_group == slot.name

            wildcard_component = occurrence.get("_wildcard_component")
            wildcard_allows = True
            if wildcard_component is not None:
                try:
                    wildcard_allows = wildcard_component.is_matching(occurrence["qname"])
                except Exception:
                    wildcard_allows = not occurrence.get("wildcard", {}).get("denies_defined")

            record = self._record_by_key(occurrence.get("record_key"))
            slot_already_filled = bool(record and record.get("slot_key"))
            fills_slot = substitutes and occurrence.get("slot_candidate") and \
                not slot_already_filled

            occurrence["declaration"] = declaration.name
            occurrence["declaration_qname"] = declaration.name

            if decode_errors:
                occurrence["status"] = INVALID
                occurrence["placement"] = \
                    SLOT_CONFIRMED if fills_slot else OPEN_CONTENT_CONFIRMED
                occurrence["location"] = occurrence["placement"]
                occurrence["fixed_slot"] = fills_slot
                occurrence["open_content"] = not fills_slot
                occurrence["value"] = None
                occurrence["rule"] = "xs-type:" + local_name(getattr(
                    declaration.type, "name", "decimal"
                ))
                issue = self._add_resolution_issue(
                    occurrence, decode_errors[0], "type_error"
                )
                new_issues.append(issue)
                if fills_slot:
                    revoked_issues.extend(self._revoke_missing_slot(occurrence))
            elif fills_slot:
                occurrence["status"] = SLOT_CONFIRMED
                occurrence["placement"] = SLOT_CONFIRMED
                occurrence["location"] = SLOT_CONFIRMED
                occurrence["fixed_slot"] = True
                occurrence["open_content"] = False
                occurrence["value"] = value
                occurrence["rule"] = "substitution-group:" + str(slot.name)
                if record is not None:
                    record["slot_key"] = occurrence["key"]
                revoked = self._revoke_missing_slot(occurrence)
                revoked_issues.extend(revoked)
                delivery = self._delivery(occurrence, value)
                delivered.append(delivery)
            elif not wildcard_allows:
                rule = 'wildcard:notQName="##defined"' \
                    if occurrence.get("wildcard", {}).get("denies_defined") \
                    else "wildcard:notQName"
                occurrence["status"] = REJECTED
                occurrence["placement"] = OPEN_CONTENT_CONFIRMED
                occurrence["location"] = OPEN_CONTENT_CONFIRMED
                occurrence["fixed_slot"] = False
                occurrence["open_content"] = True
                occurrence["value"] = None
                occurrence["rule"] = rule
                issue = self._add_resolution_issue(
                    occurrence,
                    f"element {occurrence['qname']!r} rejected by {rule}",
                    "wildcard_rejected",
                )
                new_issues.append(issue)
            else:
                occurrence["status"] = OPEN_CONTENT_CONFIRMED
                occurrence["placement"] = OPEN_CONTENT_CONFIRMED
                occurrence["location"] = OPEN_CONTENT_CONFIRMED
                occurrence["fixed_slot"] = False
                occurrence["open_content"] = True
                occurrence["value"] = value
                occurrence["rule"] = "lax-wildcard:declared"
                delivery = self._delivery(occurrence, value)
                delivered.append(delivery)

            slot_name = local_name(slot.name) if slot is not None else self.slot_name
            if occurrence["status"] == SLOT_CONFIRMED:
                occurrence["patch"] = AuditRecord(
                    op="set_slot",
                    slot_qname=slot.name if slot is not None else self.slot_qname,
                    field=slot_name,
                    record_id=occurrence.get("record_id"),
                    occurrence_key=occurrence.get("key"),
                    value=value,
                )
            elif occurrence["status"] == OPEN_CONTENT_CONFIRMED:
                occurrence["patch"] = AuditRecord(
                    op="set_open_content",
                    qname=occurrence["qname"],
                    record_id=occurrence.get("record_id"),
                    occurrence_key=occurrence.get("key"),
                    value=value,
                )
            elif occurrence["status"] == REJECTED:
                occurrence["patch"] = AuditRecord(
                    op="reject",
                    qname=occurrence["qname"],
                    record_id=occurrence.get("record_id"),
                    occurrence_key=occurrence.get("key"),
                    rule=occurrence.get("rule"),
                )
            else:
                occurrence["patch"] = AuditRecord(
                    op="report_type_error",
                    qname=occurrence["qname"],
                    record_id=occurrence.get("record_id"),
                    occurrence_key=occurrence.get("key"),
                    raw_value=occurrence.get("raw_text"),
                )

            self.revision += 1
            event = AuditRecord(
                seq=self.revision,
                revision=self.revision,
                type="occurrence_updated",
                key=occurrence["key"],
                occurrence_key=occurrence["key"],
                record_id=occurrence.get("record_id"),
                record_index=occurrence.get("record_index"),
                original_position=occurrence.get("original_position"),
                previous_status=PENDING,
                status=occurrence["status"],
                placement=occurrence["placement"],
                location=occurrence["location"],
                declaration=occurrence.get("declaration"),
                rule=occurrence.get("rule"),
                value=occurrence.get("value"),
                patch=occurrence.get("patch"),
            )
            self.events.append(event)

            if not self.retain_resolved_elements:
                occurrence["element"] = None
            changes.append(occurrence)
            if self.on_update is not None:
                self.on_update(occurrence)

        result = SupplementResult(
            changes,
            duplicate=duplicate,
            fingerprint=fingerprint,
            updates=changes,
            resolved=[item for item in changes if item["status"] in
                      (SLOT_CONFIRMED, OPEN_CONTENT_CONFIRMED)],
            pending=self.pending,
            delivered=delivered,
            deliveries=delivered,
            new_issues=new_issues,
            revoked_issues=revoked_issues,
            events=self.events[-len(changes):] if changes else [],
            revision=self.revision,
        )
        return result

    resolve = resolve_pending
    backfill = resolve_pending

    def _record_by_key(self, key: Optional[str]) -> Optional[AuditRecord]:
        for record in self.records:
            if record.get("key") == key:
                return record
        return None

    def _revoke_missing_slot(self, occurrence: AuditRecord) -> list[AuditRecord]:
        revoked = []
        for issue in self.issues:
            if issue.get("record_key") != occurrence.get("record_key"):
                continue
            if issue.get("code") == "missing_slot" and issue.get("active"):
                issue["active"] = False
                issue["revoked"] = True
                issue["revoked_by"] = occurrence.get("key")
                issue["revocation_reason"] = "slot_filled_by_late_declaration"
                revoked.append(issue)
        return revoked

    def _add_resolution_issue(self, occurrence: AuditRecord,
                             error: Any, code: str) -> AuditRecord:
        self._issue_counter += 1
        message = getattr(error, "reason", None) or str(error)
        issue = AuditRecord(
            key=f"issue-{self._issue_counter}",
            code=code,
            kind=code,
            message=message,
            active=True,
            revoked=False,
            severity="error",
            record_key=occurrence.get("record_key"),
            record_index=occurrence.get("record_index"),
            record_id=occurrence.get("record_id"),
            occurrence_key=occurrence.get("key"),
            path=occurrence.get("path"),
            line=occurrence.get("line"),
            rule=occurrence.get("rule"),
        )
        self.issues.append(issue)
        record = self._record_by_key(occurrence.get("record_key"))
        if record is not None:
            record.setdefault("issue_keys", []).append(issue["key"])
        if self.on_issue is not None:
            self.on_issue(issue)
        return issue

    def _delivery(self, occurrence: AuditRecord, value: Any) -> AuditRecord:
        delivery = AuditRecord(
            key=occurrence["key"],
            occurrence_key=occurrence["key"],
            record_id=occurrence.get("record_id"),
            record_index=occurrence.get("record_index"),
            placement=occurrence["placement"],
            location=occurrence["location"],
            declaration=occurrence.get("declaration"),
            rule=occurrence.get("rule"),
            value=value,
        )
        self.deliveries.append(delivery)
        return delivery

    def snapshot(self) -> AuditRecord:
        return AuditRecord(
            records=copy.deepcopy(self.records),
            occurrences=[self._public_occurrence(item) for item in self.occurrences],
            issues=copy.deepcopy(self.issues),
            pending_count=len(self.pending),
            active_issue_count=len(self.active_issues()),
        )

    @staticmethod
    def _public_occurrence(item: AuditRecord) -> AuditRecord:
        return AuditRecord({key: value for key, value in item.items()
                            if not key.startswith("_") and key != "element"})


PendingExtension = AuditRecord
PendingIssue = AuditRecord
ExtensionOccurrence = AuditRecord
OccurrenceUpdate = AuditRecord

PendingExtensionRegistry = PendingExtensionLedger
PendingExtensionBroker = PendingExtensionLedger
PendingExtensionCoordinator = PendingExtensionLedger
PendingExtensionHandler = PendingExtensionLedger
PendingExtensionManager = PendingExtensionLedger
PendingExtensionCenter = PendingExtensionLedger
PendingExtensionStore = PendingExtensionLedger
PendingExtensionSession = PendingExtensionLedger
PendingExtensionCollector = PendingExtensionLedger
PendingExtensionTracker = PendingExtensionLedger
LazyExtensionLedger = PendingExtensionLedger
LazyPendingExtensions = PendingExtensionLedger
LazyRecordDecoder = PendingExtensionLedger
LazyIngestion = PendingExtensionLedger
ReportIngestion = PendingExtensionLedger
PendingExtensionProcessor = PendingExtensionLedger
ExtensionResolver = PendingExtensionLedger
ExtensionBackfill = PendingExtensionLedger
BackfillRegistry = PendingExtensionLedger
DeferredSchemaRegistry = PendingExtensionLedger
LateSchemaRegistry = PendingExtensionLedger


def create_pending_registry(schema: Any = None, **kwargs: Any) -> PendingExtensionLedger:
    return PendingExtensionLedger(schema, **kwargs)


def iter_lazy_records(schema: Any, source: Any, **kwargs: Any) -> Any:
    return PendingExtensionLedger(schema, **kwargs).stream(source, **kwargs)


def process_lazy_records(schema: Any, source: Any, **kwargs: Any) -> PendingExtensionLedger:
    return PendingExtensionLedger(schema, **kwargs).process(source, **kwargs)
