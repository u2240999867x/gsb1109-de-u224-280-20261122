from decimal import Decimal

import pytest

xmlschema = pytest.importorskip("xmlschema")
from xmlschema import XMLSchema11
from xmlschema.pending_extensions import PendingExtensionLedger


CORE_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="urn:core" xmlns:core="urn:core"
           elementFormDefault="qualified">
  <xs:element name="reading" abstract="true" type="xs:decimal"/>
  <xs:element name="knownReading" substitutionGroup="core:reading"
              type="xs:decimal"/>
</xs:schema>
"""

MAIN_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:core="urn:core" targetNamespace="urn:main"
           elementFormDefault="qualified">
  <xs:import namespace="urn:core" schemaLocation="core.xsd"/>
  <xs:element name="batch">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="record" maxOccurs="unbounded">
          <xs:complexType>
            <xs:openContent mode="interleave">
              <xs:any namespace="##other" processContents="lax" {extra}/>
            </xs:openContent>
            <xs:sequence>
              <xs:element name="id" type="xs:string"/>
              <xs:element ref="core:reading"/>
              <xs:element name="end" type="xs:string"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""

EXT_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:core="urn:core" targetNamespace="urn:ext"
           elementFormDefault="qualified">
  <xs:import namespace="urn:core" schemaLocation="core.xsd"/>
  <xs:element name="measure" substitutionGroup="core:reading"
              type="xs:decimal"/>
</xs:schema>
"""


def write_schemas(tmp_path, extra=""):
    (tmp_path / "core.xsd").write_text(CORE_XSD)
    main_path = tmp_path / "main.xsd"
    main_path.write_text(MAIN_XSD.format(extra=extra))
    ext_path = tmp_path / "ext.xsd"
    ext_path.write_text(EXT_XSD)
    return main_path, ext_path


def two_record_xml(first_value="7.50"):
    return f"""
    <batch xmlns="urn:main" xmlns:core="urn:core" xmlns:ext="urn:ext">
      <record>
        <id>R1</id>
        <ext:measure>{first_value}</ext:measure>
        <end>stop</end>
      </record>
      <record>
        <id>R2</id>
        <core:knownReading>2</core:knownReading>
        <ext:measure>7.50</ext:measure>
        <end>stop</end>
      </record>
    </batch>
    """


def test_lazy_pending_extension_backfill_and_idempotence(tmp_path):
    main_path, ext_path = write_schemas(tmp_path)
    schema = XMLSchema11(str(main_path))
    ledger = PendingExtensionLedger(schema)

    outputs = list(ledger.stream(two_record_xml(), path="record"))

    assert any(not isinstance(item, xmlschema.XMLSchemaValidationError)
               for item in outputs)
    assert [record.record_id for record in ledger.records] == ["R1", "R2"]
    assert len(ledger.pending) == 2

    first, second = ledger.pending
    assert first.record_id == "R1"
    assert first.slot_candidate is True
    assert first.original_position == 2
    assert first.raw_text == "7.50"
    assert first.element is not None
    assert [issue.code for issue in ledger.active_issues("R1")] == ["missing_slot"]

    assert second.record_id == "R2"
    assert second.slot_candidate is False
    assert second.original_position == 3
    assert ledger.active_issues("R2") == []

    result = ledger.supply_schema(str(ext_path))

    assert len(result) == 2
    by_record = {item.record_id: item for item in result}
    assert by_record["R1"].status == "fixed_slot"
    assert by_record["R1"].placement == "fixed_slot"
    assert by_record["R1"].value == Decimal("7.50")
    assert "substitution-group" in by_record["R1"].rule
    assert ledger.active_issues("R1") == []

    assert by_record["R2"].status == "open_content"
    assert by_record["R2"].placement == "open_content"
    assert by_record["R2"].value == Decimal("7.50")

    deliveries = ledger.drain_deliveries()
    assert {(item.record_id, item.placement) for item in deliveries} == {
        ("R1", "fixed_slot"), ("R2", "open_content")
    }
    assert ledger.drain_deliveries() == []

    duplicate = ledger.supply_schema(str(ext_path))
    assert duplicate.duplicate is True
    assert len(duplicate) == 0
    assert ledger.drain_deliveries() == []
    assert ledger.active_issues() == []

    snapshot_repr = repr(ledger.snapshot())
    assert "stop" not in snapshot_repr
    assert "7.50" in snapshot_repr


def test_defined_qname_is_rejected_by_restricted_wildcard(tmp_path):
    main_path, ext_path = write_schemas(tmp_path, 'notQName="##defined"')
    schema = XMLSchema11(str(main_path))
    ledger = PendingExtensionLedger(schema)

    list(ledger.stream(two_record_xml(), path="record"))
    result = ledger.supply_schema(str(ext_path))
    by_record = {item.record_id: item for item in result}

    assert by_record["R1"].status == "fixed_slot"
    assert by_record["R1"].value == Decimal("7.50")
    assert ledger.active_issues("R1") == []

    second = by_record["R2"]
    assert second.status == "rejected"
    assert second.placement == "open_content"
    assert "##defined" in second.rule
    issues = ledger.active_issues("R2")
    assert len(issues) == 1
    assert issues[0].code == "wildcard_rejected"


def test_invalid_value_and_unknown_extension_remain_auditable(tmp_path):
    main_path, ext_path = write_schemas(tmp_path)
    schema = XMLSchema11(str(main_path))
    ledger = PendingExtensionLedger(schema)
    xml = """
    <batch xmlns="urn:main" xmlns:ext="urn:ext" xmlns:other="urn:other">
      <record>
        <id>R1</id>
        <ext:measure>bad</ext:measure>
        <other:note>keep raw</other:note>
        <end>stop</end>
      </record>
    </batch>
    """

    list(ledger.stream(xml, path="record"))
    result = ledger.supply_schema(str(ext_path))
    measure = result[0]

    assert measure.status == "invalid"
    assert measure.raw_text == "bad"
    assert any(issue.code == "type_error" for issue in ledger.active_issues("R1"))
    assert any(issue.code == "missing_slot" and not issue.active
               for issue in ledger.issues_for_record("R1"))

    pending = ledger.pending
    assert len(pending) == 1
    assert pending[0].local_name == "note"
    assert pending[0].raw_text == "keep raw"


def test_continued_lazy_read_uses_received_declaration(tmp_path):
    main_path, ext_path = write_schemas(tmp_path)
    schema = XMLSchema11(str(main_path))
    ledger = PendingExtensionLedger(schema)
    xml = """
    <batch xmlns="urn:main" xmlns:core="urn:core" xmlns:ext="urn:ext">
      <record><id>R1</id><ext:measure>7.50</ext:measure><end>stop</end></record>
      <record><id>R2</id><ext:measure>8.25</ext:measure><end>stop</end></record>
    </batch>
    """

    stream = ledger.stream(xml, path="record")
    first_data = None
    for item in stream:
        if isinstance(item, dict):
            first_data = item
            break

    assert first_data["ext:measure"] == "7.50"
    ledger.supply_schema(str(ext_path))

    remaining = list(stream)
    second_data = next(item for item in remaining if isinstance(item, dict))
    assert second_data["ext:measure"] == Decimal("8.25")
    r1_measure = next(item for item in ledger.occurrences if item.record_id == "R1")
    assert r1_measure.status == "fixed_slot"
    assert ledger.pending == []
