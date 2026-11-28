# SPDX-License-Identifier: MIT

import pytest

import attr
import attrs

from attr import setters
from attr.setters import DeferredAssignment


CAP = 128


class TestCooperativeSetattr:
    def test_hook_runs_once_and_forwards_through_super(self, slots):
        seen = []

        class Ledger:
            def __setattr__(self, name, value):
                seen.append(("ledger", name, value))
                object.__setattr__(self, name, value)

        def hook(instance, attrib, value):
            seen.append(("hook", value))
            return value * 2

        @attr.define(
            cooperative_setattr=True,
            slots=slots,
            on_setattr=setters.NO_OP,
        )
        class Coop(Ledger):
            x = attr.field(default=1, on_setattr=hook)
            note = attr.field(default="")
            exempt = attr.field(default="", on_setattr=setters.NO_OP)

        coop = Coop()
        coop.x = 5

        assert coop.x == 10
        assert ("hook", 5) in seen
        assert ("ledger", "x", 10) in seen

        # Fields without hooks still travel down the same chain.
        coop.note = "hello"
        assert coop.note == "hello"
        assert ("ledger", "note", "hello") in seen

        # Explicit NO_OP fields are written without any hook as well.
        coop.exempt = "waived"
        assert coop.exempt == "waived"
        assert ("ledger", "exempt", "waived") in seen

    def test_defer_and_resume_with_confirmed_value(self, slots):
        hook_calls = []
        held = {}

        class Ledger:
            def __setattr__(self, name, value):
                object.__setattr__(self, name, value)

        def hook(instance, attrib, value):
            hook_calls.append(value)
            deferred = DeferredAssignment(value)
            held["deferred"] = deferred
            raise deferred

        @attr.define(
            cooperative_setattr=True,
            slots=slots,
            on_setattr=setters.NO_OP,
        )
        class Coop(Ledger):
            x = attr.field(default=0, on_setattr=hook)

        coop = Coop()
        coop.x = 9

        # The assignment is suspended: nothing changed and nothing escaped.
        assert coop.x == 0
        deferred = held["deferred"]
        assert deferred.deferred_value == 9
        assert deferred._continuation is not None
        assert hook_calls == [9]

        # Resume with the deferred value; the hook is not run a second time.
        deferred.resume()
        assert coop.x == 9
        assert hook_calls == [9]

        # Resume again with an explicitly confirmed value.
        deferred.resume(21)
        assert coop.x == 21
        assert hook_calls == [9]

    def test_defer_raised_below_is_caught_by_nearest_attrs_class(self, slots):
        held = {}

        class Middle:
            # The layer defers the first attempt only; the armed continuation
            # re-enters below the attrs class, so the second pass goes through.
            defer_next = True

            def __setattr__(self, name, value):
                if name == "x" and type(self).defer_next:
                    type(self).defer_next = False
                    deferred = DeferredAssignment(value)
                    held["deferred"] = deferred
                    raise deferred
                object.__setattr__(self, name, value)

        def passthrough(instance, attrib, value):
            return value

        @attr.define(
            cooperative_setattr=True,
            slots=slots,
            on_setattr=setters.NO_OP,
        )
        class Coop(Middle):
            x = attr.field(default=0, on_setattr=passthrough)

        coop = Coop()
        coop.x = 7

        assert coop.x == 0
        held["deferred"].resume()
        assert coop.x == 7

    def test_redeclared_field_keeps_subclass_rule(self, slots):
        stock_calls = []
        recount_calls = []

        class Ledger:
            def __setattr__(self, name, value):
                object.__setattr__(self, name, value)

        def stock_rule(instance, attrib, value):
            stock_calls.append(value)
            return value * 6

        def recount_rule(instance, attrib, value):
            recount_calls.append(value)
            return value * 8

        @attr.define(
            cooperative_setattr=True,
            slots=slots,
            on_setattr=setters.NO_OP,
        )
        class Stock(Ledger):
            pieces = attr.field(default=0, on_setattr=stock_rule)

        @attr.define(
            cooperative_setattr=True,
            slots=slots,
            on_setattr=setters.NO_OP,
        )
        class Recount(Stock):
            pieces = attr.field(default=0, on_setattr=recount_rule)

        recount = Recount()
        recount.pieces = 2

        assert recount.pieces == 16
        assert recount_calls == [2]
        # The parent's six-pieces-per-box rule never runs on the recount sheet.
        assert stock_calls == []

        # The parent class still applies its own rule on its own sheets.
        stock = Stock()
        stock.pieces = 2
        assert stock.pieces == 12
        assert stock_calls == [2]

    def test_generator_hook_keeps_documented_semantics(self, slots):
        log = []

        def gen_hook(instance, attrib, value):
            log.append(("pre", value))
            yield value * 3
            log.append(("post", value))

        @attr.define(
            cooperative_setattr=True,
            slots=slots,
            on_setattr=setters.NO_OP,
        )
        class Coop:
            x = attr.field(default=0, on_setattr=gen_hook)

        coop = Coop()
        coop.x = 4

        assert coop.x == 12
        assert log == [("pre", 4), ("post", 4)]

    def test_non_cooperative_class_propagates_deferral(self):
        def hook(instance, attrib, value):
            raise DeferredAssignment(value)

        @attr.define
        class Plain:
            x = attr.field(default=0, on_setattr=hook)

        plain = Plain()
        with pytest.raises(DeferredAssignment) as exc_info:
            plain.x = 2

        assert exc_info.value.deferred_value == 2
        assert exc_info.value._continuation is None
        assert plain.x == 0

    def test_unarmed_resume_raises(self):
        with pytest.raises(RuntimeError, match="not been armed"):
            DeferredAssignment(1).resume()

    def test_frozen_classes_cannot_opt_in(self):
        with pytest.raises(ValueError, match="cooperative_setattr"):

            @attr.define(frozen=True, cooperative_setattr=True)
            class Bad:
                x = 1

    def test_custom_setattr_still_conflicts_with_hooks(self):
        with pytest.raises(
            ValueError, match="Can't combine custom __setattr__"
        ):

            @attr.define(cooperative_setattr=True)
            class Bad:
                x = attr.field(default=1, on_setattr=lambda i, a, v: v)

                def __setattr__(self, name, value):
                    object.__setattr__(self, name, value)

    def test_classic_attrs_entry_point_and_inspect(self, slots):
        @attr.s(cooperative_setattr=True, slots=slots)
        class Classic:
            x = attr.ib(default=1, on_setattr=lambda i, a, v: v + 1)

        classic = Classic()
        classic.x = 4
        assert classic.x == 5
        assert attrs.inspect(Classic).cooperative_setattr is True


class TestRecountScenario:
    """The scanner/audit/ledger write chain from the migration story."""

    def _build(self, slots):
        ledger_values = []
        audit_seen = []

        class LedgerLayer:
            def __setattr__(self, name, value):
                if name == "pieces":
                    ledger_values.append(value)
                object.__setattr__(self, name, value)

        def stock_rule(instance, attrib, value):
            return value * 6

        def recount_rule(instance, attrib, value):
            pieces = value * 8
            if pieces > CAP:
                msg = f"{pieces} pieces exceeds the recount cap of {CAP}"
                raise ValueError(msg)
            return pieces

        @attr.define(
            cooperative_setattr=True,
            slots=slots,
            on_setattr=setters.NO_OP,
        )
        class Stock(LedgerLayer):
            pieces = attr.field(default=0, on_setattr=stock_rule)
            note = attr.field(default="")
            exempt = attr.field(default="", on_setattr=setters.NO_OP)

        class Audit(Stock):
            pending = None

            def __setattr__(self, name, value):
                if name == "pieces":
                    audit_seen.append(value)
                    deferred = DeferredAssignment(value)
                    # Re-entry point below the audit layer.
                    deferred.arm(
                        lambda confirmed=value: super(Audit, self).__setattr__(
                            name, confirmed
                        )
                    )
                    type(self).pending = deferred
                    return

                super().__setattr__(name, value)

            def write_below_audit(self, name, value):
                super(Audit, self).__setattr__(name, value)

        @attr.define(
            cooperative_setattr=True,
            slots=slots,
            on_setattr=setters.NO_OP,
        )
        class Recount(Audit):
            pieces = attr.field(default=0, on_setattr=recount_rule)

        class Scanner(Recount):
            def __setattr__(self, name, value):
                super().__setattr__(name, value)

            def supplement_boxes(self, boxes):
                field = attr.fields(type(self)).pieces
                confirmed = recount_rule(self, field, boxes)
                self.write_below_audit("pieces", confirmed)

        return (
            Scanner,
            Stock,
            recount_rule,
            ledger_values,
            audit_seen,
        )

    def test_full_chain(self, slots):
        (
            Scanner,
            Stock,
            _recount_rule,
            ledger_values,
            audit_seen,
        ) = self._build(slots)

        scanner = Scanner()
        object.__setattr__(scanner, "pieces", 8)

        # Two boxes become sixteen pieces, then audit suspends the write.
        scanner.pieces = 2
        assert scanner.pieces == 8
        assert audit_seen == [16]
        assert ledger_values == []
        assert Scanner.pending.deferred_value == 16

        # Confirmation resumes below audit with the confirmed sixteen pieces.
        Scanner.pending.resume()
        assert scanner.pieces == 16
        assert ledger_values == [16]
        assert audit_seen == [16]

        # Seventeen boxes are rejected as over the cap, leaving the sheet alone.
        with pytest.raises(ValueError, match="136"):
            scanner.supplement_boxes(17)
        assert scanner.pieces == 16

        # Sixteen boxes are accepted and become the 128-piece cap.
        scanner.supplement_boxes(16)
        assert scanner.pieces == 128

        assert ledger_values == [16, 128]
        assert audit_seen == [16]

        # A plain stock sheet still uses the six-pieces rule.
        stock = Stock()
        stock.pieces = 2
        assert stock.pieces == 12

        # Plain notes and explicitly exempt fields still use the old chain.
        scanner.note = "ok"
        assert scanner.note == "ok"
        scanner.exempt = "waived"
        assert scanner.exempt == "waived"
