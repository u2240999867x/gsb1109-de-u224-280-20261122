"""Implementation of Rule AM10."""

from typing import Optional

from sqlfluff.core.rules import BaseRule, LintResult, RuleContext
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.rules._row_pick_base import RowPickDeterminismMixin


class Rule_AM10(RowPickDeterminismMixin, BaseRule):
    """``ROW_NUMBER()`` row pick may return a non-deterministic row.

    This is the ambiguity-grouped entry point for the deterministic row-pick
    check.  It behaves exactly like :sqlfluff:ref:`structure.deterministic_row_pick`
    (``ST13``) and exists so teams which treat ambiguous row selection as an
    ``AM`` concern can select it directly.

    A "pick latest row per group" query::

        SELECT ...
        FROM (
            SELECT ...,
                   ROW_NUMBER() OVER (
                       PARTITION BY ...
                       ORDER BY ...
                   ) AS rn
            FROM ...
        ) picked
        WHERE rn = 1

    is deterministic only when the partition, filters and ordering, together
    with catalog evidence about unique keys and relationship cardinality,
    prove that every row has a unique rank.  An outer ``DISTINCT`` does not
    help: the window assigns ranks before the outer query removes duplicates.

    The rule requires a read-only catalog declaring natural keys, nullable key
    columns and relationship cardinality.  Without one it stays silent rather
    than treating the rule's own enablement as proof.

    **Anti-pattern**

    Two receipts for the same record make the join return two rows which share
    the window ordering, so both can receive rank ``1``.

    .. code-block:: sql

        SELECT site_id, event_time, record_no
        FROM (
            SELECT e.site_id,
                   e.event_time,
                   e.record_no,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.site_id
                       ORDER BY e.event_time DESC, e.record_no
                   ) AS rn
            FROM event AS e
            JOIN receipt AS r ON r.record_no = e.record_no
        ) AS picked
        WHERE picked.rn = 1

    **Best practice**

    Add the right-side key to the ordering, or filter to one receipt first.

    .. code-block:: sql

        SELECT site_id, event_time, record_no
        FROM (
            SELECT e.site_id,
                   e.event_time,
                   e.record_no,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.site_id
                       ORDER BY e.event_time DESC, e.record_no, r.receipt_no
                   ) AS rn
            FROM event AS e
            JOIN receipt AS r ON r.record_no = e.record_no
        ) AS picked
        WHERE picked.rn = 1
    """

    name = "ambiguous.row_pick_determinism"
    aliases = ()
    groups: tuple[str, ...] = ("all", "ambiguous")
    config_keywords = [
        "catalog",
        "catalog_path",
        "report_insufficient_evidence",
    ]
    crawl_behaviour = SegmentSeekerCrawler({"select_statement"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[list[LintResult]]:
        """Assess every row pick in this SELECT."""
        return super()._eval(context)
