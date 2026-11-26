"""Implementation of Rule ST13."""

from typing import Optional

from sqlfluff.core.rules import BaseRule, LintResult, RuleContext
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.rules._row_pick_base import RowPickDeterminismMixin


class Rule_ST13(RowPickDeterminismMixin, BaseRule):
    """Row pick is not proven deterministic.

    ``ROW_NUMBER()`` is often used to pick one row per group::

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

    This rule checks whether that pick is deterministic.  It only reports a
    deterministic result when the query's partition, filters and ordering,
    together with catalog evidence about unique keys and relationship
    cardinality, prove that every row has a unique ordering position.  When
    the ordering can tie (for example because event timestamps are equal and
    the source record number is not unique), the violation names the ordering
    expressions and the key columns which still contend for the same rank.

    The rule is read-only: no fix is proposed, because adding a tie-breaker
    changes which row is selected.  A user-supplied catalog is required; the
    catalog declares natural keys, nullable key columns and relationship
    cardinality.  Without a catalog, the rule stays silent because enabling a
    rule is not evidence.

    **Anti-pattern**

    The following picks one event per site.  Two events for the same site can
    share a timestamp and a record number, so both can receive rank ``1``.

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
        ) AS picked
        WHERE picked.rn = 1

    **Best practice**

    Order by the full natural key (or filter a key tail so that the remaining
    columns are unique):

    .. code-block:: sql

        SELECT site_id, event_time, record_no
        FROM (
            SELECT e.site_id,
                   e.event_time,
                   e.record_no,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.site_id
                       ORDER BY e.event_time DESC, e.record_no, e.event_seq
                   ) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
    """

    name = "structure.deterministic_row_pick"
    aliases = ()
    groups: tuple[str, ...] = ("all", "structure")
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
