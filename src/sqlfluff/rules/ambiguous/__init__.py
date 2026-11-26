"""The ambiguous plugin bundle.

NOTE: Yes the title of this bundle is ...ambiguous. 😁
"""

from sqlfluff.core.plugin import hookimpl
from sqlfluff.core.rules import BaseRule, ConfigInfo


@hookimpl
def get_configs_info() -> dict[str, ConfigInfo]:
    """Get additional rule config validations and descriptions."""
    return {
        "fully_qualify_join_types": {
            "validation": ["inner", "outer", "both"],
            "definition": ("Which types of JOIN clauses should be fully qualified?"),
        },
        "group_by_and_order_by_style": {
            "validation": ["consistent", "implicit", "explicit"],
            "definition": (
                "The expectation for using explicit column name references "
                "or implicit positional references."
            ),
        },
        "catalog_path": {
            "definition": (
                "Path to a read-only YAML/JSON catalog declaring natural keys, "
                "nullable key columns and relationship cardinality."
            ),
        },
        "catalog": {
            "definition": (
                "Read-only table metadata (YAML/JSON mapping or inline text) "
                "declaring natural keys, nullable key columns and relationship "
                "cardinality for the deterministic row-pick rule."
            ),
        },
        "report_insufficient_evidence": {
            "validation": [True, False],
            "definition": (
                "Report row picks whose determinism cannot be proven from the "
                "catalog as insufficient evidence. Defaults to True."
            ),
        },
    }


@hookimpl
def get_rules() -> list[type[BaseRule]]:
    """Get plugin rules.

    NOTE: Rules are imported only on fetch to manage import times
    when rules aren't used.
    """
    from sqlfluff.rules.ambiguous.AM01 import Rule_AM01
    from sqlfluff.rules.ambiguous.AM02 import Rule_AM02
    from sqlfluff.rules.ambiguous.AM03 import Rule_AM03
    from sqlfluff.rules.ambiguous.AM04 import Rule_AM04
    from sqlfluff.rules.ambiguous.AM05 import Rule_AM05
    from sqlfluff.rules.ambiguous.AM06 import Rule_AM06
    from sqlfluff.rules.ambiguous.AM07 import Rule_AM07
    from sqlfluff.rules.ambiguous.AM08 import Rule_AM08
    from sqlfluff.rules.ambiguous.AM09 import Rule_AM09
    from sqlfluff.rules.ambiguous.AM10 import Rule_AM10

    return [
        Rule_AM01,
        Rule_AM02,
        Rule_AM03,
        Rule_AM04,
        Rule_AM05,
        Rule_AM06,
        Rule_AM07,
        Rule_AM08,
        Rule_AM09,
        Rule_AM10,
    ]
