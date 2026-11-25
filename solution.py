from __future__ import annotations

from faust.revision_delivery import *  # noqa: F401,F403
from faust.revision_delivery import (
    __all__ as _core_all,
    simulate_category_revision_scenario,
)

__all__ = _core_all


def main() -> None:
    service = simulate_category_revision_scenario()
    print(f"shard_counts={service.counts}")
    print(f"published_counts={service.published_counts}")
    print(
        "publications="
        + ",".join(f"{item.item_id}@r{item.revision}" for item in service.publications)
    )
    print(f"A_net={service.net_changes('A')}")
    print(f"B_net={service.net_changes('B')}")


if __name__ == "__main__":
    main()
