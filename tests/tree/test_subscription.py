from __future__ import annotations

import unittest

import pytest

from bigtree.node import node
from bigtree.tree import subscription
from bigtree.tree.subscription import (
    PathRule,
    PathSubscription,
    PathSubscriptionBatch,
    PathSubscriptionChange,
    PathSubscriptionTransaction,
)


def build_catalog() -> tuple[node.Node, node.Node, node.Node]:
    """Build a catalogue with a furniture region, siblings and an outside directory.

    Returns:
        Root, region node used as the subscription anchor, and the outside directory
    """
    root = node.Node("catalog")

    regions = node.Node("regions", parent=root)
    north = node.Node("north", parent=regions)  # subscription anchor
    east = node.Node("east", parent=regions)
    south = node.Node("south", parent=regions, level="province")

    for region, page_ids in (
        (north, ("N-list", "N-detail")),
        (east, ("E-list", "E-detail")),
        (south, ("S-list", "S-detail")),
    ):
        pages = node.Node("pages", parent=region)
        node.Node("list", parent=pages, page_id=page_ids[0])
        node.Node("detail", parent=pages, page_id=page_ids[1])

    # Existing directory outside of `regions`, same page names, different page ids.
    outside = node.Node("outside", parent=root)
    pages = node.Node("pages", parent=outside)
    node.Node("list", parent=pages, page_id="O-list")
    node.Node("detail", parent=pages, page_id="O-detail")

    return root, north, outside


class TestPathRule(unittest.TestCase):
    def setUp(self):
        self.root, self.north, self.outside = build_catalog()

    def test_resolve_wildcard(self):
        rule = PathRule("siblings", "../*/pages/*", anchor=self.north)
        actual = tuple(_node.path_name for _node in rule.resolve(self.root))
        expected = (
            "/catalog/regions/north/pages/list",
            "/catalog/regions/north/pages/detail",
            "/catalog/regions/east/pages/list",
            "/catalog/regions/east/pages/detail",
            "/catalog/regions/south/pages/list",
            "/catalog/regions/south/pages/detail",
        )
        assert actual == expected, f"Expected {expected}, received {actual}"

    def test_resolve_excludes_province_subtree(self):
        rule = PathRule(
            "siblings",
            "../*/pages/*",
            anchor=self.north,
            exclude="../south",
        )
        actual = tuple(_node.path_name for _node in rule.resolve(self.root))
        expected = (
            "/catalog/regions/north/pages/list",
            "/catalog/regions/north/pages/detail",
            "/catalog/regions/east/pages/list",
            "/catalog/regions/east/pages/detail",
        )
        assert actual == expected, f"Expected {expected}, received {actual}"

    def test_resolve_excludes_node_object(self):
        south = self.root["regions"]["south"]
        rule = PathRule(
            "siblings",
            "../*/pages/*",
            anchor=self.north,
            exclude=south,
        )
        page_ids = tuple(_node.get_attr("page_id") for _node in rule.resolve(self.root))
        assert "S-list" not in page_ids
        assert "S-detail" not in page_ids

    def test_resolve_exclude_missing_path_ignored(self):
        rule = PathRule(
            "siblings",
            "../*/pages/*",
            anchor=self.north,
            exclude=["../missing"],
        )
        assert len(rule.resolve(self.root)) == 6

    def test_resolve_defaults_to_root(self):
        rule = PathRule("all", "regions/*/pages/*")
        assert len(rule.resolve(self.root)) == 6

    def test_rule_key_changes_with_settings(self):
        rule = PathRule("siblings", "../*/pages/*", anchor=self.north)
        same = PathRule("siblings", "../*/pages/*", anchor=self.north)
        moved = PathRule("siblings", "../*/pages/*", anchor=self.outside)
        assert rule.key == same.key
        assert rule.key != moved.key

    def test_empty_name_error(self):
        with pytest.raises(ValueError):
            PathRule("", "../*/pages/*")

    def test_empty_path_error(self):
        with pytest.raises(ValueError):
            PathRule("name", "")


class TestPathSubscription(unittest.TestCase):
    def setUp(self):
        self.root, self.north, self.outside = build_catalog()
        self.subscription = PathSubscription(self.root)

    def test_snapshot_initial(self):
        self.subscription.subscribe(
            "siblings", "../*/pages/*", anchor=self.north, exclude="../south"
        )
        batch = self.subscription.snapshot()
        assert batch["siblings"].current == (
            "N-list",
            "N-detail",
            "E-list",
            "E-detail",
        )
        assert batch["siblings"].added == batch["siblings"].current
        assert not batch["siblings"].exited

    def test_snapshot_no_change(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.snapshot()
        batch = self.subscription.snapshot()
        assert not batch["siblings"].added
        assert not batch["siblings"].exited
        assert not batch.has_changes

    def test_moved_anchor_keeps_pages(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.snapshot()

        archive = node.Node("archive", parent=self.root)
        self.north.parent = archive
        batch = self.subscription.snapshot()

        # The region moved with its pages, so its own pages are neither added nor exited.
        assert not batch["siblings"].added
        assert "N-list" not in batch["siblings"].exited
        assert "N-detail" not in batch["siblings"].exited
        # Sibling directories are still discovered relative to the moved anchor.
        assert batch["siblings"].current == ("N-list", "N-detail")
        assert batch["siblings"].exited == (
            "E-list",
            "E-detail",
            "S-list",
            "S-detail",
        )

    def test_batch_move_anchor_and_migrate_sibling(self):
        self.subscription.subscribe(
            "siblings", "../*/pages/*", anchor=self.north, exclude="../south"
        )
        self.subscription.snapshot()

        archive = node.Node("archive", parent=self.root)
        with self.subscription.batch() as transaction:
            self.north.parent = archive
            self.outside.parent = archive

        change = transaction.result["siblings"]
        # Genuinely new page ids from the migrated outside directory.
        assert change.added == ("O-list", "O-detail")
        # Genuinely exited page ids from the east directory.
        assert change.exited == ("E-list", "E-detail")
        # Page names are identical across directories; identities keep them apart.
        assert change.current == ("N-list", "N-detail", "O-list", "O-detail")

    def test_transient_paths_do_not_trigger(self):
        self.subscription.subscribe(
            "siblings", "../*/pages/*", anchor=self.north, exclude="../south"
        )
        self.subscription.snapshot()

        # Move the outside directory through the region subtree and back out during one batch.
        with self.subscription.batch() as transaction:
            self.outside.parent = self.north
            self.outside.parent = self.root

        change = transaction.result["siblings"]
        assert not change.added
        assert not change.exited

    def test_batch_mid_state_not_stored(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.snapshot()

        archive = node.Node("archive", parent=self.root)
        transaction = self.subscription.begin_batch()
        self.north.parent = archive
        # Intermediate snapshot must not be retained as the batch baseline.
        batch = self.subscription.end_batch(affected=["siblings"])
        assert batch["siblings"].exited == (
            "E-list",
            "E-detail",
            "S-list",
            "S-detail",
        )
        assert transaction.result["siblings"].current == ("N-list", "N-detail")

    def test_unaffected_subscription_keeps_result(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        docs = node.Node("docs", parent=self.root)
        docs_pages = node.Node("pages", parent=docs)
        node.Node("list", parent=docs_pages, page_id="D-list")
        self.subscription.subscribe("docs", "docs/pages/*")
        initial = self.subscription.snapshot()

        archive = node.Node("archive", parent=self.root)
        with self.subscription.batch() as transaction:
            transaction.mark_affected("siblings")
            self.north.parent = archive

        retained = transaction.result["docs"]
        assert retained.unaffected
        assert retained.current == initial["docs"].current
        assert retained.retained == initial["docs"].current
        assert not retained.added
        assert not retained.exited

    def test_affected_subscription_recomputed(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.subscribe("all", "regions/*/pages/*")
        self.subscription.snapshot()

        archive = node.Node("archive", parent=self.root)
        with self.subscription.batch() as transaction:
            transaction.mark_affected("all")
            self.north.parent = archive

        change = transaction.result["all"]
        assert change.exited == ("N-list", "N-detail")
        assert not change.added

    def test_batch_rollback_on_error(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        before = self.subscription.snapshot()["siblings"].current

        archive = node.Node("archive", parent=self.root)
        with pytest.raises(RuntimeError):
            with self.subscription.batch():
                self.north.parent = archive
                raise RuntimeError("batch aborted")

        # The failed batch does not advance the baseline; undoing the change and retrying reports nothing.
        regions = self.root["regions"]
        self.north.parent = regions
        after = self.subscription.snapshot()["siblings"]
        assert set(after.current) == set(before)
        assert not after.added
        assert not after.exited

    def test_commit_error_leaves_snapshot_untouched(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.snapshot()
        pages = self.north["pages"]
        node.Node("no-id", parent=pages)

        with pytest.raises(ValueError):
            self.subscription.snapshot()

        # Previous snapshot survives, so the error can be fixed and retried.
        pages["no-id"].parent = None
        batch = self.subscription.snapshot()
        assert not batch["siblings"].added
        assert not batch["siblings"].exited

    def test_peek_does_not_store(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.snapshot()
        self.subscription.subscribe("all", "regions/*/pages/*")
        peeked = self.subscription.peek()
        assert peeked["all"].added
        stored = self.subscription.snapshot()
        assert stored["all"].added == peeked["all"].added

    def test_commit_additional_rules(self):
        rule = PathRule("siblings", "../*/pages/*", anchor=self.north)
        batch = self.subscription.commit(rules=[rule])
        assert batch["siblings"].current
        assert "siblings" in self.subscription.rules

    def test_commit_replaced_rule_starts_fresh(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.snapshot()
        replacement = PathRule(
            "siblings", "../*/pages/*", anchor=self.north, exclude="../east"
        )
        batch = self.subscription.commit(rules=[replacement])
        assert batch["siblings"].current == ("N-list", "N-detail", "S-list", "S-detail")
        assert batch["siblings"].added == batch["siblings"].current

    def test_commit_with_explicit_changes(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.snapshot()
        pages = self.subscription.resolve("siblings")
        kept = tuple(_node for _node in pages if _node.get_attr("page_id") != "E-list")
        batch = self.subscription.commit(changes={"siblings": kept})
        assert batch["siblings"].exited == ("E-list",)

    def test_missing_fixed_path_reports_exits(self):
        self.subscription.subscribe("docs", "docs/pages/*")
        docs = node.Node("docs", parent=self.root)
        docs_pages = node.Node("pages", parent=docs)
        node.Node("list", parent=docs_pages, page_id="D-list")
        self.subscription.snapshot()

        docs.parent = None
        change = self.subscription.snapshot()["docs"]
        assert change.exited == ("D-list",)
        assert not change.added
        assert not change.current

    def test_remove_rule(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.snapshot()
        self.subscription.remove_rule("siblings")
        assert not self.subscription.rules
        assert not self.subscription.snapshot().changes

    def test_clear(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.snapshot()
        self.subscription.clear()
        batch = self.subscription.snapshot()
        assert batch["siblings"].added == batch["siblings"].current

    def test_replace_rule_resets_snapshot(self):
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        self.subscription.snapshot()
        self.subscription.subscribe(
            "siblings", "../*/pages/*", anchor=self.north, exclude="../east"
        )
        batch = self.subscription.snapshot()
        assert batch["siblings"].current == ("N-list", "N-detail", "S-list", "S-detail")
        assert batch["siblings"].added == batch["siblings"].current

    def test_identity_path(self):
        self.subscription = PathSubscription(self.root, identity="path")
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        batch = self.subscription.snapshot()
        assert batch["siblings"].current[0] == "/catalog/regions/north/pages/list"

    def test_identity_node_name(self):
        self.subscription = PathSubscription(self.root, identity="node_name")
        self.subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        batch = self.subscription.snapshot()
        assert batch["siblings"].current == ("list", "detail") * 3

    def test_identity_invalid_error(self):
        with pytest.raises(ValueError):
            PathSubscription(self.root, identity="invalid")

    def test_tree_wrapper_accepted(self):
        from bigtree.tree.tree import Tree

        wrapped = PathSubscription(Tree(self.root))
        wrapped.subscribe("siblings", "../*/pages/*", anchor=self.north)
        assert wrapped.snapshot()["siblings"].current

    def test_invalid_tree_type_error(self):
        with pytest.raises(TypeError):
            PathSubscription(object())  # type: ignore[arg-type]

    def test_custom_id_attr(self):
        subscription = PathSubscription(self.root, id_attr="page_id")
        subscription.subscribe("siblings", "../*/pages/*", anchor=self.north)
        node_attr_rule = PathRule(
            "custom", "../*/pages/*", anchor=self.north, id_attr="page_id"
        )
        assert node_attr_rule.id_attr == "page_id"
        assert subscription.snapshot()["siblings"].current


class TestPromptScenario(unittest.TestCase):
    """End-to-end coverage of the subscription-region reorganisation."""

    def setUp(self):
        self.root, self.north, self.outside = build_catalog()

    def test_move_region_and_migrate_outside_in_one_batch(self):
        subscription_ = PathSubscription(self.root)
        subscription_.subscribe(
            "region",
            "../*/pages/*",
            anchor=self.north,
            exclude="../south",
        )
        subscription_.subscribe("docs", "missing/*")
        initial = subscription_.snapshot()
        assert initial["region"].current == ("N-list", "N-detail", "E-list", "E-detail")
        assert not initial["docs"].current

        archive = node.Node("archive", parent=self.root)
        with subscription_.batch() as transaction:
            transaction.mark_affected("region")
            # First the subscription region moves to another layer.
            self.north.parent = archive
            # Then an existing outside directory is migrated in as its new sibling.
            # It was never inside the moved subtree.
            self.outside.parent = archive

        change = transaction.result["region"]
        # Same page names as the east directory, but only genuinely new page ids are reported.
        assert change.added == ("O-list", "O-detail")
        assert change.exited == ("E-list", "E-detail")
        assert change.current == ("N-list", "N-detail", "O-list", "O-detail")
        assert set(change.added).isdisjoint(change.exited)

        # The unaffected subscription keeps its existing result.
        docs = transaction.result["docs"]
        assert docs.unaffected
        assert docs.retained == initial["docs"].current

    def test_names_identical_but_identities_differ(self):
        subscription_ = PathSubscription(self.root)
        subscription_.subscribe(
            "region", "../*/pages/*", anchor=self.north, exclude="../south"
        )
        subscription_.snapshot()

        # Replace the east directory's pages with the same names but new page ids.
        east_pages = self.root["regions"]["east"]["pages"]
        for child in list(east_pages.children):
            child.parent = None
        node.Node("list", parent=east_pages, page_id="E-list-v2")
        node.Node("detail", parent=east_pages, page_id="E-detail-v2")

        change = subscription_.snapshot()["region"]
        assert change.added == ("E-list-v2", "E-detail-v2")
        assert change.exited == ("E-list", "E-detail")
        assert "N-list" not in change.added + change.exited


class TestPathRuleSiblingResolution(unittest.TestCase):
    def test_region_siblings_and_province_exclusion(self):
        root = node.Node("catalog")
        country = node.Node("country", parent=root)
        regions = node.Node("regions", parent=country)
        region = node.Node("north", parent=regions)
        sibling = node.Node("east", parent=regions)
        province = node.Node("province", parent=regions)
        province_pages = node.Node("pages", parent=province)
        node.Node("list", parent=province_pages, page_id="P-list")

        for parent, page_ids in (
            (region, ("N-list",)),
            (sibling, ("E-list",)),
        ):
            pages = node.Node("pages", parent=parent)
            for page_id in page_ids:
                node.Node("list", parent=pages, page_id=page_id)

        subscription = PathSubscription(root)
        subscription.subscribe(
            "region-siblings",
            "../*/pages/*",
            anchor=region,
            exclude="../province",
        )
        batch = subscription.snapshot()
        assert batch["region-siblings"].current == ("N-list", "E-list")
        assert "P-list" not in batch["region-siblings"].current


class TestBatchTypes(unittest.TestCase):
    def setUp(self):
        self.root, self.north, self.outside = build_catalog()

    def test_change_properties(self):
        change = PathSubscriptionChange("r", ("a",), ("b",), ("a",), ("b",))
        assert change.entered == ("a",)
        assert change.removed == ("b",)
        assert change.changed
        assert not change.unaffected
        assert not change.retained
        assert "PathSubscriptionChange" in repr(change)

    def test_batch_properties(self):
        change = subscription.PathSubscriptionChange("r", ("a",), (), ("a",))
        batch = PathSubscriptionBatch({"r": change})
        assert len(batch) == 1
        assert "r" in batch
        assert list(batch) == ["r"]
        assert batch.added("r") == ("a",)
        assert batch.exited("r") == ()
        assert batch.removed("r") == ()
        assert batch.has_changes

    def test_transaction_result_before_commit_error(self):
        transaction = PathSubscriptionTransaction(PathSubscription(self.root))
        with pytest.raises(RuntimeError):
            _ = transaction.result

    def test_end_batch_without_begin_error(self):
        subscription_ = PathSubscription(self.root)
        with pytest.raises(RuntimeError):
            subscription_.end_batch()

    def test_double_begin_error(self):
        subscription_ = PathSubscription(self.root)
        subscription_.begin_batch()
        with pytest.raises(RuntimeError):
            subscription_.begin_batch()
