import unittest

import pytest

from bigtree import Tree
from bigtree.node import node
from bigtree.tree import batch
from bigtree.utils import exceptions


def build_site_tree():
    root = node.Node("站点")
    old = node.Node("旧版", parent=root)
    new = node.Node("新版", parent=root)
    old_archive = node.Node("归档", parent=old)
    new_archive = node.Node("归档", parent=new)
    old_index = node.Node("index", parent=old_archive)
    new_index = node.Node("index", parent=new_archive)
    old_archive.set_attrs({"kind": "old"})
    new_archive.set_attrs({"kind": "new"})
    old_index.set_attrs({"kind": "old-index"})
    new_index.set_attrs({"kind": "new-index"})
    return root, old, new, old_archive, new_archive, old_index, new_index


def build_pending_tree():
    root = node.Node("站点")
    node.Node("旧版", parent=root)
    node.Node("新版", parent=root)
    node.Node("待采", parent=root)
    node.Node("封存", parent=root)
    return root


class TestSwapSubtrees(unittest.TestCase):
    def test_swap_keeps_node_identity(self):
        root, old, new, old_archive, new_archive, old_index, new_index = (
            build_site_tree()
        )
        manifest = batch.swap_subtrees(root, "站点/旧版/归档", "站点/新版/归档")

        assert old_archive.parent is new
        assert new_archive.parent is old
        assert old_index.parent is old_archive
        assert new_index.parent is new_archive
        assert manifest.resolve("站点/旧版/归档") is new_archive
        assert manifest.resolve("站点/新版/归档") is old_archive
        assert manifest.resolve("站点/旧版/归档/index") is new_index
        assert manifest.resolve("站点/新版/归档/index") is old_index

    def test_repeated_path_lookup_uses_start_layout(self):
        root, old, new, old_archive, new_archive, old_index, new_index = (
            build_site_tree()
        )
        node.Node("其他", parent=root)
        operation = batch.TreeBatch(root)
        operation.swap("站点/旧版/归档", "站点/新版/归档")
        operation.move("站点/旧版/归档/index", "站点/其他")
        manifest = operation.commit()

        assert old_index.parent is root["其他"]
        assert new_index.parent is new_archive
        assert manifest.resolve("站点/其他/index") is old_index
        assert manifest.resolve("站点/旧版/归档/index") is new_index

    def test_swap_sibling_positions(self):
        root, old, new, old_archive, new_archive, old_index, new_index = (
            build_site_tree()
        )
        manifest = (
            batch.TreeBatch(root).swap("站点/旧版/归档", "站点/新版/归档").commit()
        )

        assert [child.node_name for child in old.children] == ["归档"]
        assert [child.node_name for child in new.children] == ["归档"]
        assert manifest.index_of(old_archive) == 0
        assert manifest.index_of(new_archive) == 0

    def test_swap_other_parents_keeps_position(self):
        root = node.Node("a")
        b = node.Node("b", parent=root)
        c = node.Node("c", parent=root)
        node.Node("b1", parent=b)
        node.Node("c1", parent=c)
        manifest = batch.swap_subtrees(root, "a/b", "a/c")
        assert [child.node_name for child in root.children] == ["c", "b"]
        assert manifest.path_of(b) == "/a/b"
        assert manifest.path_of(c) == "/a/c"
        assert manifest.index_of(c) == 0
        assert manifest.index_of(b) == 1
        assert b.children[0].node_name == "b1"
        assert c.children[0].node_name == "c1"


class TestValidation(unittest.TestCase):
    def test_validator_sees_complete_candidate_layout(self):
        root, old, new, old_archive, new_archive, old_index, new_index = (
            build_site_tree()
        )
        seen = {}

        def callback(layout):
            seen["nodes"] = [layout.path_of(n) for n in layout.walk()]
            saved = layout.view(old_index)
            seen["parent"] = saved.parent.path_name
            seen["left"] = saved.left_sibling
            seen["right"] = saved.right_sibling
            return True

        manifest = (
            batch.TreeBatch(root, validators=[callback])
            .swap("站点/旧版/归档", "站点/新版/归档")
            .commit()
        )

        assert seen["nodes"] == [
            "/站点",
            "/站点/旧版",
            "/站点/旧版/归档",
            "/站点/旧版/归档/index",
            "/站点/新版",
            "/站点/新版/归档",
            "/站点/新版/归档/index",
        ]
        assert seen["parent"] == "/站点/新版/归档"
        assert seen["left"] is None
        assert seen["right"] is None
        assert manifest.resolve("站点/新版/归档/index") is old_index

    def test_pending_after_sealed_rejects_batch(self):
        root = build_pending_tree()
        pending = root["待采"]
        old_order = [child.node_name for child in root.children]

        def reject_pending_after_sealed(layout):
            pending_view = layout.view(pending)
            left = pending_view.left_sibling
            return not (left is not None and left.node_name == "封存")

        manifest = batch.open_manifest(root)
        operation = batch.TreeBatch(root, validators=[reject_pending_after_sealed])
        operation.swap("站点/旧版", "站点/新版")
        operation.move("站点/待采", "站点", 3)
        with pytest.raises(exceptions.BatchValidationError):
            operation.commit()

        assert [child.node_name for child in root.children] == old_order
        assert pending.parent is root
        assert [child.node_name for child in root.children].index("待采") == 2
        assert manifest.render() == batch.open_manifest(root).render()
        assert operation.committed is False
        assert operation.committed_manifest is None

    def test_validator_error_rejected(self):
        root = build_pending_tree()

        def raise_error(layout):
            raise RuntimeError("cannot accept")

        with pytest.raises(exceptions.BatchValidationError):
            batch.TreeBatch(root, validators=[raise_error]).move(
                "站点/待采", "站点", 0
            ).commit()
        assert [child.node_name for child in root.children] == [
            "旧版",
            "新版",
            "待采",
            "封存",
        ]

    def test_registered_validator(self):
        root = build_pending_tree()
        calls = []

        def callback(layout):
            calls.append(layout.render())
            return False

        batch.register_validator(root, callback)
        try:
            with pytest.raises(exceptions.BatchValidationError):
                batch.move_subtree(root, "站点/待采", "站点", 0)
            assert len(calls) == 1
        finally:
            batch.clear_validators(root)
        batch.move_subtree(root, "站点/待采", "站点", 0)
        assert [child.node_name for child in root.children][0] == "待采"


class TestCycleAndConflict(unittest.TestCase):
    def test_move_into_descendant_rejected(self):
        root, old, new, old_archive, new_archive, old_index, new_index = (
            build_site_tree()
        )
        operation = batch.TreeBatch(root).move("站点/新版", "站点/新版/归档")
        with pytest.raises(exceptions.BatchCycleError):
            operation.commit()
        assert new.parent is root
        assert old.parent is root
        assert [child.node_name for child in root.children] == ["旧版", "新版"]

    def test_swap_with_descendant_rejected(self):
        root, old, new, old_archive, new_archive, old_index, new_index = (
            build_site_tree()
        )
        operation = batch.TreeBatch(root).swap("站点/新版", "站点/新版/归档")
        with pytest.raises(exceptions.BatchCycleError):
            operation.commit()
        assert new_archive.parent is new

    def test_commit_twice_rejected(self):
        root = build_pending_tree()
        operation = batch.TreeBatch(root).move("站点/待采", "站点", 0)
        operation.commit()
        with pytest.raises(exceptions.BatchError):
            operation.commit()

    def test_external_structure_change_rejected(self):
        root = build_pending_tree()
        operation = batch.TreeBatch(root).move("站点/待采", "站点", 0)
        root["封存"].parent = None
        with pytest.raises(exceptions.BatchConflictError):
            operation.commit()
        assert root["待采"].parent is root


class TestManifestReads(unittest.TestCase):
    def test_open_manifest_survives_successful_commit(self):
        root = build_pending_tree()
        reader = batch.open_manifest(root)
        batch.move_subtree(root, "站点/待采", "站点", 0)
        newer = batch.open_manifest(root)

        assert [child.node_name for child in reader.children_of(root)] == [
            "旧版",
            "新版",
            "待采",
            "封存",
        ]
        assert [child.node_name for child in newer.children_of(root)] == [
            "待采",
            "旧版",
            "新版",
            "封存",
        ]
        assert reader.path_of(root["待采"]) == "/站点/待采"
        assert newer.path_of(root["待采"]) == "/站点/待采"

    def test_manifest_path_lookup_survives_swap(self):
        root, old, new, old_archive, new_archive, old_index, new_index = (
            build_site_tree()
        )
        reader = batch.open_manifest(root)
        batch.swap_subtrees(root, "站点/旧版/归档", "站点/新版/归档")

        assert reader.path_of(old_index) == "/站点/旧版/归档/index"
        assert reader.path_of(new_index) == "/站点/新版/归档/index"
        assert reader.resolve("站点/旧版/归档/index") is old_index
        assert reader.resolve("站点/新版/归档/index") is new_index

    def test_view_saved_reference_under_new_parent(self):
        root, old, new, old_archive, new_archive, old_index, new_index = (
            build_site_tree()
        )
        reader = batch.open_manifest(root)
        batch.swap_subtrees(root, "站点/旧版/归档", "站点/新版/归档")
        view = reader.view(old_index)

        assert view.parent.path_name == "/站点/旧版/归档"
        assert view.path_name == "/站点/旧版/归档/index"

    def test_direct_node_edit_still_works(self):
        root = build_pending_tree()
        pending = root["待采"]
        pending.parent = root["旧版"]
        assert root["旧版"]["待采"].parent is root["旧版"]
        assert pending.left_sibling is None
        assert pending.right_sibling is None


class TestConvenienceAndTreePlugin(unittest.TestCase):
    def test_apply_batch(self):
        root, old, new, old_archive, new_archive, old_index, new_index = (
            build_site_tree()
        )
        other = node.Node("其他", parent=root)
        manifest = batch.apply_batch(
            root,
            swaps=[("站点/旧版/归档", "站点/新版/归档")],
            moves=[("站点/旧版/归档/index", "站点/其他", 0)],
        )
        assert manifest.resolve("站点/新版/归档") is old_archive
        assert manifest.resolve("站点/其他/index") is old_index
        assert other.children[0] is old_index

    def test_tree_plugin_methods(self):
        tree = Tree.from_list(["站点/旧版/归档/index", "站点/新版/归档/index"])
        old_index = tree.node["旧版"]["归档"]["index"]
        assert tree.batch().start_layout.root is tree.node
        manifest = tree.swap_subtrees("站点/旧版/归档", "站点/新版/归档")
        assert manifest.resolve("站点/新版/归档/index") is old_index
        assert tree.open_manifest().revision == manifest.revision

        def accept(layout):
            return True

        tree.register_validator(accept)
        tree.unregister_validator(accept)
        batch.clear_validators(tree.node)

    def test_tree_wrapper_and_node_reference(self):
        tree = Tree.from_list(["a/x/b", "a/y/c"])
        saved = tree.node["x"]["b"]
        manifest = batch.swap_subtrees(tree, "a/x/b", "a/y/c")
        assert manifest.view(saved).path_name == "/a/y/b"
        assert manifest.view(saved).parent.node_name == "y"
        assert manifest.node().path_name == "/a"

    def test_context_manager(self):
        tree = Tree.from_list(["a/b", "a/c"])
        with tree.batch() as operation:
            operation.swap("a/b", "a/c")
        assert [child.node_name for child in tree.node.children] == ["c", "b"]

    def test_binary_node_rejected(self):
        from bigtree import BinaryNode

        root = BinaryNode(1)
        BinaryNode(2, parent=root)
        with pytest.raises(TypeError):
            batch.TreeBatch(root)
