import pytest

from bigtree import Node, Tree, tree_to_newick
from bigtree.tree import construct
from bigtree.tree.construct import newick
from bigtree.utils import exceptions

QUOTED_NEWICK = (
    "(('A(1);x':1.2[&&NHX:code=A7],'B;2':0.0[&&NHX:code=A8])"
    "inner[&&NHX:note=foo]:3.4,C:5)root;"
)
DUPLICATE_NEWICK = "(A:0[&&NHX:code=A7],A:1.5[&&NHX:code=A8])group;"


def _describe(root):
    return [
        (
            node.node_name,
            node.depth,
            node.get_attr("code"),
            node.get_attr("length"),
            node.get_attr("note"),
            [child.node_name for child in node.children],
        )
        for node in [root, *root.descendants]
    ]


class TestCleanNewick:
    def test_clean_newick_quoted_labels_and_annotations(self):
        root = newick.clean_newick(QUOTED_NEWICK)[0]
        assert _describe(root) == [
            ("root", 1, None, None, None, ["inner", "C"]),
            ("inner", 2, None, 3.4, "foo", ["A(1);x", "B;2"]),
            ("A(1);x", 3, "A7", 1.2, None, []),
            ("B;2", 3, "A8", 0.0, None, []),
            ("C", 2, None, 5, None, []),
        ]

    def test_clean_newick_duplicate_labels_distinct_codes(self):
        root = newick.clean_newick(DUPLICATE_NEWICK)[0]
        assert [child.node_name for child in root.children] == ["A", "A"]
        assert [child.get_attr("code") for child in root.children] == ["A7", "A8"]
        assert [child.get_attr("length") for child in root.children] == [0, 1.5]

    def test_clean_newick_duplicate_labels_disallowed(self):
        with pytest.raises(exceptions.NewickError) as exc_info:
            newick.clean_newick(DUPLICATE_NEWICK, allow_duplicate_names=False)
        error = exc_info.value
        assert error.tree_index == 1
        assert error.label == "A"
        assert error.line == 1
        assert error.column == 21

    def test_clean_newick_multiple_trees(self):
        roots = newick.clean_newick(QUOTED_NEWICK + "\n" + DUPLICATE_NEWICK)
        assert len(roots) == 2
        assert [root.node_name for root in roots] == ["root", "group"]

    def test_clean_newick_file(self, tmp_path):
        filepath = tmp_path / "lab_a.nwk"
        filepath.write_text("(('A(1);x':1.2,'B;2':0)inner:3.4,C:5)root;\n")
        root = construct.clean_newick_file(filepath)[0]
        assert [node.node_name for node in root.leaves] == ["A(1);x", "B;2", "C"]
        assert root.get_attr("length") is None
        assert root["inner"].get_attr("length") == 3.4

    def test_clean_newick_files_file_order(self, tmp_path):
        first = tmp_path / "first.nwk"
        second = tmp_path / "second.nwk"
        first.write_text(QUOTED_NEWICK)
        second.write_text(DUPLICATE_NEWICK)
        roots = construct.clean_newick_files([first, second])
        assert [root.node_name for root in roots] == ["root", "group"]

    def test_clean_newick_mixed_sources(self, tmp_path):
        filepath = tmp_path / "one.nwk"
        filepath.write_text(DUPLICATE_NEWICK)
        roots = newick.clean_newick([filepath, "(D,E)second;"])
        assert [root.node_name for root in roots] == ["group", "second"]

    def test_clean_newick_empty_error(self):
        with pytest.raises(ValueError):
            newick.clean_newick("")

    def test_clean_newick_duplicate_code_error(self):
        text = "(A:0[&&NHX:code=A7],B:1[&&NHX:code=A7])root;"
        with pytest.raises(exceptions.NewickError) as exc_info:
            newick.clean_newick(text)
        error = exc_info.value
        assert error.tree_index == 1
        assert error.label == "B"
        assert error.code == "A7"
        assert (error.line, error.column) == (1, 21)
        assert "Duplicate sample code" in str(error)

    def test_clean_newick_duplicate_code_allowed(self):
        text = "(A:0[&&NHX:code=A7],B:1[&&NHX:code=A7])root;"
        root = newick.clean_newick(text, allow_duplicate_codes=True)[0]
        assert [node.get_attr("code") for node in root.leaves] == ["A7", "A7"]

    def test_clean_newick_code_error_second_file(self, tmp_path):
        first = tmp_path / "first.nwk"
        second = tmp_path / "second.nwk"
        first.write_text("(A,B)one;\n")
        second.write_text("(C[&&NHX:code=S1],D[&&NHX:code=S1])two;\n")
        with pytest.raises(exceptions.NewickError) as exc_info:
            newick.clean_newick([first, second])
        error = exc_info.value
        assert error.tree_index == 2
        assert error.label == "D"
        assert error.source == str(second)
        assert str(second) in str(error)
        assert "tree 2" in str(error)

    def test_clean_newick_unclosed_quote_error(self, tmp_path):
        filepath = tmp_path / "broken.nwk"
        filepath.write_text("(('A:1,B:2)root;\n")
        with pytest.raises(exceptions.NewickError) as exc_info:
            newick.clean_newick(filepath)
        error = exc_info.value
        assert error.tree_index == 1
        assert error.line == 1
        assert error.column == 3
        assert error.label == "A:1,B:2)root;"
        assert error.source == str(filepath)

    def test_clean_newick_error_is_value_error(self):
        assert issubclass(exceptions.NewickError, ValueError)


class TestNewickRenameRoundTrip:
    def test_rename_leaf_by_code_and_export(self):
        root = newick.clean_newick(DUPLICATE_NEWICK)[0]
        target = [
            node
            for node in root.leaves
            if node.get_attr("code") == "A7" and node.node_name == "A"
        ]
        assert len(target) == 1
        target[0].rename("A7_renamed")

        text = tree_to_newick(root, length_attr="length", attr_list=["code"])
        assert text == "(A7_renamed:0[&&NHX:code=A7],A:1.5[&&NHX:code=A8])group"

        reloaded = newick.clean_newick(text + ";")[0]
        assert [child.node_name for child in reloaded.children] == [
            "A7_renamed",
            "A",
        ]
        assert [child.get_attr("code") for child in reloaded.children] == [
            "A7",
            "A8",
        ]
        assert [child.get_attr("length") for child in reloaded.children] == [0, 1.5]

    def test_zero_and_absent_length_round_trip(self):
        root = Node("root")
        _ = Node("zero", parent=root, length=0)
        _ = Node("absent", parent=root)

        text = tree_to_newick(root, length_attr="length")
        assert text == "(zero:0,absent)root"

        reloaded = newick.clean_newick(text)[0]
        assert reloaded["zero"].get_attr("length") == 0
        assert reloaded["absent"].get_attr("length") is None

    def test_tree_wrapper_round_trip(self):
        tree = Tree(newick.clean_newick(QUOTED_NEWICK)[0])
        text = tree.to_newick(length_attr="length", attr_list=["code", "note"])
        reloaded = Tree.from_newick(text)
        assert reloaded.node["inner"].get_attr("note") == "foo"
        assert [node.get_attr("length") for node in reloaded.node.leaves] == [
            1.2,
            0.0,
            5,
        ]

    def test_export_duplicate_labels(self):
        root = newick.clean_newick(DUPLICATE_NEWICK)[0]
        text = tree_to_newick(root, length_attr="length", attr_list=["code"])
        assert text == "(A:0[&&NHX:code=A7],A:1.5[&&NHX:code=A8])group"
