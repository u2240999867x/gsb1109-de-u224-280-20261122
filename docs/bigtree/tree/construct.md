---
title: Tree Construct
---

# ✨ Construct

## Tree Construct Methods

Construct Tree from list, dictionary, and pandas/polars DataFrame.

To decide which method to use, consider your data type and data values.

| Construct tree from | Using full path     | Using parent-child relation                      | Add node attributes |
|---------------------|---------------------|--------------------------------------------------|---------------------|
| List                | `list_to_tree`      | `list_to_tree_by_relation`                       | No                  |
| Dictionary          | `dict_to_tree`      | `nested_dict_to_tree`, `nested_dict_key_to_tree` | Yes                 |
| pandas DataFrame    | `dataframe_to_tree` | `dataframe_to_tree_by_relation`                  | Yes                 |
| polars DataFrame    | `polars_to_tree`    | `polars_to_tree_by_relation`                     | Yes                 |
| Interactive UI      | NA                  | `render_tree`                                    | No                  |

| Construct tree from | Notation         | Add node attributes   |
|---------------------|------------------|-----------------------|
| String              | `str_to_tree`    | No                    |
| Newick string       | `newick_to_tree` | Yes                   |
| rich Tree           | `rich_to_tree`   | Only style attributes |

## Newick Cleaning

Use `clean_newick` when files come from different producers and need the same preprocessing. It accepts a filename,
raw Newick text, or a list mixing both, and returns one root per tree in input order.

```python
from bigtree import clean_newick

roots = clean_newick(["lab_a.nwk", "lab_b.nwk"])
```

- Quoted labels containing `(`, `)`, `;`, `,`, or `:` are preserved.
- Multiple trees per file, trailing `;`, internal-node annotations, and branch lengths are supported.
- Duplicate sibling labels are kept when their sample codes differ. Set `allow_duplicate_names=False` to reject them.
- `code_attr` (default `"code"`) identifies a sample. Duplicate leaf codes raise `NewickError` unless
  `allow_duplicate_codes=True`.
- `NewickError` carries `source`, `tree_index`, `line`, `column`, `label`, and `code`; `str(error)` reports the tree
  and label together with the file position when a file was read.

Related helpers are `clean_newick_file`, `clean_newick_files`, `newick_to_trees`, `newick_file_to_trees`,
`newick_files_to_trees`, and `read_newick`.

## Tree Add Attributes Methods

To add attributes to an existing tree,

| Add attributes from | Using full path                 | Using node name                 |
|---------------------|---------------------------------|---------------------------------|
| String              | `add_path_to_tree`              | NA                              |
| Dictionary          | `add_dict_to_tree_by_path`      | `add_dict_to_tree_by_name`      |
| pandas DataFrame    | `add_dataframe_to_tree_by_path` | `add_dataframe_to_tree_by_name` |
| polars DataFrame    | `add_polars_to_tree_by_path`    | `add_polars_to_tree_by_name`    |

!!! note

    If attributes are added to existing tree using **full path**, paths that previously did not exist will be added.<br>
    If attributes are added to existing tree using **node name**, names that previously did not exist will not be created.

These functions are not standalone functions.
Under the hood, they have the following dependency,

![Tree Constructor Dependency Diagram](https://github.com/kayjan/bigtree/raw/master/assets/docs/tree_construct.png "Tree Constructor Dependency Diagram")

-----

::: bigtree.tree.construct
