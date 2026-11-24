|:sparkles:| Construct
============================================

Construct Tree from list, dictionary, and pandas DataFrame.

To decide which method to use, consider your data type and data values.

.. list-table:: Tree Construct Methods
   :widths: 35 30 50 30 40
   :header-rows: 1

   * - Construct Tree from
     - Using full path
     - Using parent-child relation
     - Using notation
     - Add node attributes
   * - String
     - `str_to_tree`
     - NA
     - `newick_to_tree`
     - | No (for `str_to_tree`)
       | Yes (for `newick_to_tree`)
   * - List
     - `list_to_tree`
     - `list_to_tree_by_relation`
     - NA
     - No
   * - Dictionary
     - `dict_to_tree`
     - `nested_dict_to_tree`
     - NA
     - Yes
   * - DataFrame
     - `dataframe_to_tree`
     - `dataframe_to_tree_by_relation`
     - NA
     - Yes


To add attributes to existing tree,

.. list-table:: Tree Add Attributes Methods
   :widths: 30 40 30
   :header-rows: 1

   * - Add attributes from
     - Using full path
     - Using node name
   * - String
     - `add_path_to_tree`
     - NA
   * - Dictionary
     - `add_dict_to_tree_by_path`
     - `add_dict_to_tree_by_name`
   * - DataFrame
     - `add_dataframe_to_tree_by_path`
     - `add_dataframe_to_tree_by_name`

.. note::
 | If attributes are added to existing tree using **full path**, paths that previously did not exist will be added.
 | If attributes are added to existing tree using **node name**, names that previously did not exist will not be created.

These functions are not standalone functions.
Under the hood, they have the following dependency,

.. image:: https://github.com/kayjan/bigtree/raw/master/assets/docs/tree_construct.png
   :target: https://github.com/kayjan/bigtree/raw/master/assets/docs/tree_construct.png
   :alt: Tree Constructor Dependency Diagram

Newick Cleaning
---------------

Use `clean_newick` as the single entry point for Newick inputs from different producers. It accepts a filename, raw
Newick text, or a list mixing both, and returns one root per tree in input order.

.. code-block:: python

    from bigtree import clean_newick

    roots = clean_newick(["lab_a.nwk", "lab_b.nwk"])

Quoted labels containing ``(``, ``)``, ``;``, ``,``, or ``:`` are preserved. Multiple trees per file, trailing
``;``, internal-node annotations, and branch lengths are supported. Duplicate sibling labels are kept when their
sample codes differ; duplicate leaf values of `code_attr` (default ``"code"``) raise `NewickError` unless
`allow_duplicate_codes=True`. The error exposes `source`, `tree_index`, `line`, `column`, `label`, and `code`.

.. automodule:: bigtree.tree.construct
   :members:
   :show-inheritance:
