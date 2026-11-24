#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
"""
Transactional handoff editing for decoded :class:`DataElement` trees.

An opt-in editing layer on top of :class:`DataElement`. An editor keeps the
confirmed tree untouched while a :class:`HandoffDraft` is open: every draft owns
an isolated working copy supporting child insertion/removal, attribute and value
changes and whole subtree moves. A draft is either committed (the *final*
composition is validated as a whole and then applied in place) or discarded.

Confirmed objects keep their identity across a successful commit, so references
held by the user interface to the root, a group, a manifest or a detail element
stay valid and immediately expose the new declarations, types and converted
Python values. When the handoff editor is not used, DataElement keeps its
original behavior unchanged.
"""
from copy import copy
from typing import Any, Optional
from xml.etree.ElementTree import Element

from xmlschema.exceptions import XMLSchemaException, XMLSchemaValueError
from xmlschema.names import XSI_NAMESPACE, XSI_TYPE
from xmlschema.utils.decoding import raw_encode_value

from .dataobjects import DataElement
from .validators import XMLSchemaValidationError, XsdElement, XsdSimpleType

__all__ = ['HandoffEditor', 'HandoffDraft', 'HandoffError',
           'HandoffValidationError', 'HandoffChannelConflict']


class HandoffError(XMLSchemaException):
    """Base class for errors raised by the handoff editing layer."""


class HandoffValidationError(HandoffError):
    """
    Raised when a draft cannot be committed because its final composition is
    not schema valid. The confirmed tree is left unchanged and no candidate
    XML is produced.
    """

    def __init__(self, message: str, *, manifest_id: Optional[str] = None,
                 location: Optional[str] = None,
                 errors: Optional[list[XMLSchemaValidationError]] = None):
        super().__init__(message)
        self.manifest_id = manifest_id
        self.location = location
        self.errors = errors or []


class HandoffChannelConflict(HandoffError):
    """
    Raised when the effective transport channel of a manifest landing point
    changed after the draft was prepared. The handoff is never silently
    retargeted to the new channel.
    """

    def __init__(self, message: str, *, manifest_id: Optional[str] = None,
                 location: Optional[str] = None,
                 prepared_channel: Optional[str] = None,
                 current_channel: Optional[str] = None):
        super().__init__(message)
        self.manifest_id = manifest_id
        self.location = location
        self.prepared_channel = prepared_channel
        self.current_channel = current_channel


def _clone_tree(node: DataElement, origin: Optional[DataElement]) -> DataElement:
    """
    Clone the structure and plain data of a DataElement tree, sharing the schema
    components. Each clone points back through ``_origin`` to the confirmed node
    it was copied from (``None`` for nodes created inside a draft).
    """
    clone = DataElement(
        tag=node.tag,
        value=node.value,
        attrib=dict(node.attrib),
        nsmap=dict(node.nsmap),
        xmlns=list(node.xmlns) if node.xmlns is not None else None,
        xsd_element=node.xsd_element,
        xsd_type=node.xsd_type,
    )
    if node._encoder is not None and node._encoder is not node.xsd_element:
        clone._encoder = node._encoder
    clone._origin = origin
    for child in node:
        clone.append(_clone_tree(child, child))
    return clone


def _parent_map(root: DataElement) -> dict[DataElement, DataElement]:
    parents: dict[DataElement, DataElement] = {}
    stack = [root]
    while stack:
        parent = stack.pop()
        for child in parent:
            parents[child] = parent
            stack.append(child)
    return parents


class HandoffDraft:
    """
    An isolated editable composition over a confirmed :class:`DataElement` tree.
    Created through :meth:`HandoffEditor.begin_draft`.
    """

    def __init__(self, editor: 'HandoffEditor') -> None:
        self._editor = editor
        self._channel_attr = editor.channel_attr
        self._discarded = False
        self._committed = False

        # Parallel clones: the editable tree and the begin-of-draft snapshot.
        self.working = _clone_tree(editor.root, editor.root)
        self._snapshot = _clone_tree(editor.root, editor.root)
        self._working_parents = _parent_map(self.working)
        self._snapshot_parents = _parent_map(self._snapshot)

        # Origin identity -> clone. Structural moves relocate the clones but do
        # not change their origin, so these maps stay valid for the draft life.
        self._working_by_origin: dict[int, DataElement] = {
            id(node._origin): node for node in self.working.iter()
            if node._origin is not None
        }
        self._snapshot_by_origin: dict[int, DataElement] = {
            id(node._origin): node for node in self._snapshot.iter()
            if node._origin is not None
        }

    # ------------------------------------------------------------------ #
    # Accessors
    # ------------------------------------------------------------------ #
    @property
    def root(self) -> DataElement:
        """The editable working root (a clone of the confirmed root)."""
        return self.working

    def working_node(self, confirmed_node: DataElement) -> DataElement:
        """Return the editable clone of a confirmed node."""
        try:
            return self._working_by_origin[id(confirmed_node)]
        except KeyError:
            raise XMLSchemaValueError(
                "the node is not part of this draft's confirmed tree"
            ) from None

    def create_element(self, tag: str, value: Any = None,
                       attrib: Optional[dict[str, Any]] = None) -> DataElement:
        """Create a new unbound DataElement to be inserted in the draft."""
        node = DataElement(tag, value=value, attrib=attrib)
        node._origin = None
        return node

    def effective_channel(self, node: DataElement) -> Optional[str]:
        """
        Effective transport channel governing *node* in the working tree. Nodes
        without a local channel follow the nearest declaring ancestor; no local
        value is ever materialized on them.
        """
        return self._channel_of(node, self.working, self._refresh_parents())

    def _refresh_parents(self) -> dict[DataElement, DataElement]:
        self._working_parents = _parent_map(self.working)
        return self._working_parents

    def _channel_of(self, node: DataElement, root: DataElement,
                    parents: dict[DataElement, DataElement]) -> Optional[str]:
        current: Optional[DataElement] = node
        while current is not None:
            channel = current.attrib.get(self._channel_attr)
            if channel is not None:
                return channel
            current = parents.get(current)
        return None

    def _location_path(self, node: DataElement,
                       parents: dict[DataElement, DataElement]) -> str:
        chain: list[str] = []
        current: Optional[DataElement] = node
        while current is not None:
            label = current.local_name
            name = current.attrib.get('name')
            if name is not None:
                label = f"{label}[@name={name!r}]"
            chain.append(label)
            current = parents.get(current)
        return '/' + '/'.join(reversed(chain))

    @staticmethod
    def _manifest_id(node: DataElement) -> Optional[str]:
        for child in node:
            if child.local_name == 'id':
                return None if child.value is None else str(child.value)
        return None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    def discard(self) -> None:
        """Drop the draft without touching the confirmed tree."""
        if self._committed:
            raise XMLSchemaValueError("the draft has already been committed")
        self._discarded = True
        self._editor._release_draft(self)

    def commit(self, accept_current_channel: bool = False) -> None:
        """
        Validate the final composition and apply it to the confirmed tree in
        place. Raises before mutating anything if the composition cannot be
        committed.

        :param accept_current_channel: explicitly accept a landing point whose
            transport channel changed, rebinding the manifest to the current
            channel instead of raising :class:`HandoffChannelConflict`.
        """
        if self._discarded:
            raise XMLSchemaValueError("the draft has been discarded")
        if self._committed:
            raise XMLSchemaValueError("the draft has already been committed")

        # A failed commit never touches the confirmed tree and the draft stays
        # open, so it can be corrected and committed again (optionally accepting
        # the current channel) or explicitly discarded.
        working_parents = self._refresh_parents()
        conflicts = self._detect_channel_conflicts(working_parents)
        if conflicts and not accept_current_channel:
            raise conflicts[0]
        if conflicts:
            # Explicitly adopt the channels currently effective in the confirmed
            # tree for nodes the draft did not modify itself.
            self._sync_external_channels(working_parents)
            working_parents = self._refresh_parents()
        self._bind_unbound_subtrees(working_parents)
        bindings = self._resolve_bindings(working_parents)
        affected = self._compute_affected(bindings, working_parents)
        self._validate_working_tree(bindings, affected, working_parents)
        self._apply_to_confirmed(bindings, affected)
        self._committed = True
        self._editor._release_draft(self)

    # ------------------------------------------------------------------ #
    # Channel conflict detection against the live confirmed tree
    # ------------------------------------------------------------------ #
    def _ancestor_origin_chain(self, node: DataElement,
                               parents: dict[DataElement, DataElement]
                               ) -> list[Optional[int]]:
        chain: list[Optional[int]] = []
        current: Optional[DataElement] = node
        while current is not None:
            chain.append(id(current._origin) if current._origin is not None
                         else None)
            current = parents.get(current)
        return chain

    def _detect_channel_conflicts(self,
                                  working_parents: dict[DataElement, DataElement]
                                  ) -> list[HandoffChannelConflict]:
        conflicts: list[HandoffChannelConflict] = []

        for manifest in self.working.iter():
            if manifest.local_name != 'manifest' or manifest._origin is None:
                continue

            snapshot_manifest = self._snapshot_counterpart(manifest)
            if snapshot_manifest is None:
                continue
            snapshot_chain = self._ancestor_origin_chain(
                snapshot_manifest, self._snapshot_parents)
            working_chain = self._ancestor_origin_chain(
                manifest, working_parents)
            if snapshot_chain == working_chain:
                continue  # structurally not handed off

            prepared_channel = self._channel_of(manifest, self.working,
                                                working_parents)
            current_channel = self._current_channel_of(manifest,
                                                       working_parents)
            if current_channel != prepared_channel:
                conflicts.append(HandoffChannelConflict(
                    "transport channel of the landing point changed from "
                    f"{prepared_channel!r} to {current_channel!r}; the handoff "
                    "is not retargeted automatically",
                    manifest_id=self._manifest_id(manifest),
                    location=self._location_path(manifest, working_parents),
                    prepared_channel=prepared_channel,
                    current_channel=current_channel,
                ))
        return conflicts

    def _node_channel_edited(self, working_node: DataElement) -> bool:
        """Whether this draft changed the node's local channel attribute."""
        snapshot_node = self._snapshot_counterpart(working_node)
        if snapshot_node is None:
            return self._channel_attr in working_node.attrib
        return working_node.attrib.get(self._channel_attr) != \
            snapshot_node.attrib.get(self._channel_attr) or \
            (self._channel_attr in working_node.attrib) != \
            (self._channel_attr in snapshot_node.attrib)

    def _current_channel_of(self, working_node: DataElement,
                            working_parents: dict[DataElement, DataElement]
                            ) -> Optional[str]:
        """
        Effective channel the node would have if applied now. Nodes whose
        channel this draft edited contribute the draft value (possibly absent);
        other nodes contribute the value currently held by the confirmed tree,
        so changes committed concurrently are taken into account.
        """
        current: Optional[DataElement] = working_node
        while current is not None:
            if self._node_channel_edited(current) or current._origin is None:
                channel = current.attrib.get(self._channel_attr)
            else:
                channel = current._origin.attrib.get(self._channel_attr)
            if channel is not None:
                return channel
            current = working_parents.get(current)
        return None

    def _sync_external_channels(self,
                                working_parents: dict[DataElement, DataElement]
                                ) -> None:
        """
        Adopt channel changes already confirmed by others for nodes this draft
        did not modify. Nodes whose channel this draft edited keep the draft
        value. Nodes without a local channel never get a materialized value.
        """
        for working_node in self.working.iter():
            origin = working_node._origin
            if origin is None or self._node_channel_edited(working_node):
                continue
            live_value = origin.attrib.get(self._channel_attr)
            if live_value is None:
                working_node.attrib.pop(self._channel_attr, None)
            else:
                working_node.attrib[self._channel_attr] = live_value

    def _snapshot_counterpart(self,
                              working_node: DataElement) -> Optional[DataElement]:
        origin = working_node._origin
        if origin is None:
            return None
        return self._snapshot_by_origin.get(id(origin))

    # ------------------------------------------------------------------ #
    # Binding resolution from the final (effective-channel) composition
    # ------------------------------------------------------------------ #
    def _resolve_bindings(self,
                          working_parents: dict[DataElement, DataElement]
                          ) -> dict[DataElement, dict[str, Any]]:
        bindings: dict[DataElement, dict[str, Any]] = {}

        for manifest in self.working.iter():
            if manifest.local_name != 'manifest':
                continue
            parent = working_parents.get(manifest)
            local_decl = self._match_child_declaration(parent, manifest.tag)
            if local_decl is None or not local_decl.alternatives:
                continue

            channel = self._channel_of(manifest, self.working, working_parents)
            inherited = {self._channel_attr: channel} if channel is not None else {}
            expected_type = local_decl.get_alternative_type(
                Element(manifest.tag), inherited
            )

            explicit_type = self._explicit_xsi_type(
                manifest, expected_type, local_decl, working_parents
            )
            target_type = explicit_type if explicit_type is not None \
                else expected_type

            bindings[manifest] = {
                'manifest_decl': local_decl,
                'manifest_type': target_type,
                'channel': channel,
            }
        return bindings

    def _match_child_declaration(self, parent: Optional[DataElement],
                                 tag: str) -> Optional[XsdElement]:
        if parent is None or parent.xsd_element is None:
            return None
        model_group = parent.xsd_element.type.model_group
        if model_group is None:
            return None
        declaration = model_group.match_element(tag)
        return declaration if isinstance(declaration, XsdElement) else None

    def _bind_unbound_subtrees(self,
                               working_parents: dict[DataElement, DataElement]
                               ) -> None:
        """
        Bind nodes created inside the draft (which start without schema
        bindings) top-down, resolving each declaration from the model group of
        its already-bound parent. Bound nodes are left untouched.
        """
        def visit(node: DataElement) -> None:
            for child in node:
                if not isinstance(child, DataElement):
                    continue
                if child.xsd_element is None:
                    declaration = self._match_child_declaration(node, child.tag)
                    if declaration is not None:
                        child._set_schema_binding(
                            declaration, declaration.type,
                            encoder=declaration)
                visit(child)

        visit(self.working)

    def _explicit_xsi_type(self, manifest: DataElement, expected_type: Any,
                           local_decl: XsdElement,
                           working_parents: dict[DataElement, DataElement]
                           ) -> Optional[Any]:
        type_name = self._xsi_type_value(manifest)
        if type_name is None:
            return None
        try:
            return local_decl.maps.get_instance_type(
                type_name, expected_type, manifest.nsmap
            )
        except (KeyError, TypeError) as err:
            raise HandoffValidationError(
                f"xsi:type {type_name!r} cannot be used at the new location: {err}",
                manifest_id=self._manifest_id(manifest),
                location=self._location_path(manifest, working_parents),
            ) from err

    @staticmethod
    def _xsi_type_value(node: DataElement) -> Optional[str]:
        if XSI_TYPE in node.attrib:
            value = node.attrib[XSI_TYPE]
            return None if value is None else str(value).strip()
        value = node.attrib.get('xsi:type')
        if value is not None:
            return str(value).strip()
        for key, val in node.attrib.items():
            if key.startswith(f'{{{XSI_NAMESPACE}}}'):
                return None if val is None else str(val).strip()
        return None

    # ------------------------------------------------------------------ #
    # Validate the final composition only
    # ------------------------------------------------------------------ #
    def _compute_affected(self,
                          bindings: dict[DataElement, dict[str, Any]],
                          working_parents: dict[DataElement, DataElement]
                          ) -> set[int]:
        """
        Compute, before any working-tree mutation, the manifests this draft is
        responsible for. The result is reused for both validation and
        reconciliation so adopting concurrent changes can never flip it.
        """
        affected: set[int] = set()
        for manifest in bindings:
            snapshot_manifest = self._snapshot_counterpart(manifest)
            if snapshot_manifest is None:
                affected.add(id(manifest))
            elif self._manifest_affected(manifest, snapshot_manifest,
                                         working_parents):
                affected.add(id(manifest))
        return affected

    def _validate_working_tree(self,
                               bindings: dict[DataElement, dict[str, Any]],
                               affected: set[int],
                               working_parents: dict[DataElement, DataElement]
                               ) -> None:
        for manifest, info in bindings.items():
            if id(manifest) in affected:
                # Manifests this draft changes get their new bindings and the
                # staged values are converted to the new Python types.
                self._rebind_manifest(manifest, info['manifest_decl'],
                                      info['manifest_type'], convert=True)
            elif manifest._origin is not None:
                # Untouched manifests adopt the live confirmed bindings/values,
                # so changes committed by concurrent drafts are both validated
                # and left untouched on reconciliation.
                self._adopt_confirmed_binding(manifest)

        errors = list(self.working.iter_errors())
        if errors:
            manifest, location = self._nearest_manifest_error(
                errors, affected, working_parents)
            if manifest is not None:
                raise HandoffValidationError(
                    "handoff composition is not valid for manifest "
                    f"{self._manifest_id(manifest)!r} at {location}",
                    manifest_id=self._manifest_id(manifest),
                    location=location,
                    errors=errors,
                )
            raise HandoffValidationError(
                "handoff composition is not schema valid", errors=errors
            )

    def _adopt_confirmed_binding(self, working_node: DataElement) -> None:
        origin = working_node._origin
        if origin is None:
            return
        working_node._set_schema_binding(
            origin.xsd_element, origin.xsd_type, origin._encoder)
        working_node.attrib = dict(origin.attrib)
        working_node.value = origin.value
        for working_child, origin_child in zip(list(working_node), list(origin)):
            if isinstance(working_child, DataElement) and \
                    isinstance(origin_child, DataElement):
                self._adopt_confirmed_binding(working_child)

    def _nearest_manifest_error(
            self, errors: list[XMLSchemaValidationError],
            affected: set[int],
            working_parents: dict[DataElement, DataElement]
    ) -> tuple[Optional[DataElement], Optional[str]]:
        manifests = [n for n in self.working.iter()
                     if n.local_name == 'manifest']
        for error in errors:
            for manifest in manifests:
                if self._error_concerns(error, manifest, working_parents):
                    return manifest, self._location_path(
                        manifest, working_parents)

        # Encode errors carry an ElementTree element (e.g. a bad fee child),
        # so attribute the failure by validating the manifests this draft
        # changed. Only those have an encoder pinned to their target type and
        # therefore validate standalone through the current location.
        for manifest in manifests:
            if id(manifest) in affected and not manifest.is_valid():
                return manifest, self._location_path(manifest, working_parents)
        return None, None

    def _error_concerns(self, error: XMLSchemaValidationError,
                        manifest: DataElement,
                        working_parents: dict[DataElement, DataElement]) -> bool:
        elem = getattr(error, 'elem', None)
        if elem is not None and getattr(elem, 'tag', None) == manifest.tag:
            return True
        obj = getattr(error, 'obj', None)
        if isinstance(obj, DataElement):
            current: Optional[DataElement] = obj
            while current is not None:
                if current is manifest:
                    return True
                current = working_parents.get(current)
        return False

    # ------------------------------------------------------------------ #
    # Rebinding a manifest and its descendants
    # ------------------------------------------------------------------ #
    def _pinned_encoder(self, manifest_decl: XsdElement,
                        manifest_type: Any) -> XsdElement:
        """
        Build an element declaration pinned to *manifest_type*. Whole-tree
        encoding still resolves type alternatives through the inherited
        channel, but validating/encoding a manifest DataElement on its own has
        no ancestor context; the pinned encoder makes those operations use the
        declaration of its current location.
        """
        encoder = copy(manifest_decl)
        encoder.alternatives = ()
        encoder._set_type(manifest_type)
        return encoder

    def _rebind_manifest(self, manifest: DataElement,
                         manifest_decl: XsdElement, manifest_type: Any,
                         convert: bool) -> None:
        encoder = self._pinned_encoder(manifest_decl, manifest_type)
        manifest._set_schema_binding(manifest_decl, manifest_type,
                                     encoder=encoder)
        self._rebind_children(manifest, manifest_type.model_group, convert)

    def _rebind_children(self, node: DataElement,
                         model_group: Any, convert: bool) -> None:
        if model_group is None:
            return
        for child in node:
            if not isinstance(child, DataElement):
                continue
            matched = model_group.match_element(child.tag)
            if not isinstance(matched, XsdElement):
                continue
            previous_type = child.xsd_type
            child._set_schema_binding(matched, matched.type, encoder=matched)
            type_changed = previous_type is not matched.type
            if convert and type_changed and not len(child) \
                    and child.value is not None \
                    and isinstance(matched.type, XsdSimpleType):
                child.value = self._convert_value(child.value, matched.type)
            self._rebind_children(child, matched.type.model_group, convert)

    @staticmethod
    def _convert_value(value: Any, xsd_type: XsdSimpleType) -> Any:
        lexical = value if isinstance(value, str) else raw_encode_value(value)
        if lexical is None:
            return value
        return xsd_type.text_decode(lexical, 'skip')

    # ------------------------------------------------------------------ #
    # Reconcile onto the confirmed tree (identities preserved)
    # ------------------------------------------------------------------ #
    def _apply_to_confirmed(self,
                            bindings: dict[DataElement, dict[str, Any]],
                            affected: set[int]) -> None:
        self._reconcile_structure()
        self._reconcile_attributes_and_values(affected)
        self._reconcile_bindings(bindings, affected)

    def _reconcile_structure(self) -> None:
        # Rebuild the child list of every confirmed parent whose working
        # counterpart changed. New draft nodes are adopted as-is. Signatures
        # use origin identities so unchanged clone trees compare equal.
        for working_parent in self.working.iter():
            original = working_parent._origin
            if original is None:
                continue
            snapshot_parent = self._snapshot_counterpart(working_parent)
            working_sig = [id(c._origin) if c._origin is not None else id(c)
                           for c in working_parent]
            snapshot_sig = [id(c._origin) if c._origin is not None else id(c)
                            for c in snapshot_parent] \
                if snapshot_parent is not None else []
            if working_sig == snapshot_sig:
                continue
            original._children = [
                child._origin if child._origin is not None else child
                for child in working_parent
            ]

    def _protected_node_ids(self, affected: set[int]) -> set[int]:
        """Working nodes belonging to manifests this draft did not affect."""
        protected: set[int] = set()
        for manifest in self.working.iter():
            if manifest.local_name != 'manifest' or id(manifest) in affected:
                continue
            for node in manifest.iter():
                protected.add(id(node))
        return protected

    def _reconcile_attributes_and_values(self, affected: set[int]) -> None:
        protected = self._protected_node_ids(affected)
        for working_node in self.working.iter():
            if id(working_node) in protected:
                continue
            original = working_node._origin
            if original is None:
                continue
            snapshot_node = self._snapshot_counterpart(working_node)
            if snapshot_node is None:
                continue

            for key, value in working_node.attrib.items():
                if key not in snapshot_node.attrib or \
                        snapshot_node.attrib[key] != value:
                    original.attrib[key] = value
            for key in list(snapshot_node.attrib):
                if key not in working_node.attrib and key in original.attrib:
                    del original.attrib[key]

            if working_node.value != snapshot_node.value:
                original.value = working_node.value

    def _reconcile_bindings(self,
                            bindings: dict[DataElement, dict[str, Any]],
                            affected: set[int]) -> None:
        for working_manifest, info in bindings.items():
            original = working_manifest._origin
            if original is None:
                continue  # new node already bound in the working tree
            if id(working_manifest) not in affected:
                # Leave manifests this draft did not touch alone, so bindings
                # committed by concurrent drafts are preserved.
                continue
            self._rebind_manifest(original, info['manifest_decl'],
                                  info['manifest_type'], convert=True)

    def _manifest_affected(self, working_manifest: DataElement,
                           snapshot_manifest: DataElement,
                           working_parents: dict[DataElement, DataElement]) -> bool:
        # A handoff (different ancestor origin chain) or an explicit xsi:type
        # change always affects the manifest's binding.
        if self._ancestor_origin_chain(snapshot_manifest,
                                       self._snapshot_parents) != \
                self._ancestor_origin_chain(working_manifest, working_parents):
            return True
        if self._xsi_type_value(snapshot_manifest) != \
                self._xsi_type_value(working_manifest):
            return True
        # Any staged edit inside the manifest (attributes, values or children)
        # keeps it under this draft's responsibility.
        if self._subtree_modified(working_manifest, snapshot_manifest):
            return True
        # A channel edited on this draft on an ancestor (or the manifest itself)
        # can change its effective type.
        current: Optional[DataElement] = working_manifest
        while current is not None:
            if self._node_channel_edited(current):
                return True
            current = working_parents.get(current)
        return False

    def _subtree_modified(self, working_node: DataElement,
                          snapshot_node: DataElement) -> bool:
        if working_node.attrib != snapshot_node.attrib:
            return True
        if working_node.value != snapshot_node.value:
            return True
        if len(working_node) != len(snapshot_node):
            return True
        for working_child, snapshot_child in zip(working_node, snapshot_node):
            if working_child._origin is not snapshot_child._origin:
                return True
            if isinstance(working_child, DataElement) and \
                    isinstance(snapshot_child, DataElement) and \
                    self._subtree_modified(working_child, snapshot_child):
                return True
        return False


class HandoffEditor:
    """
    Optional handoff editing session bound to a confirmed :class:`DataElement`
    tree. When not used, decoded DataElement trees keep their original behavior.
    """

    def __init__(self, root: DataElement, channel_attr: str = 'channel') -> None:
        if not isinstance(root, DataElement):
            raise TypeError("the editor root must be a DataElement instance")
        self._root = root
        self.channel_attr = channel_attr
        self._draft: Optional[HandoffDraft] = None

    @property
    def root(self) -> DataElement:
        """The confirmed tree, never mutated while a draft is open."""
        return self._root

    def has_draft(self) -> bool:
        return self._draft is not None

    def begin_draft(self) -> HandoffDraft:
        """Open a new isolated draft, failing if one is already open."""
        if self._draft is not None:
            raise XMLSchemaValueError("a draft is already open")
        self._draft = HandoffDraft(self)
        return self._draft

    def discard_draft(self) -> None:
        if self._draft is None:
            raise XMLSchemaValueError("no draft is open")
        self._draft.discard()

    def _release_draft(self, draft: HandoffDraft) -> None:
        if self._draft is draft:
            self._draft = None
