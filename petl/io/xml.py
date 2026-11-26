# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
try:
    # prefer lxml as it supports XPath
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

from array import array
from contextlib import contextmanager
from io import BytesIO
from operator import attrgetter
import itertools
import re
from tempfile import TemporaryFile
from xml.parsers import expat
from xml.sax.saxutils import escape as _xml_escape
from string import Formatter
from petl.compat import pickle, string_types, text_type


# internal dependencies
from petl.util.base import Table, fieldnames, iterpeek
from petl.io.sources import read_source_from_arg
from petl.io.text import totext


def fromxml(source, *args, **kwargs):
    """
    Extract data from an XML file. E.g.::

        >>> import petl as etl
        >>> # setup a file to demonstrate with
        ... d = '''<table>
        ...     <tr>
        ...         <td>foo</td><td>bar</td>
        ...     </tr>
        ...     <tr>
        ...         <td>a</td><td>1</td>
        ...     </tr>
        ...     <tr>
        ...         <td>b</td><td>2</td>
        ...     </tr>
        ...     <tr>
        ...         <td>c</td><td>2</td>
        ...     </tr>
        ... </table>'''
        >>> with open('example.file1.xml', 'w') as f:
        ...     f.write(d)
        ...
        212
        >>> table1 = etl.fromxml('example.file1.xml', 'tr', 'td')
        >>> table1
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' | '1' |
        +-----+-----+
        | 'b' | '2' |
        +-----+-----+
        | 'c' | '2' |
        +-----+-----+


    If the data values are stored in an attribute, provide the attribute
    name as an extra positional argument::

        >>> d = '''<table>
        ...     <tr>
        ...         <td v='foo'/><td v='bar'/>
        ...     </tr>
        ...     <tr>
        ...         <td v='a'/><td v='1'/>
        ...     </tr>
        ...     <tr>
        ...         <td v='b'/><td v='2'/>
        ...     </tr>
        ...     <tr>
        ...         <td v='c'/><td v='2'/>
        ...     </tr>
        ... </table>'''
        >>> with open('example.file2.xml', 'w') as f:
        ...     f.write(d)
        ...
        220
        >>> table2 = etl.fromxml('example.file2.xml', 'tr', 'td', 'v')
        >>> table2
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' | '1' |
        +-----+-----+
        | 'b' | '2' |
        +-----+-----+
        | 'c' | '2' |
        +-----+-----+

    Data values can also be extracted by providing a mapping of field
    names to element paths::

        >>> d = '''<table>
        ...     <row>
        ...         <foo>a</foo><baz><bar v='1'/><bar v='3'/></baz>
        ...     </row>
        ...     <row>
        ...         <foo>b</foo><baz><bar v='2'/></baz>
        ...     </row>
        ...     <row>
        ...         <foo>c</foo><baz><bar v='2'/></baz>
        ...     </row>
        ... </table>'''
        >>> with open('example.file3.xml', 'w') as f:
        ...     f.write(d)
        ...
        223
        >>> table3 = etl.fromxml('example.file3.xml', 'row',
        ...                      {'foo': 'foo', 'bar': ('baz/bar', 'v')})
        >>> table3
        +------------+-----+
        | bar        | foo |
        +============+=====+
        | ('1', '3') | 'a' |
        +------------+-----+
        | '2'        | 'b' |
        +------------+-----+
        | '2'        | 'c' |
        +------------+-----+

    If `lxml <http://lxml.de/>`_ is installed, full XPath expressions can be
    used.

    Note that the default implementation is **not** streaming, i.e., the whole
    document is loaded into memory. See ``stream=True`` below for a
    forward-only alternative.

    If multiple elements match a given field, all values are reported as a
    tuple.

    If there is more than one element name used for row values, a tuple
    or list of paths can be provided, e.g.,
    ``fromxml('example.file.html', './/tr', ('th', 'td'))``.

    Optionally a custom parser can be provided, e.g.::

        >>> from lxml import etree # doctest: +SKIP
        ... my_parser = etree.XMLParser(resolve_entities=False) # doctest: +SKIP
        ... table4 = etl.fromxml('example.file1.xml', 'tr', 'td', parser=my_parser) # doctest: +SKIP

    For large documents, pass ``stream=True`` to use a forward-only,
    incremental reader with the same field mapping syntax, e.g.
    ``fromxml(source, 'record', mapping, stream=True)``. Records are selected
    by namespace URI (``urn:shipment`` by default, configurable with the
    ``namespace`` argument). Only records having that URI are emitted, and
    prefix changes within the document are handled transparently. The record
    tag defaults to ``'record'``; use ``record='...'`` or pass it as the first
    positional argument, and pass the mapping as the second.

    In streaming mode, a row is produced as soon as its direct same-namespace
    ``detail`` child has closed and all records that started before it have
    been produced. Records without a ``detail`` are completed when the record
    closes. Nested records are emitted too, in record start-tag order. A field
    mapping value may be a path to an element in the record's ``detail``
    (text), or a ``(path, attribute)`` pair; ``('.', attribute)`` reads an
    attribute of the record itself. Paths may be ``detail/...``, and
    ``.//`` descendant paths are supported. Only direct details and elements
    below the record's own detail are considered, so a same-named element
    under a different namespace, or an element attached to another record,
    does not contribute. Rows that are ready but cannot yet be emitted are
    spooled to a temporary file (``tempdir=`` selects its location). Closing
    the returned iterator releases the source and temporary resources
    immediately; a read or XML syntax error at the end of the input is still
    raised after all available rows have been produced.

    The same rules as the tree reader apply to values: no matching element
    yields ``missing`` (``None`` by default), one matching element yields its
    text or attribute, and several matching elements yield a tuple. Attributes
    with no value yield ``None``.

    """

    streaming = kwargs.pop('stream', False) or kwargs.pop('streaming', False)
    if streaming:
        source = _as_stream_source(source)
        return XmlStreamView(source, *args, **kwargs)
    source = read_source_from_arg(source)
    return XmlView(source, *args, **kwargs)


def _as_stream_source(source):
    if hasattr(source, 'open') or hasattr(source, 'read'):
        return source
    if isinstance(source, (bytes, bytearray)):
        return BytesIO(source)
    if isinstance(source, string_types):
        return read_source_from_arg(source)
    if hasattr(source, '__iter__'):
        return _IterableReader(source)
    return read_source_from_arg(source)


class _IterableReader(object):
    """Adapt a forward-only iterable of chunks to a binary reader."""

    def __init__(self, iterable):
        self.iterator = iter(iterable)
        self.done = False

    def read(self, size=-1):
        if self.done:
            return b''
        try:
            chunk = next(self.iterator)
        except StopIteration:
            self.done = True
            return b''
        if isinstance(chunk, text_type):
            return chunk.encode('utf-8')
        return chunk

    def close(self):
        self.done = True
        close = getattr(self.iterator, 'close', None)
        if close is not None:
            close()


def fromxmlstream(source, *args, **kwargs):
    """Explicit streaming variant of :func:`fromxml`.

    Equivalent to calling :func:`fromxml` with ``stream=True``.
    """
    kwargs['stream'] = True
    return fromxml(source, *args, **kwargs)


fromxml_stream = fromxmlstream


class XmlView(Table):

    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.args = args
        if len(args) == 2 and isinstance(args[1], (string_types, tuple, list)):
            self.rmatch = args[0]
            self.vmatch = args[1]
            self.vdict = None
            self.attr = None
        elif len(args) == 2 and isinstance(args[1], dict):
            self.rmatch = args[0]
            self.vmatch = None
            self.vdict = args[1]
            self.attr = None
        elif len(args) == 3:
            self.rmatch = args[0]
            self.vmatch = args[1]
            self.vdict = None
            self.attr = args[2]
        else:
            assert False, 'bad parameters'
        self.missing = kwargs.get('missing', None)
        self.user_parser = kwargs.get('parser', None)

    def __iter__(self):
        vmatch = self.vmatch
        vdict = self.vdict

        with self.source.open('rb') as xmlf:
            parser2 = _create_xml_parser(self.user_parser)
            tree = etree.parse(xmlf, parser=parser2)
            if not hasattr(tree, 'iterfind'):
                # Python 2.6 compatibility
                tree.iterfind = tree.findall

            if vmatch is not None:
                # simple case, all value paths are the same
                for rowelm in tree.iterfind(self.rmatch):
                    if self.attr is None:
                        getv = attrgetter('text')
                    else:
                        getv = lambda e: e.get(self.attr)
                    if isinstance(vmatch, string_types):
                        # match only one path
                        velms = rowelm.findall(vmatch)
                    else:
                        # match multiple paths
                        velms = itertools.chain(*[rowelm.findall(enm)
                                                  for enm in vmatch])
                    yield tuple(getv(velm)
                                for velm in velms)

            else:
                # difficult case, deal with different paths for each field

                # determine output header
                flds = tuple(sorted(map(text_type, vdict.keys())))
                yield flds

                # setup value getters
                vmatches = dict()
                vgetters = dict()
                for f in flds:
                    vmatch = self.vdict[f]
                    if isinstance(vmatch, string_types):
                        # match element path
                        vmatches[f] = vmatch
                        vgetters[f] = element_text_getter(self.missing)
                    else:
                        # match element path and attribute name
                        vmatches[f] = vmatch[0]
                        attr = vmatch[1]
                        vgetters[f] = attribute_text_getter(attr, self.missing)

                # determine data rows
                for rowelm in tree.iterfind(self.rmatch):
                    yield tuple(vgetters[f](rowelm.findall(vmatches[f]))
                                for f in flds)


_DEFAULT_STREAM_NAMESPACE = 'urn:shipment'
_NS_SEPARATOR = '\x1f'
_STREAM_CHUNK = 65536
_STREAM_COMPACT = 65536
_RECORD_OPEN = 0
_RECORD_READY = 1
_RECORD_DONE = 2


class XmlStreamView(Table):

    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.namespace = kwargs.pop('namespace', _DEFAULT_STREAM_NAMESPACE)
        if self.namespace is None:
            self.namespace = kwargs.pop('ns', None)
        if self.namespace in ('', '*'):
            self.namespace = None
        self.missing = kwargs.pop('missing', None)
        self.tempdir = kwargs.pop('tempdir', None)
        record_tag = kwargs.pop('record', None)
        record_tag = kwargs.pop('row', record_tag)
        record_tag = kwargs.pop('tag', record_tag)
        mapping = kwargs.pop('mapping', None)
        if mapping is None:
            mapping = kwargs.pop('columns', None)
        if mapping is None:
            mapping = kwargs.pop('fields', None)
        remaining = list(args)
        if remaining and isinstance(remaining[0], dict) and mapping is None:
            mapping = remaining.pop(0)
        if remaining and isinstance(remaining[0], string_types):
            record_tag = remaining[0]
            remaining.pop(0)
        if remaining and isinstance(remaining[0], dict) and mapping is None:
            mapping = remaining[0]
        if mapping is None:
            raise ValueError('stream=True requires a field mapping')
        self.record_tag = _normalize_stream_tag(record_tag or 'record')
        self.mapping = mapping
        items = sorted(mapping.items(), key=lambda i: text_type(i[0]))
        self._header = tuple(text_type(k) for k, _ in items)
        self._fields = self._header
        self._field_paths = tuple(_split_stream_path(v, self.record_tag)
                                  for _, v in items)

    def __iter__(self):
        return _stream_rows(self)

    def __len__(self):
        raise TypeError('a streaming XML view has no length')


def _normalize_stream_tag(tag):
    if not isinstance(tag, string_types):
        raise ValueError('stream=True requires a record tag name')
    tag = tag.strip()
    if tag.startswith('{') and '}' in tag:
        tag = tag.split('}', 1)[1]
    tag = tag.replace('.//', '/').replace('//', '/')
    tag = tag.lstrip('./').lstrip('/')
    if '/' in tag:
        tag = tag.rsplit('/', 1)[1]
    if ':' in tag:
        tag = tag.rsplit(':', 1)[1]
    if not tag:
        raise ValueError('stream=True requires a record tag name')
    return tag


def _split_stream_path(value, record_tag=None):
    if isinstance(value, string_types):
        path, attr = value, None
        if path.startswith('@'):
            return (), path[1:], False
        if '/@' in path:
            path, attr = path.rsplit('/@', 1)
    else:
        try:
            path, attr = value
        except (TypeError, ValueError):
            raise ValueError('invalid field mapping value: %r' % (value,))
        if not isinstance(path, string_types):
            raise ValueError('invalid field mapping value: %r' % (value,))
    descendant = './/' in path or '//' in path
    if descendant:
        path = path.replace('.//', '/').replace('//', '/')
    parts = tuple(p for p in path.split('/') if p and p != '.')
    if len(parts) > 1 and parts[0] == 'detail':
        parts = parts[1:]
    return parts, attr, descendant


def _stream_rows(view):
    with _open_stream_source(view.source) as xmlf:
        reader = _StreamingReader(xmlf, view.namespace, view.record_tag,
                                  view._fields, view._field_paths,
                                  view.missing, view.tempdir)
        rows = reader.rows()
        try:
            yield view._header
            for row in rows:
                yield row
        finally:
            rows.close()
            reader.close()


@contextmanager
def _open_stream_source(source):
    opener = getattr(source, 'open', None)
    if opener is None:
        try:
            yield source
        finally:
            close = getattr(source, 'close', None)
            if close is not None:
                close()
        return
    result = opener('rb')
    if hasattr(result, '__enter__'):
        with result as handle:
            yield handle
    else:
        try:
            yield result
        finally:
            close = getattr(result, 'close', None)
            if close is not None:
                close()


class _StreamingReader(object):

    def __init__(self, src, namespace, record_tag, fields, field_paths,
                 missing, tempdir=None):
        self.src = src
        self.namespace = namespace
        self.record_tag = record_tag
        self.fields = fields
        self.field_paths = field_paths
        self.missing = missing
        self.status = bytearray()
        self.offsets = array('Q')
        self.lengths = array('I')
        self.base = 0
        self.records_started = 0
        self.next_emit = 0
        self.spool = TemporaryFile(mode='w+b')
        if tempdir is not None:
            self.spool.close()
            self.spool = TemporaryFile(mode='w+b', dir=tempdir)
        self._closed = False

    def start_record(self):
        seq = self.records_started
        self.records_started += 1
        self.status.append(_RECORD_OPEN)
        self.offsets.append(0)
        self.lengths.append(0)
        return _Record(seq, self)

    def complete(self, record):
        if record.ready:
            return
        sparse = record.sparse()
        payload = pickle.dumps(sparse, protocol=pickle.HIGHEST_PROTOCOL)
        self.spool.seek(0, 2)
        offset = self.spool.tell()
        self.spool.write(payload)
        index = record.seq - self.base
        self.offsets[index] = offset
        self.lengths[index] = len(payload)
        self.status[index] = _RECORD_READY
        record.ready = True
        record.release()

    def _can_emit(self):
        index = self.next_emit - self.base
        return 0 <= index < len(self.status) \
            and self.status[index] == _RECORD_READY

    def _pop(self):
        index = self.next_emit - self.base
        self.spool.seek(self.offsets[index])
        sparse = pickle.loads(self.spool.read(self.lengths[index]))
        missing = self.missing
        row = tuple(missing if field not in sparse else sparse[field]
                    for field in self.fields)
        self.status[index] = _RECORD_DONE
        self.next_emit += 1
        self._compact()
        return row

    def _compact(self):
        done = self.next_emit - self.base
        if done >= _STREAM_COMPACT and done * 2 >= len(self.status):
            del self.status[:done]
            del self.offsets[:done]
            del self.lengths[:done]
            self.base = self.next_emit

    def rows(self):
        parser = expat.ParserCreate(namespace_separator=_NS_SEPARATOR)
        parser.buffer_text = True
        state = _ParseState(self)
        parser.StartElementHandler = state.start
        parser.EndElementHandler = state.end
        parser.CharacterDataHandler = state.chars
        reader = getattr(self.src, 'read1', None) or self.src.read
        error = None
        try:
            while True:
                while self._can_emit():
                    yield self._pop()
                try:
                    data = reader(_STREAM_CHUNK)
                except Exception as read_error:
                    error = read_error
                    break
                if not data:
                    break
                try:
                    parser.Parse(data, False)
                except expat.ExpatError as parse_error:
                    error = _xml_parse_error(parse_error)
                    break
            if error is None:
                try:
                    parser.Parse(b'', True)
                except expat.ExpatError as parse_error:
                    error = _xml_parse_error(parse_error)
            while self._can_emit():
                yield self._pop()
            if error is not None:
                raise error
            if state.has_open_records():
                raise ValueError('unterminated record in XML stream')
        finally:
            self.close()

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self.spool is not None:
            try:
                self.spool.close()
            finally:
                self.spool = None
        self.status = bytearray()
        self.offsets = array('Q')
        self.lengths = array('I')


class _ParseState(object):

    def __init__(self, reader):
        self.reader = reader
        self.stack = []
        self.active = []
        self.open_records = 0

    def has_open_records(self):
        return self.open_records > 0

    def start(self, qname, attrs):
        ns, local = _split_qname(qname)
        parent = self.stack[-1] if self.stack else None
        if parent is not None:
            parent.had_child = True
        frame = _Frame(ns, local, attrs, parent)
        is_record = self._in_scope(ns) and local == self.reader.record_tag

        if is_record:
            record = self.reader.start_record()
            record.ns = ns
            record.frame = frame
            record.attrs = attrs
            frame.record = record
            self.open_records += 1
            self._own_values(record, attrs, frame)

        if parent is not None and self._in_scope(ns) and local == 'detail':
            owner = parent.record
            if owner is not None and owner.frame is parent \
                    and owner.detail_frame is None:
                owner.detail_frame = frame
                frame.owner = owner
                self.active.append(owner)
                for field, parts, attr, descendant in self._paths(owner):
                    if parts and not descendant:
                        if frame.path_matches is None:
                            frame.path_matches = {}
                        frame.path_matches[(owner, field)] = -1

        if parent is not None and parent.path_matches:
            self._nested_values(frame, parent, ns, local, attrs)
        if self.active:
            self._descendant_values(frame, ns, local, attrs)

        self.stack.append(frame)

    def end(self, qname):
        frame = self.stack.pop()
        if frame.text_sinks:
            text = ''.join(frame.text_parts) if frame.text_parts else None
            for record, field in frame.text_sinks:
                record.add(field, text)
        if frame.owner is not None:
            owner = frame.owner
            owner.detail_done = True
            self.active.remove(owner)
            self.reader.complete(owner)
        if frame.record is not None:
            record = frame.record
            record.finished = True
            self.open_records -= 1
            if not record.detail_done:
                self.reader.complete(record)

    def chars(self, data):
        if not self.stack:
            return
        frame = self.stack[-1]
        if frame.text_sinks and not frame.had_child:
            frame.text_parts.append(data)

    def _in_scope(self, ns):
        return self.reader.namespace is None or ns == self.reader.namespace

    def _paths(self, record):
        for field, spec in zip(self.reader.fields, self.reader.field_paths):
            parts, attr, descendant = spec
            yield field, parts, attr, descendant

    def _own_values(self, record, attrs, frame):
        for field, parts, attr, descendant in self._paths(record):
            if parts:
                continue
            if attr is not None:
                record.add(field, _get_attr(attrs, attr))
            else:
                frame.text_sinks.append((record, field))

    def _nested_values(self, frame, parent, ns, local, attrs):
        for record in self.active:
            for field, parts, attr, descendant in self._paths(record):
                if descendant or not parts:
                    continue
                pos = parent.path_matches.get((record, field))
                if pos is None or len(parts) <= pos + 1:
                    continue
                if not _match(parts[pos + 1], ns, local, record.ns):
                    continue
                if frame.path_matches is None:
                    frame.path_matches = {}
                pos += 1
                frame.path_matches[(record, field)] = pos
                if pos == len(parts) - 1:
                    self._terminal(record, field, attr, attrs, frame)

    def _descendant_values(self, frame, ns, local, attrs):
        for record in self.active:
            for field, parts, attr, descendant in self._paths(record):
                if not descendant or not parts:
                    continue
                if _match(parts[-1], ns, local, record.ns) \
                        and _ancestors(parts[:-1], frame, record):
                    self._terminal(record, field, attr, attrs, frame)

    @staticmethod
    def _terminal(record, field, attr, attrs, frame):
        if attr is not None:
            record.add(field, _get_attr(attrs, attr))
        else:
            frame.text_sinks.append((record, field))


class _Frame(object):

    __slots__ = ('ns', 'local', 'attrs', 'parent', 'record', 'owner',
                 'path_matches', 'text_sinks', 'text_parts', 'had_child')

    def __init__(self, ns, local, attrs, parent):
        self.ns = ns
        self.local = local
        self.attrs = attrs
        self.parent = parent
        self.record = None
        self.owner = None
        self.path_matches = None
        self.text_sinks = []
        self.text_parts = []
        self.had_child = False


class _Record(object):

    __slots__ = ('seq', 'reader', 'ns', 'frame', 'attrs', 'detail_frame',
                 'detail_done', 'finished', 'values', 'ready')

    def __init__(self, seq, reader):
        self.seq = seq
        self.reader = reader
        self.ns = None
        self.frame = None
        self.attrs = None
        self.detail_frame = None
        self.detail_done = False
        self.finished = False
        self.values = {}
        self.ready = False

    def add(self, field, value):
        current = self.values.get(field)
        if current is None:
            self.values[field] = [value]
        else:
            current.append(value)

    def row(self):
        row = []
        for field in self.reader.fields:
            values = self.values.get(field)
            if not values:
                row.append(self.reader.missing)
            elif len(values) == 1:
                row.append(values[0])
            else:
                row.append(tuple(values))
        return tuple(row)

    def sparse(self):
        return dict((field, values[0] if len(values) == 1 else tuple(values))
                    for field, values in self.values.items())

    def release(self):
        self.values = {}
        self.frame = None
        self.attrs = None
        self.detail_frame = None


def _split_qname(qname):
    if _NS_SEPARATOR in qname:
        ns, local = qname.split(_NS_SEPARATOR, 1)
        return ns, local
    return '', qname


def _xml_parse_error(error):
    try:
        from xml.etree.ElementTree import ParseError
    except ImportError:
        return error
    return ParseError(str(error))


def _get_attr(attrs, name):
    if name in attrs:
        return attrs[name]
    if name.startswith('{') and '}' in name:
        ns, local = name[1:].split('}', 1)
        return attrs.get(ns + _NS_SEPARATOR + local)
    return None


def _match(token, ns, local, record_ns):
    if token == '*':
        return True
    if token.startswith('{') and '}' in token:
        token_ns, token = token[1:].split('}', 1)
        return token == local and token_ns == ns
    if ':' in token:
        token = token.rsplit(':', 1)[1]
    if token != local:
        return False
    return record_ns is None or ns == record_ns


def _ancestors(prefix, frame, record):
    index = len(prefix) - 1
    node = frame.parent
    while node is not None and node is not record.detail_frame:
        if index < 0:
            return True
        if _match(prefix[index], node.ns, node.local, record.ns):
            index -= 1
        node = node.parent
    return index < 0


def _create_xml_parser(user_parser):
    if user_parser is not None:
        return user_parser
    try:
        # Default lxml parser.
        # This will throw an error if parser is not set and lxml could not be imported
        # because Python's built XML parser doesn't like the `resolve_entities` kwarg.
        # return etree.XMLParser(resolve_entities=False)
        return etree.XMLParser(resolve_entities=False)
    except TypeError:
        # lxml not available
        return None


def element_text_getter(missing):
    def _get(v):
        if len(v) > 1:
            return tuple(e.text for e in v)
        elif len(v) == 1:
            return v[0].text
        else:
            return missing
    return _get


def attribute_text_getter(attr, missing):
    def _get(v):
        if len(v) > 1:
            return tuple(e.get(attr) for e in v)
        elif len(v) == 1:
            return v[0].get(attr)
        else:
            return missing
    return _get


def toxml(table, target=None,
          root=None, head=None, rows=None, prologue=None, epilogue=None,
          style='tag', encoding='utf-8', escape=False):
    """
    Write the table into a new xml file according to elements defined in the
    function arguments.

    The `root`, `head` and `rows` (string, optional) arguments define the tags
    and the nesting of the xml file. Each one defines xml elements with tags
    separated by slashes (`/`) like in `root/level/tag`. They can have a
    arbitrary number of tags that will reflect in more nesting levels for the
    header or record/row written in the xml file.

    For details on tag naming and nesting rules check xml `specification`_ or
    xml `references`_.

    The `rows` argument define the elements for each row of data to be written
    in the xml file. When specified, it must have at least 2 tags for defining
    the tags for `row/column`. Additional tags will add nesting enclosing all
    records/rows/lines.

    The `head` argument is similar to the rows, but aplies only to one line/row
    of header with fieldnames. When specified, it must have at least 2 tags for
    `fields/name` and the remaining will increase nesting.

    The `root` argument defines the elements enclosing `head` and `rows` and is
    required when using `head` for specifying valid xml documents.

    When none of this arguments are specified, they will default to tags that
    generate output similar to a html table:
    `root='table', head='there/tr/td', rows='tbody/tr/td'`.

    The `prologue` argument (string, optional) could be a snippet of valid xml
    that will be inserted before other elements in the xml. It can optionally
    specify the `XML Prolog` of the file.

    The `epilogue` argument (string, optional) could be a snippet of valid xml
    that will be inserted after all other xml elements except the root closing
    tag. It must specify a closing tag if the `root` argument is not specified. 

    The `style` argument select the format of the elements in the xml file. It
    can be `tag` (default), `name`, `attribute` or a custom string to format
    each row via
    `str.format <http://docs.python.org/library/stdtypes.html#str.format>`_.

    Example usage for writing files::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2]]
        >>> etl.toxml(table1, 'example.file4.xml')
        >>> # see what we did is similar a html table:
        >>> print(open('example.file4.xml').read())
        <?xml version="1.0" encoding="UTF-8"?>
        <table><thead>
         <tr><th>foo</th><th>bar</th></tr>
        </thead><tbody>
         <tr><td>a</td><td>1</td></tr>
         <tr><td>b</td><td>2</td></tr>
        </tbody></table>
        >>> # define the nesting in xml file:
        >>> etl.toxml(table1, 'example.file5.xml', rows='plan/line/cell')
        >>> print(open('example.file5.xml').read())
        <?xml version="1.0" encoding="UTF-8"?>
        <plan>
         <line><cell>a</cell><cell>1</cell></line>
         <line><cell>b</cell><cell>2</cell></line>
        </plan>
        >>> # choose other style:
        >>> etl.toxml(table1, 'example.file6.xml', rows='row/col', style='attribute')
        >>> print(open('example.file6.xml').read())
        <?xml version="1.0" encoding="UTF-8"?>
        <row>
         <col foo="a" bar="1" />
         <col foo="b" bar="2" />
        </row>
        >>> etl.toxml(table1, 'example.file6.xml', rows='row/col', style='name')
        >>> print(open('example.file6.xml').read())
        <?xml version="1.0" encoding="UTF-8"?>
        <row>
         <col><foo>a</foo><bar>1</bar></col>
         <col><foo>b</foo><bar>2</bar></col>
        </row>

    By default, values and header text are written unchanged for compatibility.
    Set `escape=True` to escape literal values in the built-in styles and
    field names written as text in the `head` element. Markup and already
    escaped entities in those values are treated as literal text too.
    XML 1.0-forbidden control characters in escaped values or header text
    raise `ValueError`, as do U+FFFE and U+FFFF.
    This does not make field names valid XML element or attribute names.
    Values substituted into custom `style` templates, and markup in
    `prologue` and `epilogue`, are written unchanged even with `escape=True`.

    For example, to write an ampersand as literal text::

        >>> sink = etl.MemorySource()
        >>> etl.toxml([['label'], ['R&D']], sink, escape=True)
        >>> b'<td>R&amp;D</td>' in sink.getvalue()
        True

    .. versionchanged:: 1.7.31

        Added the `escape` option.

    The `toxml()` function is just a wrapper over :func:`petl.io.text.totext`.
    For advanced cases use a template with `totext()` for generating xml files.

    .. versionadded:: 1.7.0

    .. _specification: https://www.w3.org/TR/xml/
    .. _references: https://www.w3schools.com/xml/xml_syntax.asp

    """
    if not root and not head and not rows:
        root = 'table'
        head = 'thead/tr/th'
        rows = 'tbody/tr/td'

    sample, table2 = iterpeek(table, 2)
    props = fieldnames(sample)

    head_props = ([_escape_xml_text(f, {'\r': '&#13;'}) for f in props]
                  if escape and head else props)
    top = _build_xml_header(style, head_props, root, head, rows, prologue, encoding)
    template = _build_cols(style, props, rows, True)
    bottom = _build_xml_footer(style, epilogue, rows, root)

    if escape and style in ('tag', 'name', 'attribute'):
        entities = {'\r': '&#13;'}
        if style == 'attribute':
            entities.update({'"': '&quot;', '\n': '&#10;', '\t': '&#9;'})
        template = _XmlTemplate(template, entities)

    totext(table2, source=target, encoding=encoding, errors='strict',
           template=template, prologue=top, epilogue=bottom)


# List forbidden XML 1.0 controls and U+FFFE/U+FFFF explicitly for CodeQL.
_invalid_xml_chars = re.compile(
    u'[\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c'
    u'\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19'
    u'\x1a\x1b\x1c\x1d\x1e\x1f\ufffe\uffff]')


def _escape_xml_text(value, entities):
    """Escape XML text, rejecting characters without an XML representation."""
    invalid = _invalid_xml_chars.search(value)
    if invalid is not None:
        raise ValueError('XML 1.0 cannot represent character U+%04X' %
                         ord(invalid.group()))
    return _xml_escape(value, entities)


class _XmlTemplate(object):
    """Escape formatted fields without changing the writer's row semantics."""

    def __init__(self, template, entities):
        self.template = template
        self.entities = entities
        self.formatter = Formatter()
        self.parts = None

    def format(*args, **kwargs):
        # Match str.format's positional-only receiver on Python 2 as well.
        self = args[0]
        if self.parts is None:
            self.parts = tuple(self.formatter.parse(self.template))
        output = []
        for literal, field, spec, conversion in self.parts:
            output.append(literal)
            if field is None:
                continue
            # Empty fields also refer to unavailable positional arguments.
            field = '0' if field == '' else field
            value, _ = self.formatter.get_field(field, (), kwargs)
            if conversion is not None:
                value = self.formatter.convert_field(value, conversion)
            if spec:
                spec = self.formatter.vformat(spec, (), kwargs)
            output.append(_escape_xml_text(format(value, spec), self.entities))
        return ''.join(output)


def _build_xml_header(style, props, root, head, rows, prologue, encoding):
    tab = _build_nesting(root, False, None) if root else ''
    nested = -1 if style in ('attribute', 'name') else -2
    if head:
        th1 = _build_nesting(head, False, nested)
        col = _build_cols(style, props, head, False)
        th2 = _build_nesting(head, True, nested)
        thd = '{0}\n{1}{2}'.format(th1, col, th2)
    else:
        thd = ''
    tbd = _build_nesting(rows, False, nested)
    if prologue and prologue.startswith('<?xml'):
        thb = '{0}{1}{2}\n'.format(tab, thd, tbd)
        return prologue + thb
    enc = encoding.upper() if encoding else 'UTF-8'
    xml = '<?xml version="1.0" encoding="%s"?>' % enc
    pre = prologue + '\n' if prologue and not root else ''
    pos = '\n' + prologue if prologue and root else ''
    res = '{0}\n{1}{2}{3}{4}{5}\n'.format(xml, pre, tab, thd, tbd, pos)
    return res


def _build_xml_footer(style, epilogue, rows, root):
    nested = -1 if style in ('attribute', 'name') else -2
    tbd = _build_nesting(rows, True, nested)
    tab = _build_nesting(root, True, 0)
    pre = epilogue + '\n' if epilogue and root else ''
    pos = '\n' + epilogue if epilogue and not root else ''
    return pre + tbd + tab + pos


def _build_nesting(path, closing, index):
    if not path:
        return ''
    fmt = '</%s>' if closing else '<%s>'
    if '/' not in path:
        return fmt % path
    parts = path.split('/')
    elements = parts[0:index] if index else parts
    if closing:
        elements.reverse()
    tags = [fmt % e for e in elements]
    return ''.join(tags)


def _build_cols(style, props, path, is_value):
    is_header = not is_value
    if style == 'tag' or is_header:
        return _build_cols_inline(props, path, is_value, True)
    if style == 'name':
        return _build_cols_inline(props, path, is_value, False)
    if style == 'attribute':
        return _build_cols_attribs(props, path)
    return style  # custom


def _build_cols_inline(props, path, is_value, use_tag):
    parts = path.split('/')
    if use_tag:
        if len(parts) < 2:
            raise ValueError("Tag not in format 'row/col': %s" % path)            
        col = parts[-1]
        row = parts[-2:-1][0]
    else:
        col = '{0}'
        row = parts[-1]
    fld = '{{{0}}}' if is_value else '{0}'
    fmt = '<{0}>{1}</{0}>'.format(col, fld)
    cols = [fmt.format(e) for e in props]
    tags = ''.join(cols)
    res = ' <{0}>{1}</{0}>\n'.format(row, tags)
    return res


def _build_cols_attribs(props, path):
    parts = path.split('/')
    row = parts[-1]
    fmt = '{0}="{{{0}}}"'
    cols = [fmt.format(e) for e in props]
    atts = ' '.join(cols)
    res = ' <{0} {1} />\n'.format(row, atts)
    return res
