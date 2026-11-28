# Lexer Implementation

from abc import abstractmethod, ABC
import re
from typing import (
    TypeVar, Type, Dict, Iterator, Collection, Callable, Optional, FrozenSet, Any,
    AnyStr, ClassVar, TYPE_CHECKING, overload, List
)
from types import ModuleType
import warnings
try:
    import interegular
except ImportError:
    pass
if TYPE_CHECKING:
    from .common import LexerConf
    from .parsers.lalr_parser_state import ParserState

from .utils import classify, get_regexp_width, Serialize, logger, TextSlice, TextOrSlice
from .exceptions import UnexpectedCharacters, ConfigurationError, LexError, UnexpectedToken
from .grammar import TOKEN_DEFAULT_PRIORITY


###{standalone
from contextlib import suppress
from copy import copy
from dataclasses import dataclass
from bisect import bisect_right
import unicodedata

try:  # For the standalone parser, we need to make sure that has_interegular is False to avoid NameErrors later on
    has_interegular = bool(interegular)
except NameError:
    has_interegular = False

class Pattern(Serialize, ABC):
    "An abstraction over regular expressions."

    value: str
    flags: Collection[str]
    raw: Optional[str]
    type: ClassVar[str]

    def __init__(self, value: str, flags: Collection[str] = (), raw: Optional[str] = None) -> None:
        self.value = value
        self.flags = frozenset(flags)
        self.raw = raw

    def __repr__(self):
        return repr(self.to_regexp())

    # Pattern Hashing assumes all subclasses have a different priority!
    def __hash__(self):
        return hash((type(self), self.value, self.flags))

    def __eq__(self, other):
        return type(self) == type(other) and self.value == other.value and self.flags == other.flags

    @abstractmethod
    def to_regexp(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def min_width(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def max_width(self) -> int:
        raise NotImplementedError()

    def _get_flags(self, value):
        # Sorted to remove the hash randomization in sets, that differs between processes
        for f in sorted(self.flags):
            value = ('(?%s:%s)' % (f, value))
        return value


class PatternStr(Pattern):
    __serialize_fields__ = 'value', 'flags', 'raw'

    type: ClassVar[str] = "str"

    def to_regexp(self) -> str:
        return self._get_flags(re.escape(self.value))

    @property
    def min_width(self) -> int:
        return len(self.value)

    @property
    def max_width(self) -> int:
        return len(self.value)


class PatternRE(Pattern):
    __serialize_fields__ = 'value', 'flags', 'raw', '_width'

    type: ClassVar[str] = "re"

    def to_regexp(self) -> str:
        return self._get_flags(self.value)

    _width = None
    def _get_width(self):
        if self._width is None:
            self._width = get_regexp_width(self.to_regexp())
        return self._width

    @property
    def min_width(self) -> int:
        return self._get_width()[0]

    @property
    def max_width(self) -> int:
        return self._get_width()[1]


class TerminalDef(Serialize):
    "A definition of a terminal"
    __serialize_fields__ = 'name', 'pattern', 'priority'
    __serialize_namespace__ = PatternStr, PatternRE

    name: str
    pattern: Pattern
    priority: int

    def __init__(self, name: str, pattern: Pattern, priority: int = TOKEN_DEFAULT_PRIORITY) -> None:
        assert isinstance(pattern, Pattern), pattern
        self.name = name
        self.pattern = pattern
        self.priority = priority

    def __repr__(self):
        return '%s(%r, %r)' % (type(self).__name__, self.name, self.pattern)

    def user_repr(self) -> str:
        if self.name.startswith('__'):  # We represent a generated terminal
            return self.pattern.raw or self.name
        else:
            return self.name

_T = TypeVar('_T', bound="Token")

class Token(str):
    """A string with meta-information, that is produced by the lexer.

    When parsing text, the resulting chunks of the input that haven't been discarded,
    will end up in the tree as Token instances. The Token class inherits from Python's ``str``,
    so normal string comparisons and operations will work as expected.

    Attributes:
        type: Name of the token (as specified in grammar)
        value: Value of the token (redundant, as ``token.value == token`` will always be true)
        start_pos: The index of the token in the text
        line: The line of the token in the text (starting with 1)
        column: The column of the token in the text (starting with 1)
        end_line: The line where the token ends
        end_column: The next column after the end of the token. For example,
            if the token is a single character with a column value of 4,
            end_column will be 5.
        end_pos: the index where the token ends (basically ``start_pos + len(token)``)
        raw: The original slice of the input text that produced this token, when the
            token was matched via NFKC normalization (see the ``nfkc_terminals``
            option). ``None`` for tokens that were not normalized.
    """
    __slots__ = ('type', 'start_pos', 'value', 'line', 'column', 'end_line', 'end_column', 'end_pos', 'raw')

    __match_args__ = ('type', 'value')

    type: str
    start_pos: Optional[int]
    value: Any
    line: Optional[int]
    column: Optional[int]
    end_line: Optional[int]
    end_column: Optional[int]
    end_pos: Optional[int]
    raw: Optional[Any]


    @overload
    def __new__(
            cls,
            type: str,
            value: Any,
            start_pos: Optional[int] = None,
            line: Optional[int] = None,
            column: Optional[int] = None,
            end_line: Optional[int] = None,
            end_column: Optional[int] = None,
            end_pos: Optional[int] = None
    ) -> 'Token':
        ...

    @overload
    def __new__(
            cls,
            type_: str,
            value: Any,
            start_pos: Optional[int] = None,
            line: Optional[int] = None,
            column: Optional[int] = None,
            end_line: Optional[int] = None,
            end_column: Optional[int] = None,
            end_pos: Optional[int] = None
    ) -> 'Token':        ...

    def __new__(cls, *args, **kwargs):
        if "type_" in kwargs:
            warnings.warn("`type_` is deprecated use `type` instead", DeprecationWarning)

            if "type" in kwargs:
                raise TypeError("Error: using both 'type' and the deprecated 'type_' as arguments.")
            kwargs["type"] = kwargs.pop("type_")

        return cls._future_new(*args, **kwargs)


    @classmethod
    def _future_new(cls, type, value, start_pos=None, line=None, column=None, end_line=None, end_column=None, end_pos=None):
        inst = super(Token, cls).__new__(cls, value)

        inst.type = type
        inst.start_pos = start_pos
        inst.value = value
        inst.line = line
        inst.column = column
        inst.end_line = end_line
        inst.end_column = end_column
        inst.end_pos = end_pos
        inst.raw = None
        return inst

    @overload
    def update(self, type: Optional[str] = None, value: Optional[Any] = None) -> 'Token':
        ...

    @overload
    def update(self, type_: Optional[str] = None, value: Optional[Any] = None) -> 'Token':
        ...

    def update(self, *args, **kwargs):
        if "type_" in kwargs:
            warnings.warn("`type_` is deprecated use `type` instead", DeprecationWarning)

            if "type" in kwargs:
                raise TypeError("Error: using both 'type' and the deprecated 'type_' as arguments.")
            kwargs["type"] = kwargs.pop("type_")

        return self._future_update(*args, **kwargs)

    def _future_update(self, type: Optional[str] = None, value: Optional[Any] = None) -> 'Token':
        return Token.new_borrow_pos(
            type if type is not None else self.type,
            value if value is not None else self.value,
            self
        )

    @classmethod
    def new_borrow_pos(cls: Type[_T], type_: str, value: Any, borrow_t: 'Token') -> _T:
        return cls(type_, value, borrow_t.start_pos, borrow_t.line, borrow_t.column, borrow_t.end_line, borrow_t.end_column, borrow_t.end_pos)

    def __reduce__(self):
        return (self.__class__, (self.type, self.value, self.start_pos, self.line, self.column,
                                 self.end_line, self.end_column, self.end_pos))

    def __repr__(self):
        return 'Token(%r, %r)' % (self.type, self.value)

    def __deepcopy__(self, memo):
        return Token(self.type, self.value, self.start_pos, self.line, self.column,
                     self.end_line, self.end_column, self.end_pos)

    def __eq__(self, other):
        if isinstance(other, Token) and self.type != other.type:
            return False

        return str.__eq__(self, other)

    __hash__ = str.__hash__


@dataclass(frozen=True)
class _TextSlice_WithLineCount(TextSlice):
    """Internal: a TextSlice carrying the line/column state at its ``start``, so the lexer can
    resume position tracking without re-counting from offset 0.
    """
    line: int
    line_start_pos: int


class LineCounter:
    "A utility class for keeping track of line & column information"

    __slots__ = 'char_pos', 'line', 'column', 'line_start_pos', 'newline_char'

    def __init__(self, newline_char):
        self.newline_char = newline_char
        self.char_pos = 0
        self.line = 1
        self.column = 1
        self.line_start_pos = 0

    @classmethod
    def from_text_slice(cls, text_slice: TextSlice) -> 'LineCounter':
        """Build a counter positioned at ``text_slice.start``. Resumes from a snapshot when the
        slice carries one (``_TextSlice_WithLineCount``); otherwise counts the prefix once.
        """
        self = cls(b'\n' if isinstance(text_slice.text, bytes) else '\n')
        if isinstance(text_slice, _TextSlice_WithLineCount):
            self.char_pos = text_slice.start
            self.line = text_slice.line
            self.line_start_pos = text_slice.line_start_pos
            self.column = text_slice.start - text_slice.line_start_pos + 1
        elif text_slice.start > 0:
            self.advance_to(text_slice.text, text_slice.start)
        return self

    def __eq__(self, other):
        if not isinstance(other, LineCounter):
            return NotImplemented

        return self.char_pos == other.char_pos and self.newline_char == other.newline_char

    def feed(self, token: AnyStr, test_newline=True):
        """Consume a token and calculate the new line & column.

        As an optional optimization, set test_newline=False if token doesn't contain a newline.
        """
        if test_newline:
            newlines = token.count(self.newline_char)
            if newlines:
                self.line += newlines
                self.line_start_pos = self.char_pos + token.rindex(self.newline_char) + 1

        self.char_pos += len(token)
        self.column = self.char_pos - self.line_start_pos + 1

    def advance_to(self, text: AnyStr, pos: int):
        """Advance the counter to absolute offset ``pos`` within ``text``, counting the newlines
        """
        newlines = text.count(self.newline_char, self.char_pos, pos)
        if newlines:
            self.line += newlines
            self.line_start_pos = text.rindex(self.newline_char, self.char_pos, pos) + 1
        self.char_pos = pos
        self.column = self.char_pos - self.line_start_pos + 1


class UnlessCallback:
    def __init__(self, scanner: 'Scanner'):
        self.scanner = scanner

    def __call__(self, t: Token):
        res = self.scanner.fullmatch(t.value)
        if res is not None:
            t.type = res
        return t


def _hangul_compose_pair(a: str, b: str) -> bool:
    # Conjoining Hangul jamo are the only starters (combining class 0) that
    # canonically compose with a following starter: L + V, and LV-syllable + T.
    oa, ob = ord(a), ord(b)
    if 0x1100 <= oa <= 0x1112 and 0x1161 <= ob <= 0x1175:
        return True
    return 0xAC00 <= oa <= 0xD7A3 and (oa - 0xAC00) % 28 == 0 and 0x11A8 <= ob <= 0x11C2


class _NfkcView:
    """An NFKC-normalized view of a text, with a bidirectional offset map.

    The text is split into segments that normalization cannot cross: each segment
    starts at a starter (combining class 0) and extends to the next starter, so
    that letters built from multiple codepoints (e.g. a letter and its combining
    marks) stay within a single normalizable unit. The concatenation of the
    normalized segments then equals the normalization of the whole text, and every
    normalized offset can be mapped back to an offset in the original text.
    """

    __slots__ = ('text', 'norm_starts', 'orig_starts', 'orig_ends', 'slice')

    text: str
    norm_starts: List[int]
    orig_starts: List[int]
    orig_ends: List[int]
    slice: TextSlice

    def __init__(self, text: str, end: int) -> None:
        bounds = [0]
        prev = ''
        for i, ch in enumerate(text):
            if i and not unicodedata.combining(ch) and not _hangul_compose_pair(prev, ch):
                bounds.append(i)
            prev = ch
        bounds.append(len(text))

        norm_parts = []
        self.norm_starts = []
        self.orig_starts = []
        self.orig_ends = []
        norm_pos = 0
        for seg_start, seg_end in zip(bounds, bounds[1:]):
            norm_seg = unicodedata.normalize('NFKC', text[seg_start:seg_end])
            norm_parts.append(norm_seg)
            self.norm_starts.append(norm_pos)
            self.orig_starts.append(seg_start)
            self.orig_ends.append(seg_end)
            norm_pos += len(norm_seg)
        self.text = ''.join(norm_parts)
        self.norm_starts.append(norm_pos)  # Sentinel: end of the normalized text

        if self.text != unicodedata.normalize('NFKC', text):
            # The segmentation assumption didn't hold for this text (extremely rare).
            # Fall back to a single segment: values stay correct, positions degrade.
            self.text = unicodedata.normalize('NFKC', text)
            self.norm_starts = [0, len(self.text)]
            self.orig_starts = [0]
            self.orig_ends = [len(text)]

        self.slice = TextSlice(self.text, 0, self.norm_index(end))

    def norm_index(self, orig_pos: int) -> int:
        """Map an original offset to a normalized offset.

        Segment interiors (e.g. between a letter and its combining mark) round up
        to the next segment boundary.
        """
        i = bisect_right(self.orig_starts, orig_pos) - 1
        if self.orig_starts[i] == orig_pos:
            return self.norm_starts[i]
        return self.norm_starts[i + 1]

    def orig_end_index(self, norm_pos: int) -> int:
        """Map a normalized exclusive-end offset to an original exclusive-end offset.

        Normalized offsets inside a segment map to the original end of that segment,
        so a token always spans its full original characters.
        """
        i = bisect_right(self.norm_starts, norm_pos) - 1
        if i >= len(self.orig_ends):
            return self.orig_ends[-1]
        if self.norm_starts[i] == norm_pos:
            return self.orig_starts[i]
        return self.orig_ends[i]

class CallChain:
    def __init__(self, callback1, callback2, cond):
        self.callback1 = callback1
        self.callback2 = callback2
        self.cond = cond

    def __call__(self, t):
        t2 = self.callback1(t)
        return self.callback2(t) if self.cond(t2) else t2


def _get_match(re_, regexp, s, flags):
    m = re_.match(regexp, s, flags)
    if m:
        return m.group(0)

def _create_unless(terminals, g_regex_flags, re_, use_bytes):
    tokens_by_type = classify(terminals, lambda t: type(t.pattern))
    assert len(tokens_by_type) <= 2, tokens_by_type.keys()
    embedded_strs = set()
    callback = {}
    for retok in tokens_by_type.get(PatternRE, []):
        unless = []
        for strtok in tokens_by_type.get(PatternStr, []):
            if strtok.priority != retok.priority:
                continue
            s = strtok.pattern.value
            if s == _get_match(re_, retok.pattern.to_regexp(), s, g_regex_flags):
                unless.append(strtok)
                if strtok.pattern.flags <= retok.pattern.flags:
                    embedded_strs.add(strtok)
        if unless:
            callback[retok.name] = UnlessCallback(Scanner(unless, g_regex_flags, re_, use_bytes=use_bytes))

    new_terminals = [t for t in terminals if t not in embedded_strs]
    # Map each embedded string terminal to the regexp terminal it was embedded into
    embedded = {}
    for retok in tokens_by_type.get(PatternRE, []):
        for strtok in embedded_strs:
            if strtok.priority == retok.priority and strtok.pattern.flags <= retok.pattern.flags \
                    and strtok.pattern.value == _get_match(re_, retok.pattern.to_regexp(), strtok.pattern.value, g_regex_flags):
                embedded[strtok.name] = retok.name
    return new_terminals, callback, embedded


class Scanner:
    def __init__(self, terminals, g_regex_flags, re_, use_bytes):
        self.terminals = terminals
        self.g_regex_flags = g_regex_flags
        self.re_ = re_
        self.use_bytes = use_bytes

        self.allowed_types = {t.name for t in self.terminals}

        self._mres = self._build_mres(terminals, len(terminals))

    def _build_mres(self, terminals, max_size):
        # Python sets an unreasonable group limit (currently 100) in its re module
        # Worse, the only way to know we reached it is by catching an AssertionError!
        # This function recursively tries less and less groups until it's successful.
        mres = []
        while terminals:
            pattern = u'|'.join(u'(?P<%s>%s)' % (t.name, t.pattern.to_regexp()) for t in terminals[:max_size])
            if self.use_bytes:
                pattern = pattern.encode('latin-1')
            try:
                mre = self.re_.compile(pattern, self.g_regex_flags)
            except AssertionError:  # Yes, this is what Python provides us.. :/
                return self._build_mres(terminals, max_size // 2)

            mres.append(mre)
            terminals = terminals[max_size:]
        return mres

    def match(self, text: TextSlice, pos):
        for mre in self._mres:
            m = mre.match(text.text, pos, text.end)
            if m:
                return m.group(0), m.lastgroup


    def fullmatch(self, text: str) -> Optional[str]:
        for mre in self._mres:
            m = mre.fullmatch(text)
            if m:
                return m.lastgroup
        return None

    def search(self, text: TextSlice, pos: int) -> Optional[int]:
        "Find the position of the earliest match, starting at pos"
        best = None
        for mre in self._mres:
            m = mre.search(text.text, pos, text.end)
            if m and (best is None or m.start() < best.start()):
                best = m
        return best.start() if best is not None else None

def _regexp_has_newline(r: str):
    r"""Expressions that may indicate newlines in a regexp:
        - newlines (\n)
        - escaped newline (\\n)
        - anything but ([^...])
        - any-char (.) when the flag (?s) exists
        - spaces (\s)
    """
    return '\n' in r or '\\n' in r or '\\s' in r or '[^' in r or ('(?s' in r and '.' in r)


class LexerState:
    """Represents the current state of the lexer as it scans the text
    (Lexer objects are only instantiated per grammar, not per text)
    """

    __slots__ = 'text', 'line_ctr', 'last_token', '_nfkc_view'

    text: TextSlice
    line_ctr: LineCounter
    last_token: Optional[Token]
    _nfkc_view: Optional[_NfkcView]

    def __init__(self, text: TextSlice, line_ctr: Optional[LineCounter] = None, last_token: Optional[Token]=None):
        if isinstance(text, TextSlice):
            if line_ctr is None:
                line_ctr = LineCounter.from_text_slice(text)

            if not (text.start <= line_ctr.char_pos <= text.end):
                raise ValueError("LineCounter.char_pos is out of bounds")

        self.text = text
        self.line_ctr = line_ctr
        self.last_token = last_token
        self._nfkc_view = None


    def __eq__(self, other):
        if not isinstance(other, LexerState):
            return NotImplemented

        return self.text == other.text and self.line_ctr == other.line_ctr and self.last_token == other.last_token

    def __copy__(self):
        return type(self)(self.text, copy(self.line_ctr), self.last_token)


class LexerThread:
    """A thread that ties a lexer instance and a lexer state, to be used by the parser
    """

    def __init__(self, lexer: 'Lexer', lexer_state: Optional[LexerState]):
        self.lexer = lexer
        self.state = lexer_state

    @classmethod
    def from_text(cls, lexer: 'Lexer', text_or_slice: TextOrSlice) -> 'LexerThread':
        text = TextSlice.cast_from(text_or_slice)
        return cls(lexer, LexerState(text))

    @classmethod
    def from_custom_input(cls, lexer: 'Lexer', text: Any) -> 'LexerThread':
        return cls(lexer, LexerState(text))

    def lex(self, parser_state):
        if self.state is None:
            raise TypeError("Cannot lex: No text assigned to lexer state")
        return self.lexer.lex(self.state, parser_state)

    def __copy__(self):
        return type(self)(self.lexer, copy(self.state))

    _Token = Token


_Callback = Callable[[Token], Token]

class Lexer(ABC):
    """Lexer interface

    Method Signatures:
        lex(self, lexer_state, parser_state) -> Iterator[Token]
    """
    @abstractmethod
    def lex(self, lexer_state: LexerState, parser_state: Any) -> Iterator[Token]:
        return NotImplemented

    def search_start(self, text: TextSlice, start_state: Any, pos: int) -> Optional[int]:
        raise ConfigurationError("scan() is not supported by %s; use the built-in 'basic' or 'contextual' lexer"
                                 % type(self).__name__)

    def make_lexer_state(self, text: str):
        "Deprecated"
        return LexerState(TextSlice.cast_from(text))


def _check_regex_collisions(terminal_to_regexp: Dict[TerminalDef, str], comparator, strict_mode, max_collisions_to_show=8):
    if not comparator:
        comparator = interegular.Comparator.from_regexes(terminal_to_regexp)

    # When in strict mode, we only ever try to provide one example, so taking
    # a long time for that should be fine
    max_time = 2 if strict_mode else 0.2

    # We don't want to show too many collisions.
    if comparator.count_marked_pairs() >= max_collisions_to_show:
        return
    for group in classify(terminal_to_regexp, lambda t: t.priority).values():
        for a, b in comparator.check(group, skip_marked=True):
            assert a.priority == b.priority
            # Mark this pair to not repeat warnings when multiple different BasicLexers see the same collision
            comparator.mark(a, b)

            # Notify the user
            message = f"Collision between Terminals {a.name} and {b.name}. "
            try:
                example = comparator.get_example_overlap(a, b, max_time).format_multiline()
            except ValueError:
                # Couldn't find an example within max_time steps.
                example = "No example could be found fast enough. However, the collision does still exists"
            if strict_mode:
                raise LexError(f"{message}\n{example}")
            logger.warning("%s The lexer will choose between them arbitrarily.\n%s", message, example)
            if comparator.count_marked_pairs() >= max_collisions_to_show:
                logger.warning("Found 8 regex collisions, will not check for more.")
                return


class AbstractBasicLexer(Lexer):
    terminals_by_name: Dict[str, TerminalDef]

    @abstractmethod
    def __init__(self, conf: 'LexerConf', comparator=None) -> None:
        ...

    @abstractmethod
    def next_token(self, lex_state: LexerState, parser_state: Any = None) -> Token:
        ...

    def lex(self, state: LexerState, parser_state: Any) -> Iterator[Token]:
        with suppress(EOFError):
            while True:
                yield self.next_token(state, parser_state)


class BasicLexer(AbstractBasicLexer):
    terminals: Collection[TerminalDef]
    ignore_types: FrozenSet[str]
    newline_types: FrozenSet[str]
    user_callbacks: Dict[str, _Callback]
    callback: Dict[str, _Callback]
    re: ModuleType
    nfkc_types: FrozenSet[str]

    def __init__(self, conf: 'LexerConf', comparator=None) -> None:
        terminals = list(conf.terminals)
        assert all(isinstance(t, TerminalDef) for t in terminals), terminals

        self.re = conf.re_module

        if not conf.skip_validation:
            # Sanitization
            terminal_to_regexp = {}
            for t in terminals:
                regexp = t.pattern.to_regexp()
                try:
                    self.re.compile(regexp, conf.g_regex_flags)
                except self.re.error:
                    raise LexError("Cannot compile token %s: %s" % (t.name, t.pattern))

                if t.pattern.min_width == 0:
                    raise LexError("Lexer does not allow zero-width terminals. (%s: %s)" % (t.name, t.pattern))
                if t.pattern.type == "re":
                    terminal_to_regexp[t] = regexp

            if not (set(conf.ignore) <= {t.name for t in terminals}):
                raise LexError("Ignore terminals are not defined: %s" % (set(conf.ignore) - {t.name for t in terminals}))

            if has_interegular:
                _check_regex_collisions(terminal_to_regexp, comparator, conf.strict)
            elif conf.strict:
                raise LexError("interegular must be installed for strict mode. Use `pip install 'lark[interegular]'`.")

        # Init
        self.newline_types = frozenset(t.name for t in terminals if _regexp_has_newline(t.pattern.to_regexp()))
        self.ignore_types = frozenset(conf.ignore)

        terminals.sort(key=lambda x: (-x.priority, -x.pattern.max_width, -len(x.pattern.value), x.name))
        self.terminals = terminals
        self.user_callbacks = conf.callbacks
        self.g_regex_flags = conf.g_regex_flags
        self.use_bytes = conf.use_bytes
        self.terminals_by_name = conf.terminals_by_name

        self._scanner: Optional[Scanner] = None
        self._search_scanner: Optional[Scanner] = None

        nfkc_names = conf.nfkc_terminals
        if nfkc_names and self.use_bytes:
            raise LexError("nfkc_terminals is not supported with use_bytes=True")
        self.nfkc_types = frozenset(t.name for t in terminals if t.name in nfkc_names)
        self._terminal_rank = {t.name: i for i, t in enumerate(terminals)}
        self._embedded_unless: Dict[str, str] = {}

        self._nfkc_scanner: Optional[Scanner] = None

    def _build_scanners(self):
        terminals, self.callback, self._embedded_unless = _create_unless(self.terminals, self.g_regex_flags, self.re, self.use_bytes)
        assert all(self.callback.values())

        for type_, f in self.user_callbacks.items():
            if type_ in self.callback:
                # Already a callback there, probably UnlessCallback.
                # Bind ``type_`` per iteration; otherwise every CallChain's
                # condition closes over the loop variable and checks the last
                # terminal's name, silently skipping the other callbacks.
                self.callback[type_] = CallChain(
                    self.callback[type_], f, lambda t, type_=type_: t.type == type_
                )
            else:
                self.callback[type_] = f

        if not self.nfkc_types:
            return Scanner(terminals, self.g_regex_flags, self.re, self.use_bytes), None

        # Terminals declared for NFKC normalization are matched against the normalized
        # text; the rest against the original text. A string terminal that was embedded
        # into a regexp terminal of the *other* group is kept in its own group, so it
        # can still match input that the regexp terminal cannot (e.g. its normalized form).
        raw_terminals = [t for t in terminals if t.name not in self.nfkc_types]
        raw_terminals += [t for t in self.terminals
                          if t.name in self._embedded_unless and t.name not in self.nfkc_types
                          and self._embedded_unless[t.name] in self.nfkc_types]
        nfkc_terminals = [t for t in terminals if t.name in self.nfkc_types]
        nfkc_terminals += [t for t in self.terminals
                           if t.name in self._embedded_unless and t.name in self.nfkc_types
                           and self._embedded_unless[t.name] not in self.nfkc_types]
        return (Scanner(raw_terminals, self.g_regex_flags, self.re, self.use_bytes),
                Scanner(nfkc_terminals, self.g_regex_flags, self.re, self.use_bytes))

    @property
    def scanner(self) -> Scanner:
        if self._scanner is None:
            self._scanner, self._nfkc_scanner = self._build_scanners()
        return self._scanner

    @property
    def nfkc_scanner(self) -> Optional[Scanner]:
        if self._scanner is None:
            self._scanner, self._nfkc_scanner = self._build_scanners()
        return self._nfkc_scanner

    @property
    def search_scanner(self) -> Scanner:
        # Used by search_start(): a match can only begin with a non-ignored terminal, so we
        # search those directly. Searching all terminals and skipping ignores would jump past
        # a real start hiding inside an ignore's span (e.g. the "a" in an ignored "xxa").
        if self._search_scanner is None:
            terminals = [t for t in self.terminals if t.name not in self.ignore_types]
            self._search_scanner = Scanner(terminals, self.g_regex_flags, self.re, self.use_bytes)
        return self._search_scanner

    def match(self, text, pos):
        return self.scanner.match(text, pos)

    def _nfkc_view(self, lex_state: LexerState) -> _NfkcView:
        # The normalized view depends only on the text, so it is built once per
        # lexed text and shared by all lexers that see this state (contextual lexer).
        view = lex_state._nfkc_view
        if view is None:
            view = _NfkcView(lex_state.text.text, lex_state.text.end)
            lex_state._nfkc_view = view
        return view

    def _match(self, lex_state: LexerState, pos: int):
        """Match the next token. Returns (value, type, raw), where raw is the original
        text slice for tokens matched via NFKC normalization, and None otherwise."""
        if not self.nfkc_types:
            res = self.match(lex_state.text, pos)
            return (res[0], res[1], None) if res else None

        best = None
        best_rank = 0
        res = self.scanner.match(lex_state.text, pos)
        if res:
            best = (res[0], res[1], None)
            best_rank = self._terminal_rank[res[1]]

        view = self._nfkc_view(lex_state)
        norm_pos = view.norm_index(pos)
        nres = self.nfkc_scanner.match(view.slice, norm_pos)
        if nres:
            nvalue, ntype = nres
            if best is None:
                replace = True
            elif self._embedded_unless.get(ntype) == best[1]:
                # The normalized match is a string terminal embedded into the regexp
                # terminal that matched the raw text; the regexp's (longer) match wins.
                replace = False
            elif self._embedded_unless.get(best[1]) == ntype:
                # Symmetric case: the raw match is embedded into the normalized regexp.
                replace = True
            else:
                # Same as a single combined pattern: the highest-ranking terminal wins.
                replace = self._terminal_rank[ntype] < best_rank
            if replace:
                raw = lex_state.text.text[pos:view.orig_end_index(norm_pos + len(nvalue))]
                best = (nvalue, ntype, raw)
        return best

    def next_token(self, lex_state: LexerState, parser_state: Any = None) -> Token:
        line_ctr = lex_state.line_ctr
        while line_ctr.char_pos < lex_state.text.end:
            res = self._match(lex_state, line_ctr.char_pos)
            if not res:
                allowed = self.scanner.allowed_types - self.ignore_types
                if self.nfkc_scanner is not None:
                    allowed |= self.nfkc_scanner.allowed_types - self.ignore_types
                if not allowed:
                    allowed = {"<END-OF-FILE>"}
                raise UnexpectedCharacters(lex_state.text.text, line_ctr.char_pos, line_ctr.line, line_ctr.column,
                                           allowed=allowed, token_history=lex_state.last_token and [lex_state.last_token],
                                           state=parser_state, terminals_by_name=self.terminals_by_name)

            value, type_, raw = res

            ignored = type_ in self.ignore_types
            t = None
            if not ignored or type_ in self.callback:
                t = Token(type_, value, line_ctr.char_pos, line_ctr.line, line_ctr.column)
                if raw is not None:
                    t.raw = raw
            line_ctr.feed(raw if raw is not None else value, type_ in self.newline_types)
            if t is not None:
                t.end_line = line_ctr.line
                t.end_column = line_ctr.column
                t.end_pos = line_ctr.char_pos
                if t.type in self.callback:
                    t = self.callback[t.type](t)
                if not ignored:
                    if not isinstance(t, Token):
                        raise LexError("Callbacks must return a token (returned %r)" % t)
                    lex_state.last_token = t
                    return t

        # EOF
        raise EOFError(self)

    def search_start(self, text: TextSlice, start_state: Any, pos: int) -> Optional[int]:
        return self.search_scanner.search(text, pos)


class ContextualLexer(Lexer):
    lexers: Dict[int, AbstractBasicLexer]
    root_lexer: AbstractBasicLexer

    BasicLexer: Type[AbstractBasicLexer] = BasicLexer

    def __init__(self, conf: 'LexerConf', states: Dict[int, Collection[str]], always_accept: Collection[str]=()) -> None:
        terminals = list(conf.terminals)
        terminals_by_name = conf.terminals_by_name

        trad_conf = copy(conf)
        trad_conf.terminals = terminals

        if has_interegular and not conf.skip_validation:
            comparator = interegular.Comparator.from_regexes({t: t.pattern.to_regexp() for t in terminals})
        else:
            comparator = None
        lexer_by_tokens: Dict[FrozenSet[str], AbstractBasicLexer] = {}
        self.lexers = {}
        for state, accepts in states.items():
            key = frozenset(accepts)
            try:
                lexer = lexer_by_tokens[key]
            except KeyError:
                accepts = set(accepts) | set(conf.ignore) | set(always_accept)
                lexer_conf = copy(trad_conf)
                lexer_conf.terminals = [terminals_by_name[n] for n in accepts if n in terminals_by_name]
                lexer = self.BasicLexer(lexer_conf, comparator)
                lexer_by_tokens[key] = lexer

            self.lexers[state] = lexer

        assert trad_conf.terminals is terminals
        trad_conf.skip_validation = True  # We don't need to verify all terminals again
        self.root_lexer = self.BasicLexer(trad_conf, comparator)

    def lex(self, lexer_state: LexerState, parser_state: 'ParserState') -> Iterator[Token]:
        try:
            while True:
                lexer = self.lexers[parser_state.position]
                yield lexer.next_token(lexer_state, parser_state)
        except EOFError:
            pass
        except UnexpectedCharacters as e:
            # In the contextual lexer, UnexpectedCharacters can mean that the terminal is defined, but not in the current context.
            # This tests the input against the global context, to provide a nicer error.
            try:
                last_token = lexer_state.last_token  # Save last_token. Calling root_lexer.next_token will change this to the wrong token
                token = self.root_lexer.next_token(lexer_state, parser_state)
                raise UnexpectedToken(token, e.allowed, state=parser_state, token_history=[last_token], terminals_by_name=self.root_lexer.terminals_by_name)
            except UnexpectedCharacters:
                raise e  # Raise the original UnexpectedCharacters. The root lexer raises it with the wrong expected set.

    def search_start(self, text: TextSlice, start_state: Any, pos: int) -> Optional[int]:
        return self.lexers[start_state].search_start(text, start_state, pos)

###}
