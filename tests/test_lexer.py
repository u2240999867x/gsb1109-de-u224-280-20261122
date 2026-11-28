from unittest import TestCase, main

from lark import Lark, Tree, TextSlice
from lark.exceptions import ConfigurationError, UnexpectedCharacters, UnexpectedInput
from lark.lexer import Token


class TestLexer(TestCase):
    def setUp(self):
        pass

    def test_basic(self):
        p = Lark("""
            start: "a" "b" "c" "d"
            %ignore " "
        """)

        res = list(p.lex("abc cba dd"))
        assert res == list('abccbadd')

        res = list(p.lex("abc cba dd", dont_ignore=True))
        assert res == list('abc cba dd')

    def test_flag_order_is_deterministic(self):
        # Two or more flags on one terminal is the only case where order exists; the
        # other flag tests all use a single flag. Without sorting, the frozenset is
        # iterated in hash order and the regexp differs between processes.
        p = Lark('start: A+\nA: /x/imsu\n', parser='lalr')
        self.assertEqual([t.pattern.to_regexp() for t in p.terminals],
                         ['(?u:(?s:(?m:(?i:x))))'])

    def test_subset_lex(self):
        p = Lark("""
            start: "a" "b" "c" "d"
            %ignore " "
        """)

        res = list(p.lex(TextSlice("xxxabc cba ddxx", 3, -2)))
        assert res == list('abccbadd')

        res = list(p.lex(TextSlice("aaaabc cba dddd", 3, -2)))
        assert res == list('abccbadd')




class TestNfkcTerminals(TestCase):
    """Tests for the ``nfkc_terminals`` option: terminals declared there are matched
    against the NFKC-normalized form of the input, while positions and the ``raw``
    attribute keep referring to the original text.
    """

    GRAMMAR = r'''
        start: READ selection TO ID note?
        selection: ALL | ID
        note: NOTE STRING
        READ: "read"
        TO: "to"
        NOTE: "note"
        ALL: "ALL"
        ID: /[A-Za-z\xC0-\xFF]+/
        STRING: /"[^"\n]*"/
        %ignore " "
    '''

    FW_ALL = '\uFF21\uFF2C\uFF2C'  # full-width "ALL"

    def make_parser(self, **options):
        options.setdefault('parser', 'lalr')
        return Lark(self.GRAMMAR, **options)

    def parse_tokens(self, parser, text):
        return list(parser.parse(text).scan_values(lambda v: isinstance(v, Token)))

    def test_keyword_and_id_keep_value_raw_and_positions(self):
        p = self.make_parser(nfkc_terminals=['ALL', 'ID'])
        note = self.FW_ALL + '|e\u0301'  # full-width letters and a combining mark
        text = 'read %s to %s note "%s"' % (self.FW_ALL, self.FW_ALL, note)
        toks = self.parse_tokens(p, text)

        self.assertEqual([t.type for t in toks], ['READ', 'ALL', 'TO', 'ID', 'NOTE', 'STRING'])

        # First occurrence: the ALL keyword; second: a regular ID. Both values are
        # normalized, both raw slices and positions refer to the original text.
        kw, ident = toks[1], toks[3]
        self.assertEqual((kw, kw.raw, kw.start_pos, kw.end_pos), ('ALL', self.FW_ALL, 5, 8))
        self.assertEqual((ident, ident.raw, ident.start_pos, ident.end_pos), ('ALL', self.FW_ALL, 12, 15))

        # Terminals not declared for normalization are matched on the original text
        self.assertIsNone(toks[0].raw)
        self.assertEqual(toks[5], '"' + note + '"')
        self.assertIsNone(toks[5].raw)
        self.assertEqual((toks[5].start_pos, toks[5].end_pos), (21, 29))

    def test_multicodepoint_letter_normalizes_as_one(self):
        p = self.make_parser(nfkc_terminals=['ALL', 'ID'])
        target = 're\u0301seau'  # 'e' + combining acute, 7 codepoints for 6 letters
        text = 'read %s to %s' % (self.FW_ALL, target)
        (id_tok,) = [t for t in self.parse_tokens(p, text) if t.type == 'ID']
        self.assertEqual(id_tok, 'r\u00e9seau')
        self.assertEqual(id_tok.raw, target)
        self.assertEqual((id_tok.start_pos, id_tok.end_pos), (12, 19))
        self.assertEqual((id_tok.column, id_tok.end_column), (13, 20))

    def test_error_position_uses_original_coordinates(self):
        p = self.make_parser(nfkc_terminals=['ALL', 'ID'])
        # Normalization shrinks 're\u0301seau' by one codepoint; the error must
        # still point at the illegal marker in the original text.
        text = 'read %s to re\u0301seau note "x" @' % self.FW_ALL
        with self.assertRaises(UnexpectedCharacters) as cm:
            p.parse(text)
        self.assertEqual(cm.exception.pos_in_stream, text.index('@'))
        self.assertEqual(cm.exception.column, text.index('@') + 1)

    def test_expansion_keeps_original_span(self):
        p = self.make_parser(nfkc_terminals=['ALL', 'ID'])
        text = 'read %s to \uFB01le' % self.FW_ALL  # the ligature expands to 'fi'
        (id_tok,) = [t for t in self.parse_tokens(p, text) if t.type == 'ID']
        self.assertEqual(id_tok, 'file')
        self.assertEqual(id_tok.raw, '\uFB01le')
        self.assertEqual((id_tok.start_pos, id_tok.end_pos), (12, 15))

    def test_selective_normalization(self):
        # Only the keyword is normalized; ID keeps matching the raw text
        p = self.make_parser(nfkc_terminals=['ALL'])
        toks = self.parse_tokens(p, 'read %s to ALL note "y"' % self.FW_ALL)
        self.assertEqual([t.type for t in toks], ['READ', 'ALL', 'TO', 'ID', 'NOTE', 'STRING'])
        with self.assertRaises(UnexpectedInput):
            p.parse('read %s to %s' % (self.FW_ALL, self.FW_ALL))

    def test_default_parser_unchanged(self):
        p = self.make_parser()
        with self.assertRaises(UnexpectedCharacters) as cm:
            p.parse('read %s to %s' % (self.FW_ALL, self.FW_ALL))
        self.assertEqual(cm.exception.pos_in_stream, 5)

    def test_parsers_do_not_interfere(self):
        nfkc_parser = self.make_parser(nfkc_terminals=['ALL', 'ID'])
        plain_parser = self.make_parser()
        text = 'read %s to %s' % (self.FW_ALL, self.FW_ALL)
        self.assertEqual([t.type for t in self.parse_tokens(nfkc_parser, text)],
                         ['READ', 'ALL', 'TO', 'ID'])
        with self.assertRaises(UnexpectedCharacters):
            plain_parser.parse(text)
        # No state leaks between the parsers
        self.assertEqual([t.type for t in self.parse_tokens(nfkc_parser, text)],
                         ['READ', 'ALL', 'TO', 'ID'])

    def test_basic_lexer(self):
        p = self.make_parser(lexer='basic', nfkc_terminals=['ALL', 'ID'])
        toks = list(p.lex('read %s to %s' % (self.FW_ALL, self.FW_ALL)))
        self.assertEqual([t.type for t in toks], ['READ', 'ALL', 'TO', 'ALL'])
        self.assertEqual([str(t) for t in toks], ['read', 'ALL', 'to', 'ALL'])
        self.assertEqual(toks[1].raw, self.FW_ALL)

    def test_unknown_terminal_raises(self):
        with self.assertRaises(ConfigurationError):
            self.make_parser(nfkc_terminals=['NOSUCH'])

if __name__ == '__main__':
    main()
