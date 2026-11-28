import sys
from copy import copy

import re

from lark.load_grammar import load_grammar
from lark.common import LexerConf, ParserConf
from lark.parsers.lalr_analysis import LALR_Analyzer, Shift, Reduce
from lark.grammar import Rule

PROBE = "/Users/guo/Documents/GSB/1109/batches/accounts55-u224-20261128/control/questions/GSB1109-20261122-U224-DE-LARK-305/workspace/probe"


def build_components(entry_name, strict):
    grammar, used_files = load_grammar(
        open(f"{PROBE}/{entry_name}").read(),
        f"{PROBE}/{entry_name}",
        [PROBE],
        False,
    )
    terminals, rules, ignore_tokens = grammar.compile(["batch", "checkpoint"], set())
    lexer_conf = LexerConf(
        terminals, re, ignore_tokens, None, {}, 0,
        use_bytes=False, strict=False,
    )
    parser_conf = ParserConf(rules, {}, ["batch", "checkpoint"])
    analyzer = LALR_Analyzer(parser_conf, debug=True, strict=strict)
    analyzer.compute_lalr()
    return grammar, terminals, rules, ignore_tokens, analyzer


def dump_rules(rules):
    print("=== compiled rules (origin, priority, expansion) ===")
    for r in rules:
        print(f"  prio={r.options.priority!s:>4}  {r}")


def dump_terminals(terminals):
    print("=== terminals (name, priority, pattern) ===")
    for t in terminals:
        print(f"  prio={t.priority!s:>4}  {t.name:10} {t.pattern.to_regexp()}")


if __name__ == "__main__":
    entry = sys.argv[1] if len(sys.argv) > 1 else "entry.lark"
    strict = "--strict" in sys.argv
    print(f"### building {entry} strict={strict}")
    try:
        grammar, terminals, rules, ignore_tokens, analyzer = build_components(entry, strict)
    except Exception as e:
        print(type(e).__name__, ":", e)
        sys.exit(0)
    dump_rules(rules)
    dump_terminals(terminals)
    print("ignore:", ignore_tokens)
    print("=== parse table (debug State keys) ===")
    table = analyzer.parse_table
    for state, actions in table.states.items():
        print(f"STATE {state!r}")
        for la, act in sorted(actions.items()):
            kind = act[0]
            if kind is Shift:
                print(f"   {la:10} SHIFT -> {act[1]!r}")
            else:
                print(f"   {la:10} REDUCE {act[1]}")
