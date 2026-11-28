"""Authoritative, replayable evidence for LALR-305.

Run:  .venv/bin/python probe/evidence.py

Everything below goes through the real deployed entry point
(Lark.open(parser='lalr', lexer='contextual')) or the same internal
LALR_Analyzer strict gate that Lark.open uses, with no custom lexer
callbacks or post-processing.
"""
import re

from lark.lark import Lark
from lark.exceptions import GrammarError, UnexpectedToken
from lark.load_grammar import load_grammar
from lark.common import LexerConf, ParserConf
from lark.parsers.lalr_analysis import LALR_Analyzer, Shift, Reduce

PROBE = "/Users/guo/Documents/GSB/1109/batches/accounts55-u224-20261128/control/questions/GSB1109-20261122-U224-DE-LARK-305/workspace/probe"

VARIANTS = [
    ("baseline (no rule priority)", "entry.lark", "headers.lark"),
    ("proposed: left.1", "entry_left1.lark", "headers_left1.lark"),
    ("contrast: right.1", "entry_right1.lark", "headers_right1.lark"),
]

TEXTS = {
    ("batch", "sync delta asset;ok"): 19,
    ("batch", "sync delta asset;ok?"): 20,
    ("checkpoint", "resume delta asset;ok"): 21,
    ("checkpoint", "resume delta asset;ok?"): 22,
}


def section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def strict_gate(entry_file):
    grammar, _ = load_grammar(
        open(f"{PROBE}/{entry_file}").read(),
        f"{PROBE}/{entry_file}", [PROBE], False)
    terminals, rules, ignore = grammar.compile(["batch", "checkpoint"], set())
    conf = LexerConf(terminals, re, ignore, None, {}, 0, use_bytes=False, strict=False)
    pconf = ParserConf(rules, {}, ["batch", "checkpoint"])
    analyzer = LALR_Analyzer(pconf, debug=True, strict=True)
    analyzer.compute_lalr()
    return analyzer, rules


def deployed_open(entry_file):
    return Lark.open(
        f"{PROBE}/{entry_file}",
        parser="lalr", lexer="contextual",
        start=["batch", "checkpoint"],
        maybe_placeholders=True, propagate_positions=True,
        keep_all_tokens=True, import_paths=[PROBE])


def interpret(tree):
    """Return the archiving interpretation: which header rule + which tail rule."""
    header = next(ch.data for ch in tree.children if hasattr(ch, "data") and ch.data in ("left", "right"))
    tail_node = next(ch for ch in tree.children if hasattr(ch, "data") and ch.data in ("lt", "rt"))
    tail_terms = [c.type for c in tail_node.children if c is not None]
    return f"{header};{tail_node.data}({','.join(tail_terms)})"


def parse_one(entry_file, start, text):
    lark = deployed_open(entry_file)
    ip = lark.parse_interactive(text, start=start)
    lexer = lark.parser.lexer
    semicolon_evidence = None
    post_semi_evidence = None
    for tok in ip.lexer_thread.lex(ip.parser_state):
        state_pos = ip.parser_state.position
        active = [t.name for t in lexer.lexers[state_pos].terminals]
        if tok.type == "SEMICOLON":
            semicolon_evidence = (state_pos, sorted(ip.choices().keys()), active)
        elif semicolon_evidence and post_semi_evidence is None:
            post_semi_evidence = (state_pos, sorted(ip.choices().keys()), active,
                                  tok.type, tok.value, (tok.start_pos, tok.end_pos),
                                  text[tok.start_pos:])
        ip.feed_token(tok)
    eof_ok = "$END" in ip.choices()
    result = {
        "accepted": eof_ok,
        "semicolon": semicolon_evidence,
        "post_semi": post_semi_evidence,
        "eof_choices": sorted(ip.choices().keys()),
        "interpretation": None,
    }
    if eof_ok:
        result["interpretation"] = interpret(ip.feed_eof(None))
    return result


class _EofFailure(Exception):
    pass


def main():
    section("STEP 1  Strict LALR gate (same analyzer used by Lark.open, strict=True)")
    for label, entry, headers in VARIANTS:
        print(f"\n[{label}]  entry={entry} import={headers}")
        try:
            analyzer, rules = strict_gate(entry)
            rr = [r for r in rules if r.origin.name in ("left", "right")]
            for r in rr:
                print(f"   built OK; rule {r.origin.name:6} priority={r.options.priority}")
        except GrammarError as e:
            print("   STRICT GATE REJECTED GRAMMAR:")
            for line in str(e).splitlines():
                print("     " + line)

    section("STEP 2  Baseline through the real deployed entry point Lark.open(...)")
    try:
        deployed_open("entry.lark")
        print("unexpected: baseline built")
    except GrammarError as e:
        print("Lark.open('entry.lark', parser='lalr', lexer='contextual') raised GrammarError:")
        for line in str(e).splitlines():
            print("   " + line)

    section("STEP 3  Semicolon competing reduction + post-semicolon contextual lexer state")
    for label, entry, headers in VARIANTS[1:]:
        print(f"\n--- {label} ({entry}) ---")
        for (start, text), n in TEXTS.items():
            res = parse_one(entry, start, text)
            verdict = ("ACCEPTED -> " + res["interpretation"]) if res["accepted"] \
                else f"REJECTED at EOF (choices={res['eof_choices']})"
            print(f"  start={start:10} len={n:2} text={text!r}")
            print(f"     {verdict}")
            semi = res["semicolon"]
            if semi:
                print(f"     at ';': parser-state={semi[0]} accept={semi[1]} lexer-order={semi[2]}")
            post = res["post_semi"]
            if post:
                print(f"     post-';' real read symbol={post[3]} value={post[4]!r} "
                      f"span={post[5]} tail handed to parser={post[6]!r}")
                print(f"               parser-state={post[0]} accept={post[1]} lexer-order={post[2]}")

    section("STEP 4  Shortest manifest per affected entry under the proposed left.1 change")
    for start in ("batch", "checkpoint"):
        print(f"\nENTRY = {start}")
        rejected, lost = [], []
        for (st, text), n in TEXTS.items():
            if st != start:
                continue
            left_res = parse_one("entry_left1.lark", start, text)
            if not left_res["accepted"]:
                rejected.append((n, text))
                continue
            right_res = parse_one("entry_right1.lark", start, text)
            if right_res["accepted"] and right_res["interpretation"] != left_res["interpretation"]:
                lost.append((n, text, left_res["interpretation"], right_res["interpretation"]))
        if rejected:
            n, t = min(rejected)
            print(f"  shortest REJECTED manifest: len={n} {t!r}")
        else:
            print("  shortest REJECTED manifest: NONE FOUND in this finite range (range exhausted, not budget)")
        if lost:
            n, t, keep, gone = min(lost)
            print(f"  shortest ACCEPTED-but-loses-interpretation: len={n} {t!r}")
            print(f"      kept under left.1 : {keep}")
            print(f"      lost interpretation: {gone} (proven by right.1 reading same text to EOF)")
        else:
            print("  shortest ACCEPTED-but-loses-interpretation: NONE FOUND in this finite range")


if __name__ == "__main__":
    main()
