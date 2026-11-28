"""Replayable evidence harness for LALR-305.

Uses the real deployed entry point:
    Lark.open(<entry grammar>, parser='lalr', lexer='contextual',
              start=['batch', 'checkpoint'], maybe_placeholders=True,
              propagate_positions=True)
and walks the interactive parser so each step records the parser-state position,
the contextual lexer's active terminal set (and its tie-break order), the token
actually handed to the parser (with source coordinates), and whether $END was
reached and accepted.
"""
import sys
from copy import copy

from lark.lark import Lark
from lark.exceptions import UnexpectedInput, UnexpectedToken, UnexpectedCharacters
from lark.lexer import Token

PROBE = "/Users/guo/Documents/GSB/1109/batches/accounts55-u224-20261128/control/questions/GSB1109-20261122-U224-DE-LARK-305/workspace/probe"

VARIANTS = {
    "baseline(unresolved)": "entry.lark",
    "left.1 (proposed)": "entry_left1.lark",
    "right.1 (contrast)": "entry_right1.lark",
}


def make_lark(entry_file):
    return Lark.open(
        f"{PROBE}/{entry_file}",
        parser="lalr",
        lexer="contextual",
        start=["batch", "checkpoint"],
        maybe_placeholders=True,
        propagate_positions=True,
        keep_all_tokens=True,
        import_paths=[PROBE],
    )


def active_lexer_terms(frontend, state_pos):
    basic = frontend.lexer.lexers[state_pos]
    return [t.name for t in basic.terminals]


def replay(entry_file, start, text):
    print("-" * 78)
    print(f"grammar={entry_file}  entry(start)={start}  text={text!r}  len={len(text)}")
    try:
        lark = make_lark(entry_file)
    except Exception as e:
        print(f"  BUILD FAILED -> {type(e).__name__}: {str(e).splitlines()[0]}")
        print("  (no deployable parse table; this manifest cannot be read)")
        return None
    frontend = lark.parser
    ip = lark.parse_interactive(text, start=start)
    lexer_thread = ip.lexer_thread
    history = []
    last_token = None
    outcome = "UNKNOWN"
    try:
        for tok in lexer_thread.lex(ip.parser_state):
            pos_before = ip.parser_state.position
            choices_before = sorted(ip.choices().keys())
            active = active_lexer_terms(frontend, pos_before)
            coord = (tok.line, tok.column, tok.end_line, tok.end_column)
            span = (tok.start_pos, tok.end_pos)
            print(f"  state={pos_before:2} accept={choices_before}")
            print(f"      lexer-terminal-order={active}")
            print(f"      READ TOKEN {tok.type:14} {tok.value!r:8} "
                  f"line:col={coord} span={span} tail={text[tok.start_pos:]!r}")
            history.append((pos_before, tok.type, tok.value, span))
            last_token = tok
            ip.feed_token(tok)
        eof_accepted = "$END" in ip.choices()
        print(f"  after last token: state={ip.parser_state.position} "
              f"choices={sorted(ip.choices().keys())}")
        print(f"  EOF reached: yes; $END accepted at this state: {eof_accepted}")
        tree = ip.feed_eof(last_token)
        outcome = "ACCEPTED"
        print("  TREE:")
        for line in tree.pretty().splitlines():
            print("    " + line)
    except UnexpectedToken as e:
        outcome = "REJECTED(UnexpectedToken)"
        print(f"  REJECTED -> UnexpectedToken at state={ip.parser_state.position}")
        print(f"      offending={e.token.type!r} value={e.token.value!r} "
              f"span={(e.token.start_pos, e.token.end_pos)}")
        print(f"      parser expected={sorted(e.expected or [])}")
        if e.token.type == "$END":
            remaining = ""
            print("      EOF reached: yes, but $END not accepted")
        else:
            remaining = text[e.token.start_pos:] if e.token.start_pos is not None else text
            print("      EOF reached: no")
        print(f"      remaining tail handed over={remaining!r}")
    except UnexpectedCharacters as e:
        outcome = "REJECTED(UnexpectedCharacters)"
        tail_start = e.pos_in_stream if e.pos_in_stream is not None else 0
        print(f"  REJECTED -> UnexpectedCharacters line={e.line} col={e.column} "
              f"allowed={sorted(e.allowed or [])} tail={text[tail_start:]!r}")
        print("      EOF reached: no")
    print(f"  OUTCOME: {outcome}")
    return outcome


if __name__ == "__main__":
    samples = [
        ("batch", "sync delta asset;ok"),
        ("batch", "sync delta asset;ok?"),
        ("checkpoint", "resume delta asset;ok"),
        ("checkpoint", "resume delta asset;ok?"),
    ]
    only = sys.argv[1] if len(sys.argv) > 1 else None
    for label, entry_file in VARIANTS.items():
        if only and only not in entry_file:
            continue
        print("#" * 78)
        print(f"# VARIANT: {label} ({entry_file})")
        print("#" * 78)
        for start, text in samples:
            replay(entry_file, start, text)
