"""Boundary probes: minimal single-letter manifests to confirm structural minima.

NAME/HIGH/LOW all match [a-z]+, so the shortest name and tail word are one letter.
These confirm accept/reject behaviour at the minimum character count, beyond the
three manifests physically held by the operator.
"""
from lark.lark import Lark
from lark.exceptions import UnexpectedToken

PROBE = "/Users/guo/Documents/GSB/1109/batches/accounts55-u224-20261128/control/questions/GSB1109-20261122-U224-DE-LARK-305/workspace/probe"


def open_lark(entry):
    return Lark.open(f"{PROBE}/{entry}", parser="lalr", lexer="contextual",
                     start=["batch", "checkpoint"], maybe_placeholders=True,
                     keep_all_tokens=True, import_paths=[PROBE])


def header_of(tree):
    return next(ch.data for ch in tree.children if hasattr(ch, "data") and ch.data in ("left", "right"))


def tail_of(tree):
    node = next(ch for ch in tree.children if hasattr(ch, "data") and ch.data in ("lt", "rt"))
    return node.data + "(" + ",".join(c.type for c in node.children if c is not None) + ")"


def try_parse(entry, start, text):
    lark = open_lark(entry)
    ip = lark.parse_interactive(text, start=start)
    for tok in ip.lexer_thread.lex(ip.parser_state):
        ip.feed_token(tok)
    if "$END" not in ip.choices():
        return f"REJECTED (expected {sorted(ip.choices().keys())})"
    tree = ip.feed_eof(None)
    return f"ACCEPTED {header_of(tree)};{tail_of(tree)}"


cases = [
    ("entry_left1.lark", "batch", "sync delta a;b"),
    ("entry_left1.lark", "batch", "sync delta a;b?"),
    ("entry_left1.lark", "checkpoint", "resume delta a;b"),
    ("entry_left1.lark", "checkpoint", "resume delta a;b?"),
    ("entry_right1.lark", "batch", "sync delta a;b"),
    ("entry_right1.lark", "checkpoint", "resume delta a;b"),
]
for entry, start, text in cases:
    print(f"{entry:18} {start:10} len={len(text):2} {text!r:22} -> {try_parse(entry, start, text)}")
