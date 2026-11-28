"""Static stand-in for the interegular strict collision check (offline environment).

interegular groups terminals by priority and only flags overlaps *within* one
priority group (see lark/lexer.py _check_regex_collisions: classify by t.priority).
Here we enumerate, for every contextual lexer state, the regex terminals in each
priority group and check whether two distinct terminals share the same regexp
(the only possible collision shape in this grammar). Run:

    .venv/bin/python probe/lex_collision_check.py
"""
from collections import defaultdict

from lark.lark import Lark

PROBE = "/Users/guo/Documents/GSB/1109/batches/accounts55-u224-20261128/control/questions/GSB1109-20261122-U224-DE-LARK-305/workspace/probe"

for entry in ("entry_left1.lark", "entry_right1.lark"):
    lark = Lark.open(f"{PROBE}/{entry}", parser="lalr", lexer="contextual",
                     start=["batch", "checkpoint"], import_paths=[PROBE])
    print(f"== {entry}")
    total_collisions = 0
    for state, basic in sorted(lark.parser.lexer.lexers.items()):
        groups = defaultdict(list)
        for t in basic.terminals:
            if t.pattern.type == "re":
                groups[t.priority].append((t.name, t.pattern.to_regexp()))
        for prio, items in groups.items():
            seen = {}
            for name, regexp in items:
                if regexp in seen:
                    total_collisions += 1
                    print(f"  COLLISION state={state} prio={prio}: {seen[regexp]} vs {name} ({regexp})")
                seen[regexp] = name
    print(f"  same-priority regex collisions across all contextual states: {total_collisions}")
