"""调度复核点：BigTree 现有 DAG 上的最小工时覆盖 + 公开通路约束。

DAG（docs_sphinx/source/demo/dag.md 规范示例）：
  a->c, a->d, b->c, c->d, c->f, c->g, d->e, d->f, g->h
入口: a（公开/内部）, b（供应商/敏感）
报表: e, f, h
工时: step 属性 (a=1, b=1, c=2, d=2, e=3; f/g/h 文档未给，不作为候选也不影响)
"""
from itertools import combinations

from bigtree import dict_to_dag

relation_dict = {
    "a": {"step": 1},
    "b": {"step": 1},
    "c": {"parents": ["a", "b"], "step": 2},
    "d": {"parents": ["a", "c"], "step": 2},
    "e": {"parents": ["d"], "step": 3},
    "f": {"parents": ["c", "d"]},
    "g": {"parents": ["c"]},
    "h": {"parents": ["g"]},
}
root = dict_to_dag(relation_dict, parent_key="parents")

# 从两个入口出发建立 name -> DAGNode 映射
nodes = {}
stack = [root]
while stack:
    node = stack.pop()
    if node.node_name in nodes:
        continue
    nodes[node.node_name] = node
    stack.extend(node.parents)
    stack.extend(node.children)
dag = nodes

SOURCES = ["a", "b"]
SINKS = ["e", "f", "h"]
SENSITIVE_SOURCE = "b"  # 供应商数据入口
CANDIDATES = ["c", "d", "g"]  # 沿途中间节点（不含入口与报表本身）
COST = {"c": 2, "d": 2, "g": 1}  # g 无 step 属性，按 1 计；不影响结论


def all_paths(src, dst):
    """枚举 src -> dst 的全部简单路径（按名字）。"""
    paths = []

    def dfs(node, trail):
        if node == dst:
            paths.append(trail[:])
            return
        for child in dag[node].children:
            dfs(child.node_name, trail + [child.node_name])

    dfs(src, [src])
    return paths


paths = {(s, t): all_paths(s, t) for s in SOURCES for t in SINKS}
print("=== 全部血缘链 ===")
for (s, t), ps in paths.items():
    for p in ps:
        print(f"  {s}->{t}: {' -> '.join(p)}")

sensitive_paths = [p for t in SINKS for p in paths[(SENSITIVE_SOURCE, t)]]
public_paths = {t: paths[("a", t)] for t in SINKS}


def covers(S):
    """S 是否封住所有敏感链，且每张报表保留一条零人工公开链。"""
    sealed = all(any(n in S for n in p) for p in sensitive_paths)
    public_ok = {
        t: any(not any(n in S for n in p) for p in public_paths[t]) for t in SINKS
    }
    return sealed, public_ok


print("\n=== 全约束求解（敏感链全覆盖 + 每张报表保留公开通路）===")
feasible = []
for r in range(1, len(CANDIDATES) + 1):
    for S in combinations(CANDIDATES, r):
        sealed, public_ok = covers(set(S))
        if sealed and all(public_ok.values()):
            feasible.append((sum(COST[n] for n in S), S))
if feasible:
    feasible.sort()
    print("  可行解:", feasible)
else:
    print("  无可行解 —— 条件顶住了。")

print("\n=== 逐报表诊断：封住敏感入口后，公开通路是否还保得住 ===")
for t in SINKS:
    ok_sets = []
    for r in range(1, len(CANDIDATES) + 1):
        for S in combinations(CANDIDATES, r):
            sealed, public_ok = covers(set(S))
            if sealed and public_ok[t]:
                ok_sets.append((sum(COST[n] for n in S), S))
    if ok_sets:
        ok_sets.sort()
        print(f"  报表 {t}: 可行，最省 {ok_sets[0][1]} 工时={ok_sets[0][0]}")
    else:
        print(f"  报表 {t}: 不可行 —— 公开通路与封敏感入口冲突")
        for p in public_paths[t]:
            print(f"    公开链: {' -> '.join(p)}")
        for p in paths[(SENSITIVE_SOURCE, t)]:
            print(f"    敏感链: {' -> '.join(p)}")

print("\n=== 放松冲突报表后的最优排班 ===")
best = None
for r in range(1, len(CANDIDATES) + 1):
    for S in combinations(CANDIDATES, r):
        sealed, public_ok = covers(set(S))
        if sealed:
            served = [t for t in SINKS if public_ok[t]]
            key = (len(served), -sum(COST[n] for n in S))
            if best is None or key > best[0]:
                best = (key, S, served, sum(COST[n] for n in S))
_, S, served, cost = best
print(f"  复核点: {sorted(S)}  总工时: {cost}  保住公开通路的报表: {served}")
sealed, public_ok = covers(set(S))
for t in SINKS:
    kept = [p for p in public_paths[t] if not any(n in S for n in p)]
    print(f"  报表 {t}: 公开通路 {' -> '.join(kept[0]) if kept else '无（被复核点截断）'}")
