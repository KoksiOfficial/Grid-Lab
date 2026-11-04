"""IEEE-118 cascade Monte Carlo core.

Workers import this module only. Dataset loading lives in loader.py and runs
in the parent process. Each worker receives the prepared topology once through
install_topo (Pool initializer).
"""

from __future__ import annotations

import random
import signal

N_BUSES = 118
N_BRANCHES = 186
MAX_K = 5
MAX_STEPS = N_BRANCHES
TRUNK_DEGREE = 3
CHUNKS_PER_WORKER = 4
SHOCK_MODES = ("weighted", "trunk", "uniform")

_TOPO: dict | None = None


def install_topo(topo: dict) -> None:
    """Store the topology for this process. Called once per worker, not per chunk."""
    global _TOPO
    _TOPO = topo


def install_topo_worker(topo: dict) -> None:
    """Pool initializer. Ctrl+C belongs to the parent; a worker must not die on it."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    install_topo(topo)


def current_topo() -> dict:
    if _TOPO is None:
        raise RuntimeError("topology was not installed in this process")
    return _TOPO


def unrated_capacity(p_ij: float, margin: float) -> float:
    """Capacity for lr <= 0. margin == 0 keeps the n01 §2.5 rule max(1.0, 2|P|)."""
    if margin == 0:
        return max(1.0, 2.0 * abs(p_ij))
    return margin * abs(p_ij)


def utilization(flow: float, capacity: float) -> float:
    if capacity > 0:
        return flow / capacity
    return float("inf") if flow > 0 else 0.0


def prepare_topology(topo: dict, bus_offset: int, unrated_margin: float, shock_mode: str) -> dict:
    """Flat, picklable lists for run_trial. Bus lists are indexed by bus id (index 0 unused)."""
    branches = topo["branches"]

    flow = [abs(b["flow"]) for b in branches]
    capacity = [b["capacity"] if b["capacity"] > 0 else unrated_capacity(b["flow"], unrated_margin)
                for b in branches]
    ends = [(b["from_bus"] + bus_offset, b["to_bus"] + bus_offset) for b in branches]

    load = [0.0] * (N_BUSES + 1)
    for bus in topo["buses"]:
        load[bus["id"]] = bus["load"]

    incident: list[list[int]] = [[] for _ in range(N_BUSES + 1)]
    for i, (u, v) in enumerate(ends):
        incident[u].append(i)
        if v != u:
            incident[v].append(i)

    if shock_mode == "weighted":
        candidates = [i for i in range(len(flow)) if 0 < utilization(flow[i], capacity[i]) < float("inf")]
        weights = [flow[i] / capacity[i] for i in candidates]
    elif shock_mode == "trunk":
        degree = [len(lines) for lines in incident]
        candidates = [i for i, (u, v) in enumerate(ends)
                      if degree[u] >= TRUNK_DEGREE or degree[v] >= TRUNK_DEGREE]
        weights = None
    elif shock_mode == "uniform":
        candidates = list(range(len(flow)))
        weights = None
    else:
        raise ValueError(f"Unknown shock mode {shock_mode!r}, expected one of {SHOCK_MODES}")

    total_consumption = sum(-p for p in load[1:] if p < 0)
    if total_consumption <= 0:
        raise ValueError("No buses with P_net < 0: consumption is zero, check data scale")

    return {
        "flow": flow,
        "capacity": capacity,
        "ends": ends,
        "load": load,
        "incident": incident,
        "shock_mode": shock_mode,
        "shock_candidates": candidates,
        "shock_weights": weights,
        "unrated_margin": unrated_margin,
        "total_consumption": total_consumption,
        "n_unrated": sum(1 for b in branches if b["capacity"] <= 0),
        "n_overloaded_initial": sum(1 for f, c in zip(flow, capacity) if f > c),
    }


def shock_share(topo: dict, line: int) -> float:
    """Probability that `line` is the first shocked line (k = 1 selection probability)."""
    candidates, weights = topo["shock_candidates"], topo["shock_weights"]
    if line not in candidates:
        return 0.0
    if weights is None:
        return 1.0 / len(candidates)
    return weights[candidates.index(line)] / sum(weights)


def trial_seed(base_seed: int, i: int) -> int:
    return base_seed * 1_000_003 + i


def sample_shock(rng: random.Random, candidates: list[int], weights: list[float] | None, k: int) -> list[int]:
    if weights is None:
        return rng.sample(candidates, k)
    pool, w = list(candidates), list(weights)
    picked = []
    for _ in range(k):
        j = rng.choices(range(len(pool)), weights=w)[0]
        picked.append(pool.pop(j))
        w.pop(j)
    return picked


def _redistribute(failed, flow, capacity, active, ends, incident) -> None:
    """Move flow of just-failed lines to ACTIVE neighbours, proportional to free reserve.

    All shares are computed from pre-update flows, then applied together.
    """
    delta: dict[int, float] = {}
    for f in failed:
        u, v = ends[f]
        neighbours = {j for j in incident[u] + incident[v] if active[j]}
        reserve = {j: capacity[j] - flow[j] for j in neighbours if capacity[j] - flow[j] > 0}
        total = sum(reserve.values())
        if total <= 0:
            continue
        for j, r in reserve.items():
            delta[j] = delta.get(j, 0.0) + flow[f] * r / total
    for f in failed:
        flow[f] = 0.0
    for j, d in delta.items():
        flow[j] += d


def run_trial(topo: dict, k: int, threshold: float, seed: int) -> dict:
    rng = random.Random(seed)
    capacity, ends, incident, load = topo["capacity"], topo["ends"], topo["incident"], topo["load"]
    flow = list(topo["flow"])
    n = len(flow)
    active = [True] * n

    initial = sample_shock(rng, topo["shock_candidates"], topo["shock_weights"], k)
    for i in initial:
        active[i] = False

    tripped: list[int] = []
    just_failed = initial
    for _ in range(MAX_STEPS):
        _redistribute(just_failed, flow, capacity, active, ends, incident)
        overloaded = [i for i in range(n) if active[i] and flow[i] > capacity[i]]
        if not overloaded:
            break
        worst = max(overloaded, key=lambda i: utilization(flow[i], capacity[i]))
        active[worst] = False
        tripped.append(worst)
        just_failed = [worst]

    lost = sum(-load[b] for b in range(1, N_BUSES + 1)
               if load[b] < 0 and not any(active[j] for j in incident[b]))
    dns_proxy = lost / topo["total_consumption"]
    blackout = dns_proxy > threshold

    return {
        "blackout": blackout,
        "dns_proxy": dns_proxy,
        "cascade_trips": len(tripped),
        "initial": initial,
        "tripped": tripped,
        "status": "BLACKOUT" if blackout else "STABLE",
    }


def _empty_stats(n_branches: int) -> dict:
    return {
        "trials": 0,
        "blackouts": 0,
        "dns_sum": 0.0,
        "cascade_trials": 0,
        "in_shock": [0] * n_branches,
        "in_shock_blackout": [0] * n_branches,
        "tripped": [0] * n_branches,
    }


def _example_of(index: int, result: dict) -> dict:
    return {
        "index": index,
        "cascade_trips": result["cascade_trips"],
        "dns_proxy": result["dns_proxy"],
        "blackout": result["blackout"],
        "initial": result["initial"],
        "tripped": result["tripped"],
    }


def _prefer_example(current: dict | None, candidate: dict) -> dict:
    """Prefer more cascade trips; tie goes to the smaller trial index."""
    if current is None:
        return candidate
    if candidate["cascade_trips"] > current["cascade_trips"]:
        return candidate
    if candidate["cascade_trips"] == current["cascade_trips"] and candidate["index"] < current["index"]:
        return candidate
    return current


def simulate_trials_chunk(args) -> dict:
    start, count, k, threshold, base_seed = args
    topo = current_topo()
    acc = _empty_stats(len(topo["flow"]))
    example = None
    for i in range(start, start + count):
        result = run_trial(topo, k, threshold, trial_seed(base_seed, i))
        acc["trials"] += 1
        acc["dns_sum"] += result["dns_proxy"]
        if result["cascade_trips"] > 0:
            acc["cascade_trials"] += 1
        if result["blackout"]:
            acc["blackouts"] += 1
        for line in result["initial"]:
            acc["in_shock"][line] += 1
            if result["blackout"]:
                acc["in_shock_blackout"][line] += 1
        for line in result["tripped"]:
            acc["tripped"][line] += 1
        example = _prefer_example(example, _example_of(i, result))
    acc["example"] = example
    return acc


def merge_stats(parts: list[dict], n_branches: int) -> dict:
    total = _empty_stats(n_branches)
    for part in parts:
        for key in ("trials", "blackouts", "dns_sum", "cascade_trials"):
            total[key] += part[key]
        for key in ("in_shock", "in_shock_blackout", "tripped"):
            total[key] = [a + b for a, b in zip(total[key], part[key])]
    return total


def tasks_for(start: int, count: int, k: int, threshold: float, base_seed: int, workers: int) -> list[tuple]:
    """Split a trial index range into chunks.

    One worker keeps a single chunk, matching grid-mimd, so dns_sum is added in
    trial order. Several workers use up to 4 chunks each.
    """
    if count < 1:
        raise ValueError("count must be >= 1")
    if workers <= 1:
        n_chunks = 1
    else:
        n_chunks = min(count, workers * CHUNKS_PER_WORKER)
    base, extra = divmod(count, n_chunks)
    tasks = []
    cursor = start
    for chunk in range(n_chunks):
        size = base + (1 if chunk < extra else 0)
        tasks.append((cursor, size, k, threshold, base_seed))
        cursor += size
    return tasks


def choose_example(parts: list[dict]) -> dict:
    best = None
    for part in parts:
        best = _prefer_example(best, part["example"])
    if best is None:
        raise RuntimeError("wave produced no trial")
    return best


def dark_buses(topo: dict, failed: set[int]) -> list[int]:
    """Consumers with no active incident line. Same condition as lost load in run_trial."""
    load = topo["load"]
    incident = topo["incident"]
    found = []
    for bus in range(1, N_BUSES + 1):
        if load[bus] < 0 and not any(line not in failed for line in incident[bus]):
            found.append(bus)
    return found


def frame_status(example: dict) -> str:
    if example["blackout"]:
        return "BLACKOUT"
    if example["cascade_trips"] > 0:
        return "CASCADE"
    return "STABLE"


def top_lines(topo: dict, stats: dict, top: int = 10) -> list[dict]:
    rows = []
    for i, n_shock in enumerate(stats["in_shock"]):
        if n_shock == 0:
            continue
        u, v = topo["ends"][i]
        rows.append({
            "branch": i,
            "from_bus": u,
            "to_bus": v,
            "in_shock": n_shock,
            "shock_share_expected": shock_share(topo, i),
            "p_blackout_given_shock": stats["in_shock_blackout"][i] / n_shock,
            "cascade_trips": stats["tripped"][i],
        })
    rows.sort(key=lambda r: (r["p_blackout_given_shock"], r["cascade_trips"]), reverse=True)
    return rows[:top]


def empty_stats() -> dict:
    return _empty_stats(N_BRANCHES)
