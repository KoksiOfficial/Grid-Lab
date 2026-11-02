"""IEEE-118 cascade Monte Carlo for Ray Grid.

The trial kernel is the stage-1 algorithm (list `run_trial`). Dataset files are
not imported here: the driver loads them once (`loader.py`) and passes the
prepared topology in. Ray workers only need this module and numpy.
"""

from __future__ import annotations

import pickle
import random
import time
from multiprocessing import Pool, cpu_count

import numpy as np

N_BUSES = 118
N_BRANCHES = 186
MAX_K = 5
MAX_STEPS = N_BRANCHES
TRUNK_DEGREE = 3
CHUNKS_PER_WORKER = 4
SHOCK_MODES = ("weighted", "trunk", "uniform")
BACKENDS = ("pool", "numpy", "ray")

_REMOTE = None


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


def _lost_load(load, active, incident) -> float:
    return sum(-load[b] for b in range(1, N_BUSES + 1)
               if load[b] < 0 and not any(active[j] for j in incident[b]))


def _trial_result(blackout: bool, dns_proxy: float, tripped: list[int], initial: list[int]) -> dict:
    return {
        "blackout": blackout,
        "dns_proxy": dns_proxy,
        "cascade_trips": len(tripped),
        "initial": initial,
        "tripped": tripped,
        "status": "BLACKOUT" if blackout else "STABLE",
    }


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

    dns_proxy = _lost_load(load, active, incident) / topo["total_consumption"]
    return _trial_result(dns_proxy > threshold, dns_proxy, tripped, initial)


def _redistribute_np(failed, flow, capacity, active, ends, incident) -> None:
    """Same shares as `_redistribute`, reading ndarray slots as Python floats."""
    delta: dict[int, float] = {}
    for f in failed:
        u, v = ends[f]
        neighbours = {j for j in incident[u] + incident[v] if bool(active[j])}
        reserve = {}
        for j in neighbours:
            free = float(capacity[j]) - float(flow[j])
            if free > 0:
                reserve[j] = free
        total = sum(reserve.values())
        if total <= 0:
            continue
        src = float(flow[f])
        for j, r in reserve.items():
            delta[j] = delta.get(j, 0.0) + src * r / total
    for f in failed:
        flow[f] = 0.0
    for j, d in delta.items():
        flow[j] = float(flow[j]) + d


def run_trial_numpy(topo: dict, k: int, threshold: float, seed: int) -> dict:
    """Same RNG and trip rule as `run_trial`. Flow, capacity, and the active mask are ndarrays."""
    rng = random.Random(seed)
    capacity = np.asarray(topo["capacity"], dtype=np.float64)
    flow = np.array(topo["flow"], dtype=np.float64, copy=True)
    ends, incident, load = topo["ends"], topo["incident"], topo["load"]
    active = np.ones(len(flow), dtype=bool)

    initial = sample_shock(rng, topo["shock_candidates"], topo["shock_weights"], k)
    active[np.asarray(initial, dtype=int)] = False

    tripped: list[int] = []
    just_failed = initial
    for _ in range(MAX_STEPS):
        _redistribute_np(just_failed, flow, capacity, active, ends, incident)
        overloaded = np.flatnonzero(active & (flow > capacity))
        if overloaded.size == 0:
            break
        utils = [utilization(float(flow[i]), float(capacity[i])) for i in overloaded]
        worst = int(overloaded[int(np.argmax(np.asarray(utils)))])
        active[worst] = False
        tripped.append(worst)
        just_failed = [worst]

    dns_proxy = _lost_load(load, active, incident) / topo["total_consumption"]
    return _trial_result(dns_proxy > threshold, dns_proxy, tripped, initial)


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


def _accumulate(acc: dict, result: dict) -> None:
    acc["trials"] += 1
    acc["dns_sum"] += result["dns_proxy"]
    if result["cascade_trips"] > 0:
        acc["cascade_trials"] += 1
    if result["blackout"]:
        acc["blackouts"] += 1
        for line in result["initial"]:
            acc["in_shock_blackout"][line] += 1
    for line in result["initial"]:
        acc["in_shock"][line] += 1
    for line in result["tripped"]:
        acc["tripped"][line] += 1


def _run_chunk(trial_fn, topo, start, count, k, threshold, base_seed) -> dict:
    acc = _empty_stats(len(topo["flow"]))
    for i in range(start, start + count):
        _accumulate(acc, trial_fn(topo, k, threshold, trial_seed(base_seed, i)))
    return acc


def simulate_trials_chunk(topo, start, count, k, threshold, base_seed) -> dict:
    return _run_chunk(run_trial, topo, start, count, k, threshold, base_seed)


def simulate_trials_chunk_numpy(topo, start, count, k, threshold, base_seed) -> dict:
    return _run_chunk(run_trial_numpy, topo, start, count, k, threshold, base_seed)


def simulate_trials_remote(topo, start, count, k, threshold, base_seed) -> dict:
    """Ray task body. The driver wraps it with `ray.remote` once; workers import this function."""
    return simulate_trials_chunk(topo, start, count, k, threshold, base_seed)


def get_remote():
    global _REMOTE
    if _REMOTE is None:
        import ray
        _REMOTE = ray.remote(simulate_trials_remote)
    return _REMOTE


def merge_stats(parts: list[dict], n_branches: int) -> dict:
    total = _empty_stats(n_branches)
    for p in parts:
        for key in ("trials", "blackouts", "dns_sum", "cascade_trials"):
            total[key] += p[key]
        for key in ("in_shock", "in_shock_blackout", "tripped"):
            total[key] = [a + b for a, b in zip(total[key], p[key])]
    return total


def integer_stats(stats: dict) -> tuple:
    return (
        stats["trials"],
        stats["blackouts"],
        stats["cascade_trials"],
        tuple(stats["in_shock"]),
        tuple(stats["in_shock_blackout"]),
        tuple(stats["tripped"]),
    )


def chunk_bounds(trials: int, slots: int) -> list[tuple[int, int]]:
    """Stage-1 split: 1 chunk when slots <= 1, otherwise min(trials, slots * 4)."""
    n_chunks = 1 if slots <= 1 else min(trials, slots * CHUNKS_PER_WORKER)
    n_chunks = max(1, min(n_chunks, trials))
    base, extra = divmod(trials, n_chunks)
    bounds, start = [], 0
    for c in range(n_chunks):
        count = base + (1 if c < extra else 0)
        bounds.append((start, count))
        start += count
    return bounds


def resolve_slots(backend: str, explicit: int | None) -> int:
    if explicit is not None:
        return explicit
    if backend == "ray":
        import ray
        cpus = int(ray.cluster_resources().get("CPU", 0)) if ray.is_initialized() else 0
        return max(1, cpus or cpu_count() or 1)
    return cpu_count() or 1


def get_cluster_summary() -> dict:
    try:
        import ray
    except Exception:
        return {"connected": False, "nodes": 0, "cpus": 0, "node_details": []}
    if not ray.is_initialized():
        return {"connected": False, "nodes": 0, "cpus": 0, "node_details": []}
    nodes = [n for n in ray.nodes() if n.get("Alive")]
    details = []
    for node in nodes:
        res = node.get("Resources", {})
        details.append({
            "node_id": node.get("NodeID", "")[:8],
            "ip": node.get("NodeManagerAddress", "?"),
            "cpus": int(res.get("CPU", 0)),
        })
    return {
        "connected": True,
        "nodes": len(nodes),
        "cpus": int(ray.cluster_resources().get("CPU", 0)),
        "node_details": details,
    }


def top_lines(stats: dict, topo: dict, top: int = 10) -> list[dict]:
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


def snapshot(stats: dict, topo: dict, reference: dict, scenario: int, k: int, seed: int, threshold: float) -> dict:
    n = stats["trials"]
    return {
        "scenario": scenario,
        "trials": n,
        "k": k,
        "seed": seed,
        "threshold": threshold,
        "shock_mode": topo["shock_mode"],
        "unrated_margin": topo["unrated_margin"],
        "p_blackout": stats["blackouts"] / n,
        "mean_dns_proxy": stats["dns_sum"] / n,
        "cascade_fraction": stats["cascade_trials"] / n,
        "reference": reference,
        "top_lines": top_lines(stats, topo),
    }


def _execute(backend: str, topo: dict, trials: int, k: int, threshold: float, seed: int, slots: int):
    """One barrier over all trials. Returns (stats, elapsed_s, put_ms, n_chunks)."""
    n_candidates = len(topo["shock_candidates"])
    if n_candidates < k:
        raise ValueError(f"{n_candidates} shock candidates ({topo['shock_mode']}), k={k}")
    bounds = chunk_bounds(trials, slots)
    n_branches = len(topo["flow"])
    put_ms = None

    if backend == "ray":
        import ray
        t_put = time.perf_counter()
        topo_ref = ray.put(topo)
        put_ms = (time.perf_counter() - t_put) * 1000.0
        remote = get_remote()
        t0 = time.perf_counter()
        futures = [
            remote.remote(topo_ref, start, count, k, threshold, seed)
            for start, count in bounds
        ]
        parts = ray.get(futures)
        elapsed = time.perf_counter() - t0
    elif slots <= 1:
        trial_chunk = simulate_trials_chunk_numpy if backend == "numpy" else simulate_trials_chunk
        t0 = time.perf_counter()
        parts = [trial_chunk(topo, start, count, k, threshold, seed) for start, count in bounds]
        elapsed = time.perf_counter() - t0
    else:
        worker = simulate_trials_chunk_numpy if backend == "numpy" else simulate_trials_chunk
        tasks = [(topo, start, count, k, threshold, seed) for start, count in bounds]
        t0 = time.perf_counter()
        with Pool(processes=slots) as pool:
            parts = pool.starmap(worker, tasks)
        elapsed = time.perf_counter() - t0

    return merge_stats(parts, n_branches), elapsed, put_ms, len(bounds)


def run_benchmark(loaded: dict, *, trials: int, k: int, seed: int, threshold: float,
                  workers: int | None, backends: tuple[str, ...]) -> dict:
    topo = loaded["topo"]
    topo_bytes = len(pickle.dumps(topo))
    records = []
    keys = []
    for backend in backends:
        slots = resolve_slots(backend, workers)
        n_chunks_planned = len(chunk_bounds(trials, slots))
        print(f"backend={backend}  slots={slots}  chunks={n_chunks_planned}")
        stats, elapsed, put_ms, n_chunks = _execute(backend, topo, trials, k, threshold, seed, slots)
        keys.append(integer_stats(stats))
        rate = trials / elapsed if elapsed > 0 else 0.0
        cluster = get_cluster_summary() if backend == "ray" else None
        record = {
            "architecture": f"RAY_GRID_{backend.upper()}",
            "backend": backend,
            "scenario": loaded["scenario"],
            "k": k,
            "seed": seed,
            "threshold": threshold,
            "shock_mode": topo["shock_mode"],
            "unrated_margin": topo["unrated_margin"],
            "trials": trials,
            "chunks": n_chunks,
            "cpu_workers": slots,
            "topology_bytes": topo_bytes,
            "topology_put_ms": None if put_ms is None else round(put_ms, 3),
            "time_includes_pool_startup": backend in ("pool", "numpy") and slots > 1,
            "cluster": cluster,
            "metrics": {
                "total_execution_time_sec": round(elapsed, 4),
                "throughput_trials_per_sec": round(rate, 1),
            },
            "result": snapshot(stats, topo, loaded["reference"], loaded["scenario"], k, seed, threshold),
        }
        records.append(record)
        print(
            f"  time={elapsed:.3f}s  {rate:,.1f} trials/s  "
            f"P(blackout)={record['result']['p_blackout']:.5f}  "
            f"cascade={record['result']['cascade_fraction']:.5f}"
        )
    deterministic = None
    if len(records) > 1:
        deterministic = all(key == keys[0] for key in keys[1:])
    return {
        "benchmark_suite": "ray_grid_power",
        "compare_mode": len(backends) > 1,
        "deterministic_across_backends": deterministic,
        "runs": records if len(records) > 1 else records[0],
    }
