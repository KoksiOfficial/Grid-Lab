"""IEEE-118 cascade Monte Carlo engine: topology, run_trial, simulate_trials_chunk, GridSimulationEngine."""

from __future__ import annotations

import random
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import numpy as np

GRID_DIR = Path(__file__).resolve().parent
LAB_DIR = GRID_DIR.parents[1]
SCRIPTS_DIR = LAB_DIR.parent / "cloud_grid" / "source" / "dataset_scripts"
DEFAULT_DATA = LAB_DIR / "dataset" / "dataset_cascades"
sys.path.insert(0, str(SCRIPTS_DIR))

from load_powergraph_cascades import build_topology, load_powergraph_cascades  # noqa: E402

N_BUSES = 118
N_BRANCHES = 186
MAX_K = 5
MAX_STEPS = N_BRANCHES
TRUNK_DEGREE = 3
CHUNKS_PER_WORKER = 4
SHOCK_MODES = ("weighted", "trunk", "uniform")


# ---------------------------------------------------------------------------
# Topology
# ---------------------------------------------------------------------------

def pick_scenario(data: Path, seed: int) -> int:
    labels = load_powergraph_cascades(data, load_large=False)
    return random.Random(seed).randrange(labels.n_samples)


def check_loaded(pg, topo) -> int:
    """Validate sizes and bList range; return bus id offset (1 if bList is 0-based)."""
    if pg.blist.shape != (N_BRANCHES, 2):
        raise ValueError(f"bList shape {pg.blist.shape}, expected ({N_BRANCHES}, 2)")
    if len(topo["buses"]) != N_BUSES or len(topo["branches"]) != N_BRANCHES:
        raise ValueError(f"buses={len(topo['buses'])} branches={len(topo['branches'])}, "
                         f"expected {N_BUSES}/{N_BRANCHES}")
    if not np.array_equal(pg.blist, np.round(pg.blist)):
        raise ValueError("bList contains non-integer bus ids")
    lo, hi = int(pg.blist.min()), int(pg.blist.max())
    offset = 1 if lo == 0 else 0
    if lo + offset < 1 or hi + offset > N_BUSES:
        raise ValueError(f"bList ids in {lo}..{hi}, expected 1..{N_BUSES}")
    return offset


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


# ---------------------------------------------------------------------------
# Level B: one trial (sequential)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Level C: many trials
# ---------------------------------------------------------------------------

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


def simulate_trials_chunk(args) -> dict:
    topo, start, count, k, threshold, base_seed = args
    acc = _empty_stats(len(topo["flow"]))
    for i in range(start, start + count):
        r = run_trial(topo, k, threshold, trial_seed(base_seed, i))
        acc["trials"] += 1
        acc["dns_sum"] += r["dns_proxy"]
        if r["cascade_trips"] > 0:
            acc["cascade_trials"] += 1
        if r["blackout"]:
            acc["blackouts"] += 1
        for line in r["initial"]:
            acc["in_shock"][line] += 1
            if r["blackout"]:
                acc["in_shock_blackout"][line] += 1
        for line in r["tripped"]:
            acc["tripped"][line] += 1
    return acc


def merge_stats(parts: list[dict], n_branches: int) -> dict:
    total = _empty_stats(n_branches)
    for p in parts:
        for key in ("trials", "blackouts", "dns_sum", "cascade_trials"):
            total[key] += p[key]
        for key in ("in_shock", "in_shock_blackout", "tripped"):
            total[key] = [a + b for a, b in zip(total[key], p[key])]
    return total


class GridSimulationEngine:
    def __init__(self):
        self.pg = None
        self.topo: dict | None = None
        self.scenario: int | None = None
        self.reference: dict = {}
        self.stats: dict | None = None
        self.elapsed = 0.0
        self.reset_params()

    def load_scenario(self, data: Path, scenario: int | None, seed: int,
                      unrated_margin: float = 2.0, shock_mode: str = "weighted") -> None:
        s = pick_scenario(data, seed) if scenario is None else scenario
        pg = load_powergraph_cascades(data, sample_index=slice(s, s + 1))
        if pg.n_samples != 1:
            raise ValueError(f"Scenario {s} not found (loaded {pg.n_samples} samples)")
        raw = build_topology(pg.blist, pg.Ef[0], pg.Bf[0])
        offset = check_loaded(pg, raw)
        if offset:
            print("WARNING: bList is 0-based, bus ids shifted by +1")

        self.pg = pg
        self.scenario = s
        self.topo = prepare_topology(raw, offset, unrated_margin, shock_mode)
        self.reference = {"dns_MW": float(pg.of_reg[0]), "output_features": float(pg.of_bi[0])}

    def reset_params(self, trials: int = 1000, k: int = 1, seed: int = 42, threshold: float = 0.05) -> None:
        self.trials = trials
        self.k = k
        self.seed = seed
        self.threshold = threshold
        self.stats = None

    def _tasks(self, n_chunks: int) -> list[tuple]:
        n_chunks = max(1, min(n_chunks, self.trials))
        base, extra = divmod(self.trials, n_chunks)
        tasks, start = [], 0
        for c in range(n_chunks):
            count = base + (1 if c < extra else 0)
            tasks.append((self.topo, start, count, self.k, self.threshold, self.seed))
            start += count
        return tasks

    def run(self, workers: int) -> dict:
        n_candidates = len(self.topo["shock_candidates"])
        if n_candidates < self.k:
            raise ValueError(f"{n_candidates} shock candidates ({self.topo['shock_mode']}), k={self.k}")
        n_branches = len(self.topo["flow"])
        t0 = time.perf_counter()
        if workers == 1:
            parts = [simulate_trials_chunk(t) for t in self._tasks(1)]
        else:
            with Pool(processes=workers) as pool:
                parts = pool.map(simulate_trials_chunk, self._tasks(workers * CHUNKS_PER_WORKER))
        self.elapsed = time.perf_counter() - t0
        self.stats = merge_stats(parts, n_branches)
        return self.stats

    def top_lines(self, top: int = 10) -> list[dict]:
        st = self.stats
        rows = []
        for i, n_shock in enumerate(st["in_shock"]):
            if n_shock == 0:
                continue
            u, v = self.topo["ends"][i]
            rows.append({
                "branch": i,
                "from_bus": u,
                "to_bus": v,
                "in_shock": n_shock,
                "shock_share_expected": shock_share(self.topo, i),
                "p_blackout_given_shock": st["in_shock_blackout"][i] / n_shock,
                "cascade_trips": st["tripped"][i],
            })
        rows.sort(key=lambda r: (r["p_blackout_given_shock"], r["cascade_trips"]), reverse=True)
        return rows[:top]

    def get_snapshot(self, top: int = 10) -> dict:
        st = self.stats
        n = st["trials"]
        return {
            "scenario": self.scenario,
            "trials": n,
            "k": self.k,
            "seed": self.seed,
            "threshold": self.threshold,
            "shock_mode": self.topo["shock_mode"],
            "unrated_margin": self.topo["unrated_margin"],
            "p_blackout": st["blackouts"] / n,
            "mean_dns_proxy": st["dns_sum"] / n,
            "cascade_fraction": st["cascade_trials"] / n,
            "reference": self.reference,
            "top_lines": self.top_lines(top),
        }
