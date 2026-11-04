#!/usr/bin/env python3
"""IEEE-118 cascade Monte Carlo web server.

    python app.py
    python app.py --headless --trials 10000 --k 1 --scenario 83810 --workers 1

One process loads a scenario once and hands the topology to Pool workers
through an initializer. /api/step runs trials_per_step independent trials.
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from multiprocessing import Pool, cpu_count
from pathlib import Path

from engine import (
    MAX_K,
    MAX_STEPS,
    N_BRANCHES,
    SHOCK_MODES,
    choose_example,
    dark_buses,
    empty_stats,
    frame_status,
    install_topo,
    install_topo_worker,
    merge_stats,
    simulate_trials_chunk,
    tasks_for,
    top_lines,
)
GRID_DIR = Path(__file__).resolve().parent
DEFAULT_OUT = GRID_DIR / "benchmark_results_docker_web.json"
MAX_BATCH_STEPS = 100
MAX_TRIALS_PER_STEP = 100_000
SESSION_TTL = 300

CONFIG = None
WORLD = None
SESSIONS = None
SHARED_POOL = None
POOL_LOCK = threading.Lock()


@dataclass
class Config:
    headless: bool
    port: int
    workers: int
    k: int
    trials: int
    trials_per_step: int
    scenario: int
    seed: int
    threshold: float
    shock: str
    unrated_margin: float
    data: Path
    script_dir: Path
    out: Path


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="IEEE-118 cascade Monte Carlo web server")
    parser.add_argument("--headless", action="store_true", help="Benchmark over workers 1, 2, 4, ... --workers")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--k", type=int, default=None)
    parser.add_argument("--trials", type=int, default=1000, help="Headless trial count")
    parser.add_argument("--trials-per-step", type=int, default=None)
    parser.add_argument("--scenario", type=int, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--threshold", type=float, default=None)
    parser.add_argument("--shock", choices=SHOCK_MODES, default=None)
    parser.add_argument("--unrated-margin", type=float, default=None)
    parser.add_argument("--data", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    return parser


def dataset_loader():
    """Import the dataset loader on demand so Pool workers never open it."""
    import loader
    return loader


def build_config(args) -> Config:
    loader = dataset_loader()
    if args.headless:
        k = 1 if args.k is None else args.k
    else:
        k = env_int("SIM_K", 2) if args.k is None else args.k
    shock = args.shock or os.environ.get("SIM_SHOCK") or "weighted"
    if shock not in SHOCK_MODES:
        raise ValueError(f"shock must be one of {', '.join(SHOCK_MODES)}")
    margin = env_float("SIM_UNRATED_MARGIN", 2.0) if args.unrated_margin is None else args.unrated_margin
    if not (margin == 0 or margin >= 1):
        raise ValueError("unrated margin must be 0 or >= 1")
    threshold = env_float("SIM_THRESHOLD", 0.05) if args.threshold is None else args.threshold
    if not (0 < threshold <= 1):
        raise ValueError("threshold must be in (0, 1]")
    trials_per_step = env_int("SIM_TRIALS_PER_STEP", 1000) if args.trials_per_step is None else args.trials_per_step
    if not 1 <= trials_per_step <= MAX_TRIALS_PER_STEP:
        raise ValueError(f"trials per step must be in 1..{MAX_TRIALS_PER_STEP}")
    workers = cpu_count() or 1
    if args.workers is not None:
        workers = args.workers
    elif os.environ.get("SIM_WORKERS"):
        workers = env_int("SIM_WORKERS", workers)
    if workers < 1:
        raise ValueError("workers must be >= 1")
    if args.trials < 1:
        raise ValueError("trials must be >= 1")
    scenario = env_int("SIM_SCENARIO", 83810) if args.scenario is None else args.scenario
    if scenario < 0:
        raise ValueError("scenario must be >= 0")
    return Config(
        headless=args.headless,
        port=env_int("PORT", 8080) if args.port is None else args.port,
        workers=workers,
        k=k,
        trials=args.trials,
        trials_per_step=trials_per_step,
        scenario=scenario,
        seed=env_int("SIM_SEED", 42) if args.seed is None else args.seed,
        threshold=threshold,
        shock=shock,
        unrated_margin=margin,
        data=args.data or loader.resolve_data_dir(),
        script_dir=loader.resolve_script_dir(),
        out=args.out,
    )


def ensure_k(topo: dict, k: int) -> None:
    max_k = min(MAX_K, len(topo["flow"]) // 10)
    if not 1 <= k <= max_k:
        raise ValueError(f"k must be in 1..{max_k}")
    n_candidates = len(topo["shock_candidates"])
    if k > n_candidates:
        raise ValueError(f"{n_candidates} shock candidates ({topo['shock_mode']}), k={k}")


def get_shared_pool(workers: int, topo: dict):
    global SHARED_POOL
    if SHARED_POOL is None:
        SHARED_POOL = Pool(processes=workers, initializer=install_topo_worker, initargs=(topo,))
    return SHARED_POOL


def shutdown_shared_pool(terminate: bool = False) -> None:
    global SHARED_POOL
    if SHARED_POOL is not None:
        if terminate:
            SHARED_POOL.terminate()
        else:
            SHARED_POOL.close()
        SHARED_POOL.join()
        SHARED_POOL = None


def commit_world(loaded: dict) -> None:
    """Install a freshly loaded scenario and hand it to the workers once."""
    global WORLD
    shutdown_shared_pool()
    WORLD = loaded
    install_topo(loaded["topo"])
    if CONFIG.workers > 1:
        get_shared_pool(CONFIG.workers, loaded["topo"])
    if SESSIONS is not None:
        SESSIONS.clear_counters()
    print(f"loaded scenario s={loaded['scenario']} once")


def execute_wave(start: int, count: int, k: int, threshold: float, seed: int):
    """Run one index range. Caller holds POOL_LOCK. Timer starts after the pool exists."""
    topo = WORLD["topo"]
    workers = CONFIG.workers
    tasks = tasks_for(start, count, k, threshold, seed, workers)
    install_topo(topo)
    if workers == 1:
        t0 = time.perf_counter()
        parts = [simulate_trials_chunk(task) for task in tasks]
        return parts, time.perf_counter() - t0
    pool = get_shared_pool(workers, topo)
    t0 = time.perf_counter()
    parts = pool.map(simulate_trials_chunk, tasks)
    return parts, time.perf_counter() - t0


def execute_local(topo: dict, start: int, count: int, k: int, threshold: float, seed: int, workers: int):
    """Headless run. The pool is created before the timer and closed after it."""
    tasks = tasks_for(start, count, k, threshold, seed, workers)
    if workers == 1:
        install_topo(topo)
        t0 = time.perf_counter()
        parts = [simulate_trials_chunk(task) for task in tasks]
        return parts, time.perf_counter() - t0
    pool = Pool(processes=workers, initializer=install_topo_worker, initargs=(topo,))
    try:
        t0 = time.perf_counter()
        parts = pool.map(simulate_trials_chunk, tasks)
        elapsed = time.perf_counter() - t0
    finally:
        pool.close()
        pool.join()
    return parts, elapsed


def build_frame(session, example: dict, trials_this_step: int, elapsed_s: float) -> dict:
    stats = session.stats
    n = stats["trials"]
    topo = WORLD["topo"]
    failed = set(example["initial"]) | set(example["tripped"])
    return {
        "session_id": session.sid,
        "tick": session.tick,
        "trials_this_step": trials_this_step,
        "trials_total": n,
        "calc_time_ms": round(elapsed_s * 1000, 1),
        "p_blackout": (stats["blackouts"] / n) if n else 0.0,
        "mean_dns_proxy": (stats["dns_sum"] / n) if n else 0.0,
        "cascade_fraction": (stats["cascade_trials"] / n) if n else 0.0,
        "status": frame_status(example),
        "k": session.k,
        "threshold": session.threshold,
        "display": {
            "trial_index": example["index"],
            "cascade_trips": example["cascade_trips"],
            "dns_proxy": example["dns_proxy"],
            "blackout": bool(example["blackout"]),
            "shock": list(example["initial"]),
            "tripped": list(example["tripped"]),
            "dark_buses": dark_buses(topo, failed),
        },
        "top_lines": top_lines(topo, stats, 10),
    }


class SessionState:
    def __init__(self, sid: str, defaults: dict):
        self.sid = sid
        self.k = defaults["k"]
        self.seed = defaults["seed"]
        self.threshold = defaults["threshold"]
        self.trials_per_step = defaults["trials_per_step"]
        self.reset_counters()

    def reset_counters(self) -> None:
        self.cursor = 0
        self.tick = 0
        self.stats = empty_stats()

    def run_wave(self) -> dict:
        topo = WORLD["topo"]
        ensure_k(topo, self.k)
        count = self.trials_per_step
        parts, elapsed = execute_wave(self.cursor, count, self.k, self.threshold, self.seed)
        merged = merge_stats(parts, len(topo["flow"]))
        self.stats = merge_stats([self.stats, merged], len(topo["flow"]))
        self.cursor += count
        self.tick += 1
        return build_frame(self, choose_example(parts), count, elapsed)


class SessionManager:
    def __init__(self, defaults: dict, ttl: int = SESSION_TTL):
        self.sessions = {}
        self.last_active = {}
        self.defaults = defaults
        self.ttl = ttl
        self._lock = threading.Lock()

    def get(self, sid: str) -> SessionState:
        now = time.time()
        with self._lock:
            self._cleanup(now)
            if sid not in self.sessions:
                self.sessions[sid] = SessionState(sid, self.defaults)
            self.last_active[sid] = now
            return self.sessions[sid]

    def count(self) -> int:
        with self._lock:
            return len(self.sessions)

    def clear_counters(self) -> None:
        with self._lock:
            for session in self.sessions.values():
                session.reset_counters()

    def _cleanup(self, now: float) -> None:
        expired = [sid for sid, last in self.last_active.items() if now - last > self.ttl]
        for sid in expired:
            self.sessions.pop(sid, None)
            self.last_active.pop(sid, None)


def as_int(value, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be an integer")
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{name} must be an integer")
    return int(value)


def as_float(value, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    return float(value)


def parse_reset(params: dict, session: SessionState):
    """Validate a reset body. Returns the new session fields and a loaded world, or None."""
    new = {
        "k": session.k,
        "seed": session.seed,
        "threshold": session.threshold,
        "trials_per_step": session.trials_per_step,
        "scenario": WORLD["scenario"],
        "shock": WORLD["topo"]["shock_mode"],
        "unrated_margin": WORLD["topo"]["unrated_margin"],
    }
    if "k" in params:
        new["k"] = as_int(params["k"], "k")
    if "seed" in params:
        new["seed"] = as_int(params["seed"], "seed")
    if "threshold" in params:
        new["threshold"] = as_float(params["threshold"], "threshold")
    if "trials_per_step" in params:
        new["trials_per_step"] = as_int(params["trials_per_step"], "trials_per_step")
    if "scenario" in params:
        new["scenario"] = as_int(params["scenario"], "scenario")
    if "shock" in params:
        new["shock"] = params["shock"]
    if "unrated_margin" in params:
        new["unrated_margin"] = as_float(params["unrated_margin"], "unrated_margin")

    if new["shock"] not in SHOCK_MODES:
        raise ValueError(f"shock must be one of {', '.join(SHOCK_MODES)}")
    if not (0 < new["threshold"] <= 1):
        raise ValueError("threshold must be in (0, 1]")
    if not 1 <= new["trials_per_step"] <= MAX_TRIALS_PER_STEP:
        raise ValueError(f"trials_per_step must be in 1..{MAX_TRIALS_PER_STEP}")
    if new["scenario"] < 0:
        raise ValueError("scenario must be >= 0")
    if not (new["unrated_margin"] == 0 or new["unrated_margin"] >= 1):
        raise ValueError("unrated_margin must be 0 or >= 1")

    reload = (
        new["scenario"] != WORLD["scenario"]
        or new["shock"] != WORLD["topo"]["shock_mode"]
        or new["unrated_margin"] != WORLD["topo"]["unrated_margin"]
    )
    loaded = None
    if reload:
        from loader import load_once
        loaded = load_once(
            WORLD["data"], new["scenario"], new["unrated_margin"], new["shock"], WORLD["script_dir"],
        )
        loaded["data"] = WORLD["data"]
        loaded["script_dir"] = WORLD["script_dir"]
        ensure_k(loaded["topo"], new["k"])
    else:
        ensure_k(WORLD["topo"], new["k"])
    return new, loaded


def parse_steps(params: dict) -> int:
    if "steps" not in params:
        return 10
    steps = as_int(params["steps"], "steps")
    if not 1 <= steps <= MAX_BATCH_STEPS:
        raise ValueError(f"steps must be in 1..{MAX_BATCH_STEPS}")
    return steps


def edge_payload(topo: dict) -> list[dict]:
    return [
        {"id": i, "from": u, "to": v, "flow": topo["flow"][i], "capacity": topo["capacity"][i]}
        for i, (u, v) in enumerate(topo["ends"])
    ]


def health_payload() -> dict:
    pool_ready = CONFIG.workers <= 1 or SHARED_POOL is not None
    return {
        "status": "healthy",
        "service": "ieee118-cascade",
        "stage": "03_web",
        "workers": CONFIG.workers,
        "scenario": WORLD["scenario"],
        "dataset_loaded": True,
        "pool": pool_ready,
        "timestamp": time.time(),
    }


def metrics_payload() -> dict:
    topo = WORLD["topo"]
    return {
        "service": "ieee118-cascade",
        "active_sessions": SESSIONS.count(),
        "workers": CONFIG.workers,
        "pid": os.getpid(),
        "scenario": WORLD["scenario"],
        "shock_mode": topo["shock_mode"],
        "unrated_margin": topo["unrated_margin"],
        "reference": WORLD["reference"],
        "layout": WORLD["layout"],
        "nodes": WORLD["nodes"],
        "edges": edge_payload(topo),
    }


def worker_counts(max_workers: int) -> list[int]:
    counts, n = [], 1
    while n < max_workers:
        counts.append(n)
        n *= 2
    counts.append(max_workers)
    return counts


def integer_stats(stats: dict) -> tuple:
    return (
        stats["trials"], stats["blackouts"], stats["cascade_trials"],
        tuple(stats["in_shock"]), tuple(stats["in_shock_blackout"]), tuple(stats["tripped"]),
    )


def speedup_path(out: Path) -> Path:
    if out.name == DEFAULT_OUT.name:
        return out.with_name("speedup.png")
    return out.with_name(f"{out.stem}_speedup.png")


def plot_speedup(runs: list[dict], out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ns = [r["workers"] for r in runs]
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(ns, [r["speedup"] for r in runs], "o-", label="measured")
    ax.plot(ns, ns, "--", color="#64748b", label="ideal")
    ax.set_xlabel("workers")
    ax.set_ylabel("speedup = time(1) / time(n)")
    ax.set_title("IEEE-118 cascade MC: speedup vs workers")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    plt.close(fig)


def snapshot(loaded: dict, stats: dict, k: int, seed: int, threshold: float) -> dict:
    topo = loaded["topo"]
    n = stats["trials"]
    return {
        "scenario": loaded["scenario"],
        "trials": n,
        "k": k,
        "seed": seed,
        "threshold": threshold,
        "shock_mode": topo["shock_mode"],
        "unrated_margin": topo["unrated_margin"],
        "p_blackout": stats["blackouts"] / n,
        "mean_dns_proxy": stats["dns_sum"] / n,
        "cascade_fraction": stats["cascade_trials"] / n,
        "reference": loaded["reference"],
        "top_lines": top_lines(topo, stats, 10),
    }


def run_headless(cfg: Config) -> int:
    from loader import load_once
    try:
        loaded = load_once(cfg.data, cfg.scenario, cfg.unrated_margin, cfg.shock, cfg.script_dir)
        ensure_k(loaded["topo"], cfg.k)
    except Exception as exc:
        print(f"failed to load scenario: {exc}", file=sys.stderr)
        return 1
    print(f"loaded scenario s={loaded['scenario']} once")
    topo = loaded["topo"]
    runs = []
    reference = None
    deterministic = True
    last_stats = None
    for n in worker_counts(cfg.workers):
        parts, elapsed = execute_local(topo, 0, cfg.trials, cfg.k, cfg.threshold, cfg.seed, n)
        stats = merge_stats(parts, N_BRANCHES)
        key = integer_stats(stats)
        if reference is None:
            reference = key
        elif key != reference:
            deterministic = False
        last_stats = stats
        runs.append({
            "workers": n,
            "time_s": elapsed,
            "trials_per_sec": cfg.trials / elapsed,
        })
        print(f"workers={n:<3} time={elapsed:8.3f}s  {cfg.trials / elapsed:10.1f} trials/s")

    t1 = runs[0]["time_s"]
    for row in runs:
        n = row["workers"]
        row["speedup"] = t1 / row["time_s"]
        row["efficiency"] = row["speedup"] / n
        row["amdahl_serial_fraction"] = (n / row["speedup"] - 1) / (n - 1) if n > 1 else None

    result = {
        "params": {
            "trials": cfg.trials,
            "k": cfg.k,
            "seed": cfg.seed,
            "threshold": cfg.threshold,
            "shock_mode": topo["shock_mode"],
            "unrated_margin": topo["unrated_margin"],
            "cpu_count": os.cpu_count(),
            "max_steps": MAX_STEPS,
            "trip_rule": "one worst overloaded line per step",
            "shock_candidates": len(topo["shock_candidates"]),
            "n_unrated": topo["n_unrated"],
            "n_overloaded_initial": topo["n_overloaded_initial"],
            "topology_pickle_bytes": len(pickle.dumps(topo)),
            "time_includes_pool_startup": False,
        },
        "deterministic_across_workers": deterministic,
        "runs": runs,
        "result": snapshot(loaded, last_stats, cfg.k, cfg.seed, cfg.threshold),
    }
    cfg.out.parent.mkdir(parents=True, exist_ok=True)
    cfg.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Saved: {cfg.out.resolve()}")
    png = speedup_path(cfg.out)
    plot_speedup(runs, png)
    print(f"Saved: {png.resolve()}")
    snap = result["result"]
    print(f"P(blackout)={snap['p_blackout']:.5f}  mean dns={snap['mean_dns_proxy']:.8g}  "
          f"cascade={snap['cascade_fraction']:.5f}")
    for row in snap["top_lines"][:3]:
        print(f"branch {row['branch']} {row['from_bus']}-{row['to_bus']} "
              f"P(blackout|shock)={row['p_blackout_given_shock']:.4f}")
    if not deterministic:
        print("WARNING: results differ between worker counts")
    return 0


HTML_PAGE = """<!DOCTYPE html>
<html lang="uk">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IEEE-118 cascade</title>
    <style>
        :root {
            --bg-color: #0b0f19;
            --panel-bg: #111827;
            --panel-border: #1f2937;
            --text-primary: #f3f4f6;
            --text-secondary: #9ca3af;
            --accent: #38bdf8;
            --danger: #ef4444;
            --warning: #f59e0b;
            --success: #10b981;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: var(--bg-color);
            color: var(--text-primary);
            padding: 18px;
            display: flex;
            flex-direction: column;
            gap: 16px;
            min-height: 100vh;
        }
        header, .card {
            background: var(--panel-bg);
            border: 1px solid var(--panel-border);
            border-radius: 10px;
        }
        header {
            padding: 14px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
        }
        header h1 { font-size: 1.25rem; color: var(--accent); }
        header p { font-size: 0.78rem; color: var(--text-secondary); margin-top: 4px; }
        .badge {
            background: #1e293b;
            color: #38bdf8;
            border: 1px solid #334155;
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 0.75rem;
            font-family: monospace;
        }
        .main-layout {
            display: grid;
            grid-template-columns: 360px 1fr;
            gap: 16px;
            flex: 1;
        }
        @media (max-width: 900px) {
            .main-layout { grid-template-columns: 1fr; }
        }
        .card { padding: 18px; display: flex; flex-direction: column; gap: 12px; }
        .section-title {
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-secondary);
            font-weight: 700;
            border-bottom: 1px solid var(--panel-border);
            padding-bottom: 6px;
        }
        .form-row { display: flex; flex-direction: column; gap: 6px; }
        .form-row label { font-size: 0.82rem; color: var(--text-secondary); display: flex; justify-content: space-between; }
        input[type="range"] { accent-color: var(--accent); }
        input[type="number"], select {
            background: #0f172a;
            color: var(--text-primary);
            border: 1px solid #334155;
            border-radius: 6px;
            padding: 8px 10px;
        }
        .btn-group { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
        .btn {
            background: #1f2937;
            color: var(--text-primary);
            border: 1px solid #374151;
            padding: 10px 14px;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 600;
            font-size: 0.85rem;
        }
        .btn:hover { background: #374151; }
        .btn-primary { background: #0284c7; border-color: #0369a1; color: #fff; }
        .btn-danger { background: #991b1b; border-color: #7f1d1d; color: #fff; }
        .metrics-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
        .metric-box { background: #0f172a; border: 1px solid #1e293b; padding: 10px 12px; border-radius: 6px; }
        .metric-label { font-size: 0.72rem; color: var(--text-secondary); text-transform: uppercase; }
        .metric-value { font-size: 1.05rem; font-weight: 700; margin-top: 4px; font-family: monospace; }
        .hint, #infoLine, #referenceLine, #layoutNote, #topLines {
            font-size: 0.78rem;
            color: var(--text-secondary);
            line-height: 1.4;
        }
        #infoLine { color: #e2e8f0; min-height: 1.2em; }
        .canvas-wrap {
            background: #050811;
            border: 1px solid var(--panel-border);
            border-radius: 10px;
            position: relative;
            min-height: 520px;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 10px;
            padding: 12px;
        }
        canvas { width: 100%; height: auto; max-width: 720px; background: #050811; }
        .hud {
            position: absolute;
            top: 14px;
            left: 14px;
            background: rgba(17, 24, 39, 0.85);
            border: 1px solid #374151;
            border-radius: 6px;
            padding: 8px 12px;
            font-family: monospace;
            font-size: 0.78rem;
            color: #94a3b8;
            pointer-events: none;
        }
        .legend { display: flex; flex-wrap: wrap; gap: 10px 14px; font-size: 0.75rem; color: var(--text-secondary); }
        .swatch { display: inline-block; width: 18px; height: 4px; margin-right: 6px; vertical-align: middle; border-radius: 2px; }
        .swatch.util { background: linear-gradient(90deg, #0d0887, #cc4778, #f0f921); height: 8px; }
        .swatch.shock { background: #f59e0b; height: 8px; }
        .swatch.cascade { background: #ef4444; height: 8px; }
        .swatch.dark { background: transparent; border: 2px solid #64748b; border-radius: 50%; width: 10px; height: 10px; }
        .status-pill { display: inline-block; padding: 2px 8px; border-radius: 4px; font-weight: 700; }
        .status-STABLE { background: rgba(16, 185, 129, 0.2); color: #34d399; }
        .status-CASCADE { background: rgba(245, 158, 11, 0.2); color: #fbbf24; }
        .status-BLACKOUT { background: rgba(239, 68, 68, 0.2); color: #f87171; }
        .status-READY { background: rgba(56, 189, 248, 0.15); color: #7dd3fc; }
    </style>
</head>
<body>
    <header>
        <div>
            <h1>IEEE-118 cascade</h1>
            <p>Одна хвиля /api/step — це кілька незалежних випробувань. Граф показує одне з них.</p>
            <p id="referenceLine">еталон завантажується…</p>
        </div>
        <div style="display:flex; gap:8px; align-items:center;">
            <span class="badge" id="sessionBadge">Session: init...</span>
            <span class="badge" id="workerBadge">workers</span>
        </div>
    </header>
    <div class="main-layout">
        <aside class="card">
            <div class="section-title">Параметри</div>
            <div class="form-row">
                <label>k (ліній у шоку): <span id="kVal">@@K@@</span></label>
                <input type="range" id="k" min="1" max="5" step="1" value="@@K@@">
            </div>
            <div class="form-row">
                <label>Випробувань на крок</label>
                <input type="number" id="trialsPerStep" min="1" max="100000" step="1" value="@@TRIALS@@">
            </div>
            <div class="form-row">
                <label>Поріг dns_proxy</label>
                <input type="number" id="threshold" min="0.01" max="1" step="0.01" value="@@THRESHOLD@@">
            </div>
            <div class="form-row">
                <label>Сценарій s</label>
                <input type="number" id="scenario" min="0" step="1" value="@@SCENARIO@@">
            </div>
            <div class="form-row">
                <label>Шок</label>
                <select id="shock">
                    <option value="weighted" @@SEL_WEIGHTED@@>weighted</option>
                    <option value="trunk" @@SEL_TRUNK@@>trunk</option>
                    <option value="uniform" @@SEL_UNIFORM@@>uniform</option>
                </select>
            </div>
            <p class="hint">Параметри застосовуються кнопкою «Скинути». Вона обнуляє лічильники цієї сесії.</p>
            <div class="btn-group">
                <button class="btn btn-primary" id="btnToggle" type="button">Запуск</button>
                <button class="btn" id="btnStep" type="button">1 крок</button>
            </div>
            <div class="btn-group">
                <button class="btn" id="btnReset" type="button">Скинути</button>
                <button class="btn" id="btnBatch" type="button">Batch</button>
            </div>
            <p id="infoLine"></p>
            <div class="section-title">Телеметрія</div>
            <div class="metrics-grid">
                <div class="metric-box">
                    <div class="metric-label">P(blackout)</div>
                    <div class="metric-value" id="pBlackout">—</div>
                </div>
                <div class="metric-box">
                    <div class="metric-label">Статус кадру</div>
                    <div class="metric-value" id="simStatus"><span class="status-pill status-READY">READY</span></div>
                </div>
                <div class="metric-box">
                    <div class="metric-label">Середній dns_proxy</div>
                    <div class="metric-value" id="meanDns">—</div>
                </div>
                <div class="metric-box">
                    <div class="metric-label">Частка каскадів</div>
                    <div class="metric-value" id="cascadeFrac">—</div>
                </div>
                <div class="metric-box">
                    <div class="metric-label">Випробувань</div>
                    <div class="metric-value" id="trialTotal">0</div>
                </div>
                <div class="metric-box">
                    <div class="metric-label">Час хвилі</div>
                    <div class="metric-value" id="calcTime">0 ms</div>
                </div>
            </div>
            <div class="section-title">Топ ліній сесії</div>
            <div id="topLines">ще немає шоку</div>
        </aside>
        <main class="canvas-wrap">
            <canvas id="gridCanvas" width="720" height="720"></canvas>
            <div class="hud" id="hud">кадр — | trial — | cascade_trips —</div>
            <div class="legend">
                <span><i class="swatch util"></i>активна, завантаження</span>
                <span><i class="swatch shock"></i>N-k шок</span>
                <span><i class="swatch cascade"></i>каскад</span>
                <span><i class="swatch dark"></i>знеструмлений споживач</span>
            </div>
            <p id="layoutNote">Розташування вузлів алгоритмічне, не географія.</p>
        </main>
    </div>
    <script>
        const canvas = document.getElementById("gridCanvas");
        const ctx = canvas.getContext("2d");
        let isRunning = false;
        let isCalculating = false;
        let graph = null;
        let display = null;
        let hoverText = "наведіть на лінію";
        let frameTick = 0;
        const sessionId = localStorage.getItem("sim_session") || ("sess_" + Math.random().toString(36).substring(2, 10));
        localStorage.setItem("sim_session", sessionId);
        document.getElementById("sessionBadge").textContent = "ID: " + sessionId;

        const kInput = document.getElementById("k");
        kInput.oninput = () => document.getElementById("kVal").textContent = kInput.value;

        const PLASMA = [[13, 8, 135], [126, 3, 168], [204, 71, 120], [248, 149, 64], [240, 249, 33]];

        function plasma(t) {
            const x = Math.min(1, Math.max(0, t)) * (PLASMA.length - 1);
            const i = Math.min(PLASMA.length - 2, Math.floor(x));
            const f = x - i;
            const c = PLASMA[i].map((v, k) => Math.round(v + (PLASMA[i + 1][k] - v) * f));
            return "rgb(" + c[0] + "," + c[1] + "," + c[2] + ")";
        }

        function lerp(a, b, t) {
            const c = a.map((v, i) => Math.round(v + (b[i] - v) * t));
            return "rgb(" + c[0] + "," + c[1] + "," + c[2] + ")";
        }

        async function postJson(endpoint, data) {
            const res = await fetch(endpoint, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-Simulation-Session": sessionId
                },
                body: JSON.stringify(data || {})
            });
            const body = await res.json();
            if (!res.ok) throw new Error(body.error || res.statusText);
            return body;
        }

        function nodeExtents(nodes) {
            let maxPos = 0;
            let maxNeg = 0;
            nodes.forEach((node) => {
                if (node.load > maxPos) maxPos = node.load;
                if (-node.load > maxNeg) maxNeg = -node.load;
            });
            return { maxPos: maxPos || 1, maxNeg: maxNeg || 1 };
        }

        function nodeColor(load, extents) {
            if (load < 0) return lerp([224, 242, 254], [3, 105, 161], Math.min(1, Math.abs(load) / extents.maxNeg));
            return lerp([254, 243, 199], [194, 65, 12], Math.min(1, load / extents.maxPos));
        }

        function pointOf(node) {
            const pad = 36;
            const w = canvas.width - pad * 2;
            const h = canvas.height - pad * 2;
            return [pad + node.x * w, pad + (1 - node.y) * h];
        }

        function edgeState(id) {
            if (!display) return "active";
            if (display.shock.indexOf(id) >= 0) return "shock";
            if (display.tripped.indexOf(id) >= 0) return "cascade";
            return "active";
        }

        function edgeColor(edge, state) {
            if (state === "shock") return "#f59e0b";
            if (state === "cascade") return "#ef4444";
            const util = edge.capacity > 0 ? edge.flow / edge.capacity : 1;
            return plasma(util);
        }

        function paint() {
            ctx.fillStyle = "#050811";
            ctx.fillRect(0, 0, canvas.width, canvas.height);
            if (!graph) {
                ctx.fillStyle = "#94a3b8";
                ctx.font = "16px sans-serif";
                ctx.fillText("Завантаження графа…", 40, 40);
                return;
            }
            const byId = new Map(graph.nodes.map((node) => [node.id, node]));
            const extents = nodeExtents(graph.nodes);
            const dark = new Set(display && display.dark_buses ? display.dark_buses : []);
            ["active", "shock", "cascade"].forEach((state) => {
                graph.edges.forEach((edge) => {
                    if (edgeState(edge.id) !== state) return;
                    const a = byId.get(edge.from);
                    const b = byId.get(edge.to);
                    if (!a || !b) return;
                    const p = pointOf(a);
                    const q = pointOf(b);
                    ctx.strokeStyle = edgeColor(edge, state);
                    ctx.lineWidth = state === "active" ? 1.3 : 2.6;
                    ctx.beginPath();
                    ctx.moveTo(p[0], p[1]);
                    ctx.lineTo(q[0], q[1]);
                    ctx.stroke();
                });
            });
            graph.nodes.forEach((node) => {
                const p = pointOf(node);
                ctx.beginPath();
                ctx.fillStyle = nodeColor(node.load, extents);
                ctx.arc(p[0], p[1], 5, 0, Math.PI * 2);
                ctx.fill();
                if (dark.has(node.id)) {
                    ctx.beginPath();
                    ctx.strokeStyle = "#64748b";
                    ctx.lineWidth = 2;
                    ctx.arc(p[0], p[1], 8, 0, Math.PI * 2);
                    ctx.stroke();
                }
            });
        }

        function distToSegment(px, py, x1, y1, x2, y2) {
            const dx = x2 - x1;
            const dy = y2 - y1;
            const len2 = dx * dx + dy * dy || 1;
            let t = ((px - x1) * dx + (py - y1) * dy) / len2;
            t = Math.max(0, Math.min(1, t));
            return Math.hypot(px - (x1 + t * dx), py - (y1 + t * dy));
        }

        function stateLabel(state) {
            if (state === "shock") return "N-k шок";
            if (state === "cascade") return "каскад";
            return "активна";
        }

        canvas.addEventListener("mousemove", (event) => {
            if (!graph) return;
            const rect = canvas.getBoundingClientRect();
            const x = (event.clientX - rect.left) * (canvas.width / rect.width);
            const y = (event.clientY - rect.top) * (canvas.height / rect.height);
            const byId = new Map(graph.nodes.map((node) => [node.id, node]));
            let best = null;
            let bestDist = 8;
            graph.edges.forEach((edge) => {
                const a = byId.get(edge.from);
                const b = byId.get(edge.to);
                if (!a || !b) return;
                const p = pointOf(a);
                const q = pointOf(b);
                const dist = distToSegment(x, y, p[0], p[1], q[0], q[1]);
                if (dist < bestDist) {
                    bestDist = dist;
                    best = edge;
                }
            });
            hoverText = best
                ? (best.from + "–" + best.to + " · " + stateLabel(edgeState(best.id)))
                : "наведіть на лінію";
            const hud = document.getElementById("hud");
            const trial = display ? display.trial_index : "—";
            const trips = display ? display.cascade_trips : "—";
            hud.textContent = "кадр " + frameTick + " | trial " + trial + " | cascade_trips " + trips + " | " + hoverText;
        });

        function applyMetrics(data) {
            graph = data;
            document.getElementById("scenario").value = data.scenario;
            document.getElementById("shock").value = data.shock_mode;
            document.getElementById("workerBadge").textContent = "workers " + data.workers;
            const ref = data.reference || {};
            document.getElementById("referenceLine").textContent =
                "s=" + data.scenario + "  dns_MW=" + ref.dns_MW + "  output_features=" + ref.output_features;
            document.getElementById("layoutNote").textContent =
                "Розташування вузлів — " + data.layout + ", не географія.";
            paint();
        }

        async function loadMetrics() {
            const res = await fetch("/api/metrics");
            const data = await res.json();
            applyMetrics(data);
        }

        function renderTop(lines) {
            const el = document.getElementById("topLines");
            el.replaceChildren();
            if (!lines || !lines.length) {
                el.textContent = "ще немає шоку";
                return;
            }
            lines.slice(0, 5).forEach((row) => {
                const div = document.createElement("div");
                div.textContent = row.branch + "  " + row.from_bus + "–" + row.to_bus
                    + "  P=" + Number(row.p_blackout_given_shock).toFixed(3);
                el.appendChild(div);
            });
        }

        function updateDashboard(data) {
            document.getElementById("pBlackout").textContent = Number(data.p_blackout).toFixed(5);
            document.getElementById("meanDns").textContent = Number(data.mean_dns_proxy).toFixed(5);
            document.getElementById("cascadeFrac").textContent = Number(data.cascade_fraction).toFixed(5);
            document.getElementById("trialTotal").textContent = Number(data.trials_total).toLocaleString();
            document.getElementById("calcTime").textContent = data.calc_time_ms + " ms";
            frameTick = data.tick;
            document.getElementById("simStatus").innerHTML =
                '<span class="status-pill status-' + data.status + '">' + data.status + "</span>";
            display = data.display;
            renderTop(data.top_lines);
            const hud = document.getElementById("hud");
            hud.textContent = "кадр " + data.tick + " | trial " + display.trial_index
                + " | cascade_trips " + display.cascade_trips + " | " + hoverText;
            paint();
        }

        function clearTelemetry() {
            display = null;
            document.getElementById("pBlackout").textContent = "—";
            document.getElementById("meanDns").textContent = "—";
            document.getElementById("cascadeFrac").textContent = "—";
            document.getElementById("trialTotal").textContent = "0";
            document.getElementById("calcTime").textContent = "0 ms";
            frameTick = 0;
            document.getElementById("simStatus").innerHTML = '<span class="status-pill status-READY">READY</span>';
            document.getElementById("topLines").textContent = "ще немає шоку";
            document.getElementById("hud").textContent = "кадр — | trial — | cascade_trips — | " + hoverText;
            paint();
        }

        async function executeStep() {
            if (isCalculating) return;
            isCalculating = true;
            try {
                updateDashboard(await postJson("/api/step", {}));
                document.getElementById("infoLine").textContent = "";
            } catch (err) {
                document.getElementById("infoLine").textContent = String(err.message || err);
                isRunning = false;
                document.getElementById("btnToggle").textContent = "Запуск";
                document.getElementById("btnToggle").classList.remove("btn-danger");
            } finally {
                isCalculating = false;
            }
        }

        async function resetSimulation() {
            if (isCalculating) return;
            isCalculating = true;
            try {
                await postJson("/api/reset", {
                    k: Number(kInput.value),
                    threshold: Number(document.getElementById("threshold").value),
                    trials_per_step: Number(document.getElementById("trialsPerStep").value),
                    scenario: Number(document.getElementById("scenario").value),
                    shock: document.getElementById("shock").value
                });
                clearTelemetry();
                await loadMetrics();
                document.getElementById("infoLine").textContent = "Параметри застосовано. Лічильники сесії скинуто.";
            } catch (err) {
                document.getElementById("infoLine").textContent = String(err.message || err);
            } finally {
                isCalculating = false;
            }
        }

        document.getElementById("btnToggle").onclick = () => {
            isRunning = !isRunning;
            document.getElementById("btnToggle").textContent = isRunning ? "Пауза" : "Запуск";
            document.getElementById("btnToggle").classList.toggle("btn-danger", isRunning);
        };
        document.getElementById("btnStep").onclick = () => executeStep();
        document.getElementById("btnReset").onclick = () => resetSimulation();
        document.getElementById("btnBatch").onclick = async () => {
            if (isCalculating) return;
            isCalculating = true;
            try {
                const data = await postJson("/api/batch", { steps: 10 });
                updateDashboard(data);
                document.getElementById("infoLine").textContent =
                    "Batch " + data.steps + ": " + data.total_time_ms + " ms, середній крок " + data.avg_step_ms + " ms";
            } catch (err) {
                document.getElementById("infoLine").textContent = String(err.message || err);
            } finally {
                isCalculating = false;
            }
        };

        function loop() {
            if (isRunning && !isCalculating) executeStep();
            requestAnimationFrame(loop);
        }
        requestAnimationFrame(loop);
        loadMetrics().catch((err) => {
            document.getElementById("infoLine").textContent = String(err.message || err);
        });
    </script>
</body>
</html>
"""


def render_page(cfg: Config) -> str:
    selected = {mode: ("selected" if mode == cfg.shock else "") for mode in SHOCK_MODES}
    page = HTML_PAGE
    page = page.replace("@@K@@", str(cfg.k))
    page = page.replace("@@TRIALS@@", str(cfg.trials_per_step))
    page = page.replace("@@THRESHOLD@@", f"{cfg.threshold:g}")
    page = page.replace("@@SCENARIO@@", str(cfg.scenario))
    page = page.replace("@@SEL_WEIGHTED@@", selected["weighted"])
    page = page.replace("@@SEL_TRUNK@@", selected["trunk"])
    page = page.replace("@@SEL_UNIFORM@@", selected["uniform"])
    return page


class GridHandler(BaseHTTPRequestHandler):
    def route(self) -> str:
        return self.path.split("?", 1)[0]

    def get_session_id(self) -> str:
        header_sid = self.headers.get("X-Simulation-Session")
        if header_sid:
            return header_sid
        cookie = self.headers.get("Cookie", "")
        for item in cookie.split(";"):
            parts = item.strip().split("=", 1)
            if len(parts) == 2 and parts[0] == "sim_session":
                return parts[1]
        return "default_session"

    def read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            params = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError:
            return {}
        if not isinstance(params, dict):
            return {}
        return params

    def write_json(self, code: int, payload: dict, cookie: str | None = None) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if cookie:
            self.send_header("Set-Cookie", f"sim_session={cookie}; Path=/; SameSite=Lax")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = self.route()
        if path in ("/", "/index.html"):
            sid = self.get_session_id()
            if sid == "default_session":
                sid = "sess_" + uuid.uuid4().hex[:10]
            body = render_page(CONFIG).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Set-Cookie", f"sim_session={sid}; Path=/; SameSite=Lax")
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/api/health":
            self.write_json(200, health_payload())
            return
        if path == "/api/metrics":
            self.write_json(200, metrics_payload())
            return
        self.write_json(404, {"error": "Not Found"})

    def do_POST(self) -> None:
        path = self.route()
        sid = self.get_session_id()
        params = self.read_json()
        try:
            if path == "/api/reset":
                self.handle_reset(sid, params)
            elif path == "/api/step":
                self.handle_step(sid)
            elif path == "/api/batch":
                self.handle_batch(sid, params)
            else:
                self.write_json(404, {"error": "Not Found"})
        except (ValueError, OSError) as exc:
            self.write_json(400, {"error": str(exc)})
        except Exception as exc:
            self.write_json(500, {"error": str(exc)})

    def handle_reset(self, sid: str, params: dict) -> None:
        session = SESSIONS.get(sid)
        new, loaded = parse_reset(params, session)
        with POOL_LOCK:
            if loaded is not None:
                commit_world(loaded)
            session.k = new["k"]
            session.seed = new["seed"]
            session.threshold = new["threshold"]
            session.trials_per_step = new["trials_per_step"]
            session.reset_counters()
        self.write_json(200, {"status": "ok", "session_id": sid, "reloaded": loaded is not None}, cookie=sid)

    def handle_step(self, sid: str) -> None:
        session = SESSIONS.get(sid)
        with POOL_LOCK:
            frame = session.run_wave()
        self.write_json(200, frame, cookie=sid)

    def handle_batch(self, sid: str, params: dict) -> None:
        steps = parse_steps(params)
        session = SESSIONS.get(sid)
        t0 = time.perf_counter()
        with POOL_LOCK:
            last = None
            for _ in range(steps):
                last = session.run_wave()
        total = time.perf_counter() - t0
        last["steps"] = steps
        last["total_time_ms"] = round(total * 1000, 1)
        last["avg_step_ms"] = round((total / steps) * 1000, 1)
        self.write_json(200, last, cookie=sid)

    def log_message(self, fmt, *args):
        if args and "/api/step" in str(args[0]):
            return
        super().log_message(fmt, *args)


def serve(cfg: Config) -> int:
    global CONFIG, SESSIONS
    from loader import load_once
    CONFIG = cfg
    SESSIONS = SessionManager({
        "k": cfg.k,
        "seed": cfg.seed,
        "threshold": cfg.threshold,
        "trials_per_step": cfg.trials_per_step,
    })
    try:
        loaded = load_once(cfg.data, cfg.scenario, cfg.unrated_margin, cfg.shock, cfg.script_dir)
        ensure_k(loaded["topo"], cfg.k)
    except Exception as exc:
        print(f"failed to load scenario: {exc}", file=sys.stderr)
        return 1
    loaded["data"] = cfg.data
    loaded["script_dir"] = cfg.script_dir
    try:
        server = HTTPServer(("0.0.0.0", cfg.port), GridHandler)
    except OSError as exc:
        print(f"failed to listen on port {cfg.port}: {exc}", file=sys.stderr)
        print(f"try: python app.py --port {cfg.port + 1}", file=sys.stderr)
        return 1

    with POOL_LOCK:
        commit_world(loaded)

    print("=" * 60)
    print(" IEEE-118 cascade web")
    print(f" URL:      http://localhost:{cfg.port}")
    print(f" Workers:  {cfg.workers}")
    print(f" Scenario: {cfg.scenario}")
    print(f" PID:      {os.getpid()}")
    print("=" * 60)

    def handle_signal(sig, _frame):
        print(f"\n[Signal Handler] Caught signal {sig} ({signal.Signals(sig).name}). Shutting down.")
        shutdown_shared_pool(terminate=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        shutdown_shared_pool()
    return 0


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        cfg = build_config(args)
    except ValueError as exc:
        parser.error(str(exc))
    if cfg.headless:
        return run_headless(cfg)
    return serve(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
