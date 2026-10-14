#!/usr/bin/env python3
"""Stage 1: IEEE-118 cascade Monte Carlo on one PowerGraph scenario, local MIMD (multiprocessing.Pool).

    python app.py --trials 1000 --workers 1 --viz
    python app.py --headless --trials 10000 --k 1
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
from pathlib import Path

from engine import (
    DEFAULT_DATA,
    GRID_DIR,
    MAX_K,
    MAX_STEPS,
    N_BRANCHES,
    N_BUSES,
    SHOCK_MODES,
    GridSimulationEngine,
)

DEFAULT_OUT = GRID_DIR / "benchmark_results_mimd_grid.json"
PLOTS_DIR = GRID_DIR / "plots"


def worker_counts(max_workers: int) -> list[int]:
    counts, n = [], 1
    while n < max_workers:
        counts.append(n)
        n *= 2
    counts.append(max_workers)
    return counts


def _integer_stats(st: dict) -> tuple:
    return (st["trials"], st["blackouts"], st["cascade_trials"],
            tuple(st["in_shock"]), tuple(st["in_shock_blackout"]), tuple(st["tripped"]))


def speedup_path(out: Path) -> Path:
    if out.name == DEFAULT_OUT.name:
        return out.with_name("speedup.png")
    return out.with_name(f"{out.stem}_speedup.png")


def plot_speedup(runs: list[dict], out: Path) -> None:
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


def run_headless_benchmark(engine: GridSimulationEngine, max_workers: int, out: Path) -> dict:
    runs = []
    reference_stats = None
    deterministic = True
    for n in worker_counts(max_workers):
        engine.run(n)
        if reference_stats is None:
            reference_stats = _integer_stats(engine.stats)
        elif _integer_stats(engine.stats) != reference_stats:
            deterministic = False
        runs.append({
            "workers": n,
            "time_s": engine.elapsed,
            "trials_per_sec": engine.trials / engine.elapsed,
        })
        print(f"workers={n:<3} time={engine.elapsed:8.3f}s  {engine.trials / engine.elapsed:10.1f} trials/s")

    t1 = runs[0]["time_s"]
    for r in runs:
        n = r["workers"]
        r["speedup"] = t1 / r["time_s"]
        r["efficiency"] = r["speedup"] / n
        r["amdahl_serial_fraction"] = (n / r["speedup"] - 1) / (n - 1) if n > 1 else None

    topo = engine.topo
    result = {
        "params": {
            "trials": engine.trials,
            "k": engine.k,
            "seed": engine.seed,
            "threshold": engine.threshold,
            "shock_mode": topo["shock_mode"],
            "unrated_margin": topo["unrated_margin"],
            "cpu_count": os.cpu_count(),
            "max_steps": MAX_STEPS,
            "trip_rule": "one worst overloaded line per step",
            "shock_candidates": len(topo["shock_candidates"]),
            "n_unrated": topo["n_unrated"],
            "n_overloaded_initial": topo["n_overloaded_initial"],
            "topology_pickle_bytes": len(pickle.dumps(topo)),
            "time_includes_pool_startup": True,
        },
        "deterministic_across_workers": deterministic,
        "runs": runs,
        "result": engine.get_snapshot(),
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Saved: {out.resolve()}")

    png = speedup_path(out)
    plot_speedup(runs, png)
    print(f"Saved: {png.resolve()}")
    return result


def save_data_plots(pg, s: int, out_dir: Path) -> None:
    from visualize_grid_graph import plot_graph
    from visualize_powergraph import plot_branch_flow_vs_capacity, plot_node_load

    out_dir.mkdir(parents=True, exist_ok=True)
    paths = [
        out_dir / f"branch_flow_vs_capacity_s{s}.png",
        out_dir / f"node_loads_s{s}.png",
        out_dir / f"ieee118_s{s}.png",
    ]
    plot_branch_flow_vs_capacity(pg, 0, paths[0])
    plot_node_load(pg, 0, paths[1])
    plot_graph(pg.blist, bus_features=pg.Bf[0], branch_features=pg.Ef[0],
               title=f"IEEE-118 (scenario s={s})", out=paths[2])
    for p in paths:
        print(f"Saved: {p.resolve()}")


def print_scenario(engine: GridSimulationEngine) -> None:
    pg, topo = engine.pg, engine.topo
    p_net, p_ij, lr = pg.Bf[0][:, 0], pg.Ef[0][:, 0], pg.Ef[0][:, 3]
    margin = topo["unrated_margin"]
    rule = "max(1.0, 2|P|)" if margin == 0 else f"{margin}*|P|"
    print(f"scenario s={engine.scenario}  buses={N_BUSES} branches={N_BRANCHES}")
    print(f"reference: dns_MW={engine.reference['dns_MW']:.4g}  "
          f"output_features={engine.reference['output_features']:.0f}")
    print(f"P_net: min={p_net.min():.4g} max={p_net.max():.4g}  consumers={int((p_net < 0).sum())}  "
          f"total_consumption={topo['total_consumption']:.4g}")
    print(f"P_ij:  min={p_ij.min():.4g} max={p_ij.max():.4g}")
    print(f"lr:    min={lr.min():.4g} max={lr.max():.4g}  lr<=0: {topo['n_unrated']} -> capacity {rule}")
    print(f"overloaded before shock: {topo['n_overloaded_initial']}")
    print(f"shock={topo['shock_mode']}: {len(topo['shock_candidates'])} candidate lines")


def print_snapshot(snap: dict, elapsed: float) -> None:
    print(f"\ntrials={snap['trials']} k={snap['k']} shock={snap['shock_mode']} time={elapsed:.3f}s")
    print(f"P(blackout)      = {snap['p_blackout']:.5f}")
    print(f"mean dns_proxy   = {snap['mean_dns_proxy']:.5f}")
    print(f"cascade fraction = {snap['cascade_fraction']:.5f}")
    print("\nbranch  from-to    in_shock  expected  P(blackout|shock)  cascade_trips")
    for r in snap["top_lines"]:
        expected = r["shock_share_expected"] * snap["trials"] * snap["k"]
        print(f"{r['branch']:>6}  {r['from_bus']:>3}-{r['to_bus']:<3}  {r['in_shock']:>9}  {expected:>8.1f}  "
              f"{r['p_blackout_given_shock']:>17.4f}  {r['cascade_trips']:>13}")


def main() -> int:
    parser = argparse.ArgumentParser(description="IEEE-118 cascade Monte Carlo, local MIMD")
    parser.add_argument("--headless", action="store_true", help="Benchmark over workers 1, 2, 4, ... --workers")
    parser.add_argument("--trials", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--k", type=int, default=1)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA,
                        help="Parent directory of the inner dataset_cascades folder")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--scenario", type=int, default=None, help="Scenario index s (default: random from --seed)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--threshold", type=float, default=0.05)
    parser.add_argument("--shock", choices=SHOCK_MODES, default="weighted")
    parser.add_argument("--unrated-margin", type=float, default=2.0,
                        help="Capacity of lines with lr <= 0 = margin * |P_ij|; 0 = n01 rule max(1.0, 2|P_ij|)")
    parser.add_argument("--viz", action="store_true", help="Save data plots for scenario s to plots/")
    args = parser.parse_args()

    if args.trials < 1:
        parser.error("--trials must be >= 1")
    if args.workers < 1:
        parser.error("--workers must be >= 1")
    if not (args.unrated_margin == 0 or args.unrated_margin >= 1):
        parser.error("--unrated-margin must be 0 or >= 1 (below 1 lines start overloaded)")

    engine = GridSimulationEngine()
    engine.load_scenario(args.data, args.scenario, args.seed, args.unrated_margin, args.shock)

    max_k = min(MAX_K, len(engine.topo["flow"]) // 10)
    if not 1 <= args.k <= max_k:
        parser.error(f"--k must be in 1..{max_k}")

    print_scenario(engine)
    if args.viz:
        save_data_plots(engine.pg, engine.scenario, PLOTS_DIR)

    engine.reset_params(trials=args.trials, k=args.k, seed=args.seed, threshold=args.threshold)

    if args.headless:
        result = run_headless_benchmark(engine, args.workers, args.out)
        print_snapshot(result["result"], engine.elapsed)
        if not result["deterministic_across_workers"]:
            print("WARNING: results differ between worker counts")
    else:
        engine.run(args.workers)
        print_snapshot(engine.get_snapshot(), engine.elapsed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
