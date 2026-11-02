#!/usr/bin/env python3
"""Ray Grid driver for the IEEE-118 cascade Monte Carlo.

Cluster (1 head + 1 worker), from this directory on both machines:

  python run.py head
  python run.py client --head <HEAD_IP>
  python run.py status
  python run.py load --trials 10000 --wait-nodes 2 --backend ray
  python run.py load --trials 10000 --compare --wait-nodes 2
  python run.py stop

`loader` is imported only inside `load` / `local`. A Windows spawn worker
re-imports this file and must not open the dataset.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_PORT = 6379
STAGE1_JSON = SCRIPT_DIR.parent / "grid-mimd" / "benchmark_results_mimd_grid.json"
SMOKE_JSON = SCRIPT_DIR / "benchmark_results_local_smoke.json"
RESULT_JSON = SCRIPT_DIR / "benchmark_results_ray_grid.json"
RUNTIME_EXCLUDES = ["__pycache__", "*.pyc", "*.json", "REPORT.md", "status.log"]
_RAY_OK = False


def _ensure_utf8_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8")
            except Exception:
                pass


def detect_lan_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"


def run_cmd(cmd, check=True):
    print("$", " ".join(cmd))
    return subprocess.run(cmd, check=check)


def _is_loopback(host: str) -> bool:
    return host in ("127.0.0.1", "localhost", "::1")


def ray_supported() -> bool:
    """Import ray in a child process so a missing AVX set (SIGILL) does not kill this one."""
    if importlib.util.find_spec("ray") is None:
        return False
    try:
        res = subprocess.run(
            [sys.executable, "-c", "import ray"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=20,
        )
        return res.returncode == 0
    except (subprocess.SubprocessError, OSError):
        return False


def require_ray() -> None:
    global _RAY_OK
    if _RAY_OK:
        return
    if ray_supported():
        _RAY_OK = True
        return
    print(
        "ПОМИЛКА: Ray недоступний на цій системі.\n"
        "         Можлива причина: процесор без AVX, або пакет ray не встановлено.\n"
        "         'ray start' на такому CPU падає з 'Помилкова інструкція' (SIGILL).\n"
        "         Для pool і numpy: python run.py local --skip-ray",
        file=sys.stderr,
    )
    raise SystemExit(1)


def runtime_env() -> dict:
    return {"working_dir": str(SCRIPT_DIR), "excludes": RUNTIME_EXCLUDES}


def connect_ray():
    """Attach to the local `ray start` node. Retry: GCS may still be coming up after `ray start`."""
    import ray

    if ray.is_initialized():
        return ray
    last: Exception | None = None
    for attempt in range(10):
        try:
            ray.init(address="auto", ignore_reinit_error=True, runtime_env=runtime_env())
            return ray
        except (ConnectionError, OSError) as exc:
            last = exc
            if attempt == 9:
                break
            time.sleep(1)
    assert last is not None
    raise last


def _reject(msg: str) -> int:
    print(msg, file=sys.stderr)
    return 2


def _check_common(args) -> int | None:
    if args.trials < 1:
        return _reject("--trials must be >= 1")
    if args.workers is not None and args.workers < 1:
        return _reject("--workers must be >= 1")
    if not (args.unrated_margin == 0 or args.unrated_margin >= 1):
        return _reject("--unrated-margin must be 0 or >= 1 (below 1 lines start overloaded)")
    return None


def _records(payload: dict) -> list[dict]:
    runs = payload.get("runs", [])
    if isinstance(runs, dict):
        return [runs]
    return [rec for rec in runs if isinstance(rec, dict)]


def print_loaded(loaded: dict, args) -> None:
    topo = loaded["topo"]
    summary = loaded["summary"]
    margin = topo["unrated_margin"]
    rule = "max(1.0, 2|P|)" if margin == 0 else f"{margin}*|P|"
    ref = loaded["reference"]
    print(f"scenario s={loaded['scenario']}  buses=118 branches=186")
    print(f"reference: dns_MW={ref['dns_MW']:.4g}  output_features={ref['output_features']:.0f}")
    print(
        f"P_net: min={summary['p_net_min']:.4g} max={summary['p_net_max']:.4g}  "
        f"consumers={summary['consumers']}  total_consumption={topo['total_consumption']:.4g}"
    )
    print(f"P_ij:  min={summary['p_ij_min']:.4g} max={summary['p_ij_max']:.4g}")
    print(f"lr:    min={summary['lr_min']:.4g} max={summary['lr_max']:.4g}  "
          f"lr<=0: {topo['n_unrated']} -> capacity {rule}")
    print(f"overloaded before shock: {topo['n_overloaded_initial']}")
    print(f"shock={topo['shock_mode']}: {len(topo['shock_candidates'])} candidate lines")
    print(f"trials={args.trials} k={args.k} seed={args.seed} threshold={args.threshold}")


def print_snapshot(snap: dict) -> None:
    print(f"\ntrials={snap['trials']} k={snap['k']} shock={snap['shock_mode']}")
    print(f"P(blackout)      = {snap['p_blackout']:.5f}")
    print(f"mean dns_proxy   = {snap['mean_dns_proxy']:.5f}")
    print(f"cascade fraction = {snap['cascade_fraction']:.5f}")
    print("\nbranch  from-to    in_shock  expected  P(blackout|shock)  cascade_trips")
    for row in snap["top_lines"]:
        expected = row["shock_share_expected"] * snap["trials"] * snap["k"]
        print(
            f"{row['branch']:>6}  {row['from_bus']:>3}-{row['to_bus']:<3}  {row['in_shock']:>9}  "
            f"{expected:>8.1f}  {row['p_blackout_given_shock']:>17.4f}  {row['cascade_trips']:>13}"
        )


def _same_workload(record: dict, stage1: dict) -> bool:
    params = stage1.get("params", {})
    result = stage1.get("result", {})
    return (
        params.get("trials") == record["trials"]
        and params.get("k") == record["k"]
        and params.get("seed") == record["seed"]
        and params.get("threshold") == record["threshold"]
        and params.get("shock_mode") == record["shock_mode"]
        and params.get("unrated_margin") == record["unrated_margin"]
        and result.get("scenario") == record["scenario"]
    )


def check_stage1(record: dict) -> bool:
    """Compare published stage-1 counters when the workload is the same N, k, seed, scenario."""
    if record["backend"] != "pool":
        return True
    if not STAGE1_JSON.is_file():
        print(f"Stage 1 JSON not found, kernel check skipped: {STAGE1_JSON}")
        return True
    stage1 = json.loads(STAGE1_JSON.read_text(encoding="utf-8"))
    if not _same_workload(record, stage1):
        print("Stage 1 JSON is a different workload, kernel check skipped")
        return True

    got = record["result"]
    ref = stage1["result"]
    mismatches = []
    if got["p_blackout"] != ref["p_blackout"]:
        mismatches.append(f"p_blackout {got['p_blackout']} != {ref['p_blackout']}")
    if got["cascade_fraction"] != ref["cascade_fraction"]:
        mismatches.append(f"cascade_fraction {got['cascade_fraction']} != {ref['cascade_fraction']}")
    if record["chunks"] == 1 and abs(got["mean_dns_proxy"] - ref["mean_dns_proxy"]) > 1e-12:
        mismatches.append(
            f"mean_dns_proxy {got['mean_dns_proxy']!r} != {ref['mean_dns_proxy']!r}"
        )
    for a, b in zip(got["top_lines"], ref["top_lines"]):
        if (a["branch"], a["in_shock"], a["cascade_trips"]) != (b["branch"], b["in_shock"], b["cascade_trips"]):
            mismatches.append(
                f"top line {a['branch']}: in_shock {a['in_shock']} trips {a['cascade_trips']} "
                f"!= branch {b['branch']} in_shock {b['in_shock']} trips {b['cascade_trips']}"
            )
            break
    if len(got["top_lines"]) != len(ref["top_lines"]):
        mismatches.append(f"top_lines {len(got['top_lines'])} != {len(ref['top_lines'])}")
    if mismatches:
        print("WARNING: pool result differs from stage 1:", file=sys.stderr)
        for line in mismatches:
            print(f"  {line}", file=sys.stderr)
        return False
    print(f"Kernel check vs {STAGE1_JSON.name}: OK")
    return True


def execute(args, backends: tuple[str, ...], out: Path) -> int:
    from engine import MAX_K, get_cluster_summary, run_benchmark
    from loader import DEFAULT_DATA, load_once

    if "ray" in backends:
        require_ray()
        connect_ray()
        cluster = get_cluster_summary()
        print(f"Ray cluster: {cluster['nodes']} node(s), {cluster['cpus']} CPU(s)")
        for node in cluster["node_details"]:
            print(f"  - {node['ip']}: {node['cpus']} CPU (id {node['node_id']})")

    loaded = load_once(args.data or DEFAULT_DATA, args.scenario, args.unrated_margin, args.shock)
    print_loaded(loaded, args)
    max_k = min(MAX_K, len(loaded["topo"]["flow"]) // 10)
    if not 1 <= args.k <= max_k:
        return _reject(f"--k must be in 1..{max_k}")

    payload = run_benchmark(
        loaded,
        trials=args.trials,
        k=args.k,
        seed=args.seed,
        threshold=args.threshold,
        workers=args.workers,
        backends=backends,
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Saved: {out.resolve()}")

    records = _records(payload)
    ok = True
    if payload["deterministic_across_backends"] is False:
        print("WARNING: integer counters differ between backends", file=sys.stderr)
        ok = False
    elif payload["deterministic_across_backends"]:
        print("Integer counters match across backends")
    dns_values = [rec["result"]["mean_dns_proxy"] for rec in records]
    if len(dns_values) > 1 and max(dns_values) - min(dns_values) > 1e-9:
        print("WARNING: mean_dns_proxy differs between backends", file=sys.stderr)
        ok = False
    for record in records:
        if not check_stage1(record):
            ok = False
    print_snapshot(records[-1]["result"])
    return 0 if ok else 1


def cmd_head(args) -> int:
    require_ray()
    lan_ip = args.node_ip or detect_lan_ip()
    cmd = [
        "ray", "start", "--head", "--disable-usage-stats",
        f"--port={args.port}", f"--node-ip-address={lan_ip}",
    ]
    if args.num_cpus:
        cmd.append(f"--num-cpus={args.num_cpus}")
    run_cmd(cmd)
    print()
    print("=" * 60)
    print(f"Ray HEAD ready at {lan_ip}:{args.port}")
    print("On the worker machine:")
    print(f"  python run.py client --head {lan_ip} --port {args.port}")
    print("Then on this machine:")
    print("  python run.py status")
    print("  python run.py load --trials 10000 --wait-nodes 2 --backend ray")
    print("=" * 60)
    return 0


def cmd_client(args) -> int:
    require_ray()
    local_ip = detect_lan_ip()
    if _is_loopback(args.head):
        print(
            "ERROR: --head 127.0.0.1 / localhost is not a separate worker.\n"
            f"       On the other PC run: python run.py client --head {local_ip} --port {args.port}",
            file=sys.stderr,
        )
        return 1
    if args.head == local_ip and not args.force:
        print(
            "ERROR: --head is THIS machine's LAN IP — client must run on the other PC.\n"
            f"       You are on {local_ip}; head is already here.\n"
            "       Same-machine second node (not the lab run): add --force",
            file=sys.stderr,
        )
        return 1
    if args.head == local_ip and args.force:
        print("WARNING: --force — second Ray node on the SAME machine.", file=sys.stderr)

    cmd = ["ray", "start", f"--address={args.head}:{args.port}", "--disable-usage-stats"]
    if args.num_cpus:
        cmd.append(f"--num-cpus={args.num_cpus}")
    run_cmd(cmd)
    print()
    print("=" * 60)
    print(f"Joined Ray cluster at {args.head}:{args.port}")
    print("Worker is running in the background. Keep this machine online.")
    print("To leave the cluster: python run.py stop")
    print("=" * 60)
    if args.watch:
        print("Watching cluster status (Ctrl+C stops the watch only)...")
        try:
            while True:
                subprocess.run(["ray", "status"], check=False)
                time.sleep(args.watch_interval)
        except KeyboardInterrupt:
            print("Stopped status watch.")
    return 0


def cmd_status(_args) -> int:
    run_cmd(["ray", "status"], check=False)
    return 0


def cmd_stop(_args) -> int:
    run_cmd(["ray", "stop"])
    return 0


def wait_for_cluster_nodes(min_nodes: int, timeout_sec: int, head_ip: str) -> None:
    import ray

    workers_needed = max(0, min_nodes - 1)
    deadline = time.time() + timeout_sec
    print()
    print(f"Очікую {min_nodes} вузл(и) (head + {workers_needed} worker).")
    if workers_needed:
        print(f"На іншій машині: python run.py client --head {head_ip}")
        print(f"Load стартує після {min_nodes}/{min_nodes}. Один ПК: без --wait-nodes, або python run.py local")
        print()
    while time.time() < deadline:
        alive = [n for n in ray.nodes() if n.get("Alive")]
        if len(alive) >= min_nodes:
            cpus = int(ray.cluster_resources().get("CPU", 0))
            print(f"Cluster ready: {len(alive)} node(s), {cpus} CPU(s)")
            return
        print(f"Waiting for nodes... {len(alive)}/{min_nodes} (Ctrl+C — скасувати)")
        time.sleep(2)
    alive_n = len([n for n in ray.nodes() if n.get("Alive")])
    raise TimeoutError(
        f"Only {alive_n} node(s) after {timeout_sec}s; expected at least {min_nodes}."
    )


def _local_backends(args) -> tuple[str, ...] | int:
    if args.backend and args.skip_ray and args.backend == "ray":
        return _reject("--backend ray conflicts with --skip-ray")
    if args.backend:
        return (args.backend,)
    names = ["pool", "numpy"]
    if not args.skip_ray:
        names.append("ray")
    return tuple(names)


def cmd_local(args) -> int:
    failed = _check_common(args)
    if failed:
        return failed
    backends = _local_backends(args)
    if isinstance(backends, int):
        return backends
    if "ray" in backends:
        require_ray()
        print("--- Starting Ray HEAD (single node, no worker required) ---")
        lan_ip = detect_lan_ip()
        run_cmd(
            [
                "ray", "start", "--head", "--disable-usage-stats",
                f"--port={DEFAULT_PORT}", f"--node-ip-address={lan_ip}",
            ],
            check=False,
        )
    return execute(args, backends, args.out)


def cmd_load(args) -> int:
    failed = _check_common(args)
    if failed:
        return failed
    backends = ("pool", "numpy", "ray") if args.compare else (args.backend,)
    if "ray" in backends or args.wait_nodes > 0:
        require_ray()
        connect_ray()
    if args.wait_nodes > 0:
        wait_for_cluster_nodes(args.wait_nodes, args.wait_timeout, detect_lan_ip())
    return execute(args, backends, args.out)


def _stage1_row(data: dict) -> dict:
    runs = data.get("runs") or []
    one = next((r for r in runs if r.get("workers") == 1), runs[0] if runs else {})
    result = data.get("result") or {}
    params = data.get("params") or {}
    return {
        "trials": params.get("trials", result.get("trials")),
        "time_s": one.get("time_s"),
        "throughput": one.get("trials_per_sec"),
        "p_blackout": result.get("p_blackout"),
    }


def _ray_row(data: dict) -> dict:
    runs = data.get("runs", data)
    record = {}
    if isinstance(runs, list):
        record = next((rec for rec in runs if isinstance(rec, dict) and rec.get("backend") == "ray"), {})
        if not record and runs:
            record = runs[-1] if isinstance(runs[-1], dict) else {}
    elif isinstance(runs, dict):
        record = runs
    metrics = record.get("metrics") or {}
    result = record.get("result") or {}
    return {
        "trials": record.get("trials"),
        "time_s": metrics.get("total_execution_time_sec"),
        "throughput": metrics.get("throughput_trials_per_sec"),
        "p_blackout": result.get("p_blackout"),
        "nodes": (record.get("cluster") or {}).get("nodes"),
        "cpus": (record.get("cluster") or {}).get("cpus"),
        "record": record,
    }


def cmd_report(args) -> int:
    baseline_path = Path(args.baseline)
    candidate_path = Path(args.candidate)
    missing = [str(p) for p in (baseline_path, candidate_path) if not p.is_file()]
    if missing:
        print("JSON not found:\n  " + "\n  ".join(missing), file=sys.stderr)
        return 1
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
    a = _stage1_row(baseline)
    b = _ray_row(candidate)

    def cell(value) -> str:
        if isinstance(value, float):
            return f"{value:.4g}"
        return "—" if value is None else str(value)

    print("=" * 72)
    print(" Stage 1 (workers=1)  vs  Ray Grid")
    print("=" * 72)
    print(f"{'':28} {'stage 1':>18} {'ray':>18}")
    print("-" * 72)
    for title, key in (
        ("Trials", "trials"),
        ("Time, s", "time_s"),
        ("Trials/s", "throughput"),
        ("P(blackout)", "p_blackout"),
    ):
        print(f"{title:28} {cell(a[key]):>18} {cell(b[key]):>18}")
    print("-" * 72)
    print(f"Ray cluster: nodes={cell(b['nodes'])}  cpus={cell(b['cpus'])}")
    if a["trials"] != b["trials"]:
        print("NOTE: trial counts differ — compare trials/s, not raw wall time.")
    print("Stage 1 JSON also has a worker sweep; this row is workers=1 only.")

    runs = candidate.get("runs")
    if isinstance(runs, list):
        print()
        print(f"{'backend':<10} {'where':<16} {'time_s':>12} {'trials/s':>14}")
        for rec in runs:
            where = "head + worker" if rec.get("backend") == "ray" else "head only"
            metrics = rec.get("metrics") or {}
            print(
                f"{rec.get('backend', '?'):<10} {where:<16} "
                f"{metrics.get('total_execution_time_sec', 0):>12} "
                f"{metrics.get('throughput_trials_per_sec', 0):>14}"
            )
    print("=" * 72)
    return 0


def _add_workload_args(parser: argparse.ArgumentParser, out_default: Path) -> None:
    parser.add_argument("--trials", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=None, help="Pool size, or Ray slots (default: auto)")
    parser.add_argument("--k", type=int, default=1)
    parser.add_argument("--data", type=Path, default=None, help="Parent of the inner dataset_cascades folder")
    parser.add_argument("--scenario", type=int, default=83810)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--threshold", type=float, default=0.05)
    parser.add_argument("--shock", choices=("weighted", "trunk", "uniform"), default="weighted")
    parser.add_argument("--unrated-margin", type=float, default=2.0, dest="unrated_margin",
                        help="Capacity of lr <= 0 lines; 0 = n01 rule max(1.0, 2|P|)")
    parser.add_argument("--out", type=Path, default=out_default)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="IEEE-118 cascade Monte Carlo on a Ray cluster")
    sub = parser.add_subparsers(dest="command", required=True)

    head = sub.add_parser("head", help="Start the Ray head node on this machine")
    head.add_argument("--port", type=int, default=DEFAULT_PORT)
    head.add_argument("--node-ip", type=str, default=None, help="LAN IP advertised to the worker")
    head.add_argument("--num-cpus", type=int, default=None)
    head.set_defaults(func=cmd_head)

    client = sub.add_parser("client", help="Join the cluster as a worker")
    client.add_argument("--head", required=True, help="Head node LAN IP")
    client.add_argument("--port", type=int, default=DEFAULT_PORT)
    client.add_argument("--num-cpus", type=int, default=None)
    client.add_argument("--watch", action="store_true")
    client.add_argument("--watch-interval", type=float, default=5.0)
    client.add_argument("--force", action="store_true", help="Allow a second node on the head machine")
    client.set_defaults(func=cmd_client)

    status = sub.add_parser("status", help="Show ray status")
    status.set_defaults(func=cmd_status)
    stop = sub.add_parser("stop", help="Stop the local Ray node")
    stop.set_defaults(func=cmd_stop)

    local = sub.add_parser("local", help="Smoke test on this machine: pool, numpy, then ray")
    local.add_argument("--backend", choices=("pool", "numpy", "ray"), default=None)
    local.add_argument("--skip-ray", action="store_true")
    _add_workload_args(local, SMOKE_JSON)
    local.set_defaults(func=cmd_local)

    load = sub.add_parser("load", help="Run the benchmark (from the head machine)")
    load.add_argument("--backend", choices=("pool", "numpy", "ray"), default="ray")
    load.add_argument("--compare", action="store_true", help="Run pool, numpy, and ray")
    load.add_argument("--wait-nodes", type=int, default=0)
    load.add_argument("--wait-timeout", type=int, default=180)
    _add_workload_args(load, RESULT_JSON)
    load.set_defaults(func=cmd_load)

    report = sub.add_parser("report", help="Print stage 1 (workers=1) next to the ray run")
    report.add_argument("--baseline", type=str, default=str(STAGE1_JSON))
    report.add_argument("--candidate", type=str, default=str(RESULT_JSON))
    report.set_defaults(func=cmd_report)
    return parser


def main() -> int:
    _ensure_utf8_stdio()
    args = build_parser().parse_args()
    try:
        return args.func(args)
    except subprocess.CalledProcessError as exc:
        print(f"Command failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode or 1
    except TimeoutError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except ConnectionError as exc:
        print(str(exc), file=sys.stderr)
        print("Спочатку на head: python run.py head", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
