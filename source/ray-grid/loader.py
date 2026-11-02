"""Load one IEEE-118 scenario on the driver and hand the prepared topology to the engine.

Imported only from `run.py` commands `load` and `local`. `engine.py` does not
import this module, so a Pool or Ray worker never opens the .mat files.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

from engine import N_BRANCHES, N_BUSES, prepare_topology

GRID_DIR = Path(__file__).resolve().parent
LAB_DIR = GRID_DIR.parents[1]
SCRIPTS_DIR = LAB_DIR.parent / "cloud_grid" / "source" / "dataset_scripts"
DEFAULT_DATA = LAB_DIR / "dataset" / "dataset_cascades"
DEFAULT_SCENARIO = 83810


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


def load_once(data: Path, scenario: int, unrated_margin: float, shock_mode: str) -> dict:
    """Read the .mat scenario once and return a picklable topology plus the reference labels."""
    script = SCRIPTS_DIR / "load_powergraph_cascades.py"
    if not script.is_file():
        raise FileNotFoundError(f"Dataset loader not found: {script}")
    sys.path.insert(0, str(SCRIPTS_DIR))
    from load_powergraph_cascades import build_topology, load_powergraph_cascades

    pg = load_powergraph_cascades(data, sample_index=slice(scenario, scenario + 1))
    if pg.n_samples != 1:
        raise ValueError(f"Scenario {scenario} not found (loaded {pg.n_samples} samples)")
    raw = build_topology(pg.blist, pg.Ef[0], pg.Bf[0])
    offset = check_loaded(pg, raw)
    if offset:
        print("WARNING: bList is 0-based, bus ids shifted by +1")
    topo = prepare_topology(raw, offset, unrated_margin, shock_mode)
    p_net, p_ij, lr = pg.Bf[0][:, 0], pg.Ef[0][:, 0], pg.Ef[0][:, 3]
    return {
        "topo": topo,
        "scenario": scenario,
        "reference": {"dns_MW": float(pg.of_reg[0]), "output_features": float(pg.of_bi[0])},
        "summary": {
            "p_net_min": float(p_net.min()),
            "p_net_max": float(p_net.max()),
            "consumers": int((p_net < 0).sum()),
            "p_ij_min": float(p_ij.min()),
            "p_ij_max": float(p_ij.max()),
            "lr_min": float(lr.min()),
            "lr_max": float(lr.max()),
        },
    }
