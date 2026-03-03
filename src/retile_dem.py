"""Runner script to retile processed DEM tiles using a config file."""

from __future__ import annotations

# custom imports
from tnm.retile import retile_tiles

import json
import os
import sys
from pathlib import Path

# mount the src directory
src_dir = os.path.dirname(__file__)
root_dir = os.path.abspath(os.path.join(src_dir, ".."))
config_dir = os.path.join(root_dir, "configs")
sys.path.append(src_dir)


def load_config(config_path: str) -> dict:
    """Load configuration from a JSON file."""
    with open(config_path, "r") as file:
        return json.load(file)


if __name__ == "__main__":
    config = load_config(os.path.join(config_dir, "retile_dem.json"))
    files = config["files"]
    params = config["params"]

    retile_tiles(
        tiles_folder=Path(files["tiles"]),
        grids_file=Path(files["retiled_grids"]),
        domain_file=Path(files["domain"]),
        buffer_distance=params.get("retile_buffer_distance", 0),
        output_folder=Path(files["retile"]),
        target_res=params.get("retile_target_res"),
        nodata_val=params.get("retile_nodata_val", params.get("nodata_val", -9999)),
        compress=params.get("retile_compress", "LZW"),
        resampling=params.get("retile_resampling", "bilinear"),
        blocksize=params.get("retile_blocksize", 256),
        n_workers=params.get("retile_n_workers", params.get("n_workers", 2)),
        batch_size=params.get("retile_batch_size", 25),
        pattern=params.get("retile_pattern", "*.tif"),
        overwrite=params.get("retile_overwrite", False),
        id_field=params.get("retile_id_field"),
        output_prefix=params.get("retile_output_prefix", ""),
        logs_folder=Path(files["logs"]),
    )
