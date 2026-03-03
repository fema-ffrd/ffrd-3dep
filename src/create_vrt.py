"""Runner script to create a VRT for retiled DEM outputs using a config file."""

from __future__ import annotations

# custom imports
from tnm.tnm import create_vrt

# standard imports
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

    create_vrt(
        domain_id=params["id"],
        input_folder=Path(files["retile"]),
        vrt_folder=Path(files["vrt"]),
        target_res=params.get("retile_target_res", params.get("target_res", 4)),
        pattern="*.tif",
        resolution="average",
        nodata=params.get("retile_nodata_val", params.get("nodata_val", -9999)),
        relative=True,
    )
