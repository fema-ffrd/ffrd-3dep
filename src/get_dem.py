"""Runner script to acquire DEM tiles from the National Map."""

from __future__ import annotations

# custom imports
from tnm.tnm import (
    load_txt_file,
    main_get_aws_paths,
    main_get_dem_tiles,
)

# standard imports
import os
import sys
import json

# mount the src directory
src_dir = os.path.dirname(__file__)
root_dir = os.path.abspath(os.path.join(src_dir, ".."))
config_dir = os.path.join(root_dir, "configs")
sys.path.append(src_dir)


def load_config(config_path):
    """Load configuration from a JSON file."""
    with open(config_path, "r") as file:
        return json.load(file)


if __name__ == "__main__":
    config = load_config(os.path.join(config_dir, "get_dem.json"))
    files = config["files"]
    params = config["params"]
    # 1. Get AWS paths for a given area of interest
    aws_paths = main_get_aws_paths(
        dataset=params["dataset"],
        grids_file=files["grids"],
        domain_file=files["domain"],
        aws_output_folder=files["aws"],
        buffer_distance=params["buffer_distance"],
        max_retries=params["max_retries"],
        retry_delay=params["retry_delay"],
    )

    # 2. Acquire DEM tiles using the aws paths
    aws_file = os.path.join(files["aws"], "aws_paths.txt")
    aws_paths = load_txt_file(aws_file)
    df_results = main_get_dem_tiles(
        paths=aws_paths,
        tiles_folder=files["tiles"],
        logs_folder=files["logs"],
        domain_file=files["domain"],
        target_res=params["target_res"],
        nodata_val=params["nodata_val"],
        buffer_distance=params["buffer_distance"],
        n_workers=params["n_workers"],
        batch_size=params["chunk_size"],
    )
