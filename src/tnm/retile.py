"""Retile processed DEM tiles to a new tiling scheme."""

from __future__ import annotations

import gc
import json
import os
import re
import shutil
import subprocess
import tempfile
from multiprocessing import get_context
from pathlib import Path
from typing import Iterable, Optional

import geopandas as gpd
import rasterio
from shapely import wkb
from shapely.geometry import mapping


DEFAULT_GDAL_CACHE_MB = 256
DEFAULT_GDAL_THREADS = "1"
COMMON_ID_FIELDS = ("tile_id", "id", "grid_id", "name")


def _safe_name(value: object) -> str:
    """Return a filesystem-safe name built from a value."""
    value_str = str(value)
    value_str = value_str.strip()
    if not value_str:
        return "unnamed"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value_str)


def _resolve_id_field(gdf: gpd.GeoDataFrame, id_field: Optional[str]) -> Optional[str]:
    if id_field is not None:
        if id_field not in gdf.columns:
            raise ValueError(f"id_field '{id_field}' not found in grids file")
        return id_field

    for candidate in COMMON_ID_FIELDS:
        if candidate in gdf.columns:
            return candidate

    return None


def _ensure_gdal_tools() -> None:
    """Fail fast if required GDAL CLI tools are missing."""
    missing = [tool for tool in ("gdalbuildvrt", "gdalwarp") if not shutil.which(tool)]
    if missing:
        missing_list = ", ".join(missing)
        raise RuntimeError(
            f"Missing GDAL tools: {missing_list}. Ensure GDAL is installed and on PATH."
        )


def build_vrt(
    input_folder: Path,
    vrt_path: Path,
    pattern: str = "*.tif",
    nodata: Optional[float] = None,
    resolution: Optional[str] = "average",
) -> Path:
    """Create a VRT that mosaics all tiles in the input folder."""
    input_folder = Path(input_folder)
    if not input_folder.exists():
        raise FileNotFoundError(f"Input folder does not exist: {input_folder}")

    tif_files = list(input_folder.glob(pattern))
    if not tif_files:
        raise FileNotFoundError(
            f"No files matching '{pattern}' found in {input_folder}"
        )

    vrt_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = ["gdalbuildvrt"]
    if resolution:
        cmd.extend(["-resolution", resolution])
    if nodata is not None:
        cmd.extend(["-srcnodata", str(nodata), "-vrtnodata", str(nodata)])

    cmd.append(str(vrt_path))
    cmd.extend([str(f) for f in tif_files])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"gdalbuildvrt failed: {result.stderr}")

    return vrt_path


def _write_cutline_geojson(geometry, output_path: Path) -> None:
    """Write a single-geometry GeoJSON file for GDAL cutline usage."""
    feature = {
        "type": "Feature",
        "properties": {"id": 1},
        "geometry": mapping(geometry),
    }
    payload = {"type": "FeatureCollection", "features": [feature]}
    with open(output_path, "w") as handle:
        json.dump(payload, handle)


def _retile_worker(args: tuple) -> dict:
    (
        vrt_path,
        output_path,
        geometry_wkb,
        cutline_srs,
        target_res,
        nodata_val,
        compress,
        resampling,
        overwrite,
        gdal_cache_mb,
        blocksize,
    ) = args

    result = {
        "output_path": str(output_path),
        "success": False,
        "message": "",
    }

    if not overwrite and os.path.exists(output_path):
        result["success"] = True
        result["message"] = "Skipped (exists)"
        return result

    geom = wkb.loads(geometry_wkb) if geometry_wkb else None
    if geom is None or geom.is_empty:
        result["message"] = "Skipped (empty geometry)"
        return result

    if not geom.is_valid:
        geom = geom.buffer(0)

    os.environ["GDAL_CACHEMAX"] = str(gdal_cache_mb)
    os.environ["GDAL_NUM_THREADS"] = DEFAULT_GDAL_THREADS

    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
            temp_file = Path(tmp.name)
        _write_cutline_geojson(geom, temp_file)

        cmd = [
            "gdalwarp",
            "-of",
            "COG",
            "-cutline",
            str(temp_file),
            "-cutline_srs",
            cutline_srs,
            "-crop_to_cutline",
            "-dstnodata",
            str(nodata_val),
            "-r",
            resampling,
            "-wo",
            "NUM_THREADS=1",
        ]

        if target_res is not None:
            cmd.extend(["-tr", str(target_res), str(target_res), "-tap"])

        if overwrite:
            cmd.append("-overwrite")

        if compress:
            cmd.extend(["-co", f"COMPRESS={compress}"])

        if blocksize:
            cmd.extend(["-co", f"BLOCKSIZE={blocksize}"])

        cmd.extend([str(vrt_path), str(output_path)])

        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            result["message"] = proc.stderr.strip() or "gdalwarp failed"
            return result

        result["success"] = True
        result["message"] = "Success"
        return result
    finally:
        if temp_file and temp_file.exists():
            try:
                temp_file.unlink()
            except OSError:
                pass
        gc.collect()


def _batch_iter(items: list, batch_size: int) -> Iterable[list]:
    for idx in range(0, len(items), batch_size):
        yield items[idx : idx + batch_size]


def retile_tiles(
    tiles_folder: Path,
    grids_file: Path,
    domain_file: Optional[Path],
    buffer_distance: float,
    output_folder: Path,
    target_res: Optional[float],
    nodata_val: float = -9999,
    compress: str = "LZW",
    resampling: str = "bilinear",
    blocksize: int = 256,
    n_workers: int = 4,
    batch_size: int = 50,
    pattern: str = "*.tif",
    vrt_path: Optional[Path] = None,
    overwrite: bool = False,
    id_field: Optional[str] = None,
    output_prefix: str = "",
    logs_folder: Optional[Path] = None,
) -> gpd.GeoDataFrame:
    """Retile processed DEM tiles into a new tiling scheme."""
    tiles_folder = Path(tiles_folder)
    grids_file = Path(grids_file)
    output_folder = Path(output_folder)
    domain_file = Path(domain_file) if domain_file else None

    if not grids_file.exists():
        raise FileNotFoundError(f"Grids file does not exist: {grids_file}")

    output_folder.mkdir(parents=True, exist_ok=True)

    _ensure_gdal_tools()

    if vrt_path is None:
        vrt_path = output_folder / "retile_source.vrt"

    if not vrt_path.exists():
        build_vrt(tiles_folder, vrt_path, pattern=pattern, nodata=nodata_val)

    gdf = gpd.read_parquet(grids_file)
    if gdf.empty:
        raise ValueError("Grids file contains no features")

    id_field = _resolve_id_field(gdf, id_field)

    with rasterio.open(vrt_path) as src:
        raster_crs = src.crs
        raster_crs_wkt = raster_crs.to_wkt() if raster_crs else None

    if raster_crs is None:
        raise ValueError("Source VRT has no CRS")

    if raster_crs_wkt is None:
        raise ValueError("Source VRT has invalid CRS")

    if gdf.crs is None:
        raise ValueError("Grids file has no CRS")

    if gdf.crs != raster_crs:
        gdf = gdf.to_crs(raster_crs)

    if domain_file is not None:
        if not domain_file.exists():
            raise FileNotFoundError(f"Domain file does not exist: {domain_file}")

        domain_gdf = gpd.read_file(domain_file)
        if domain_gdf.empty:
            raise ValueError("Domain file contains no features")
        if domain_gdf.crs is None:
            raise ValueError("Domain file has no CRS")

        if domain_gdf.crs != raster_crs:
            domain_gdf = domain_gdf.to_crs(raster_crs)

        domain_geom = domain_gdf.geometry.union_all()
        if buffer_distance:
            domain_geom = domain_geom.buffer(buffer_distance)

        domain_clip = gpd.GeoDataFrame(
            {"geometry": [domain_geom]},
            crs=raster_crs,
        )
        gdf = (
            gdf.sjoin(domain_clip, how="inner", predicate="intersects")
            .drop(columns=["index_right"])
            .reset_index(drop=True)
        )

        if gdf.empty:
            raise ValueError("No grid tiles intersect the domain")

    print(f"Retiling {len(gdf)} grid tiles")

    task_args = []
    for idx, row in gdf.iterrows():
        tile_id = row[id_field] if id_field else idx
        tile_name = _safe_name(tile_id)
        output_name = f"{output_prefix}{tile_name}.tif"
        output_path = output_folder / output_name
        geom_wkb = wkb.dumps(row.geometry) if row.geometry is not None else None
        task_args.append(
            (
                vrt_path,
                output_path,
                geom_wkb,
                raster_crs_wkt,
                target_res,
                nodata_val,
                compress,
                resampling,
                overwrite,
                DEFAULT_GDAL_CACHE_MB,
                blocksize,
            )
        )

    ctx = get_context("spawn")
    results = []

    for batch in _batch_iter(task_args, batch_size):
        with ctx.Pool(processes=n_workers, maxtasksperchild=1) as pool:
            batch_results = pool.map(_retile_worker, batch)
        results.extend(batch_results)
        gc.collect()

    results_df = gpd.GeoDataFrame(results)

    success_count = int(results_df["success"].sum())
    fail_count = len(results_df) - success_count
    print(f"Retile complete: {success_count} succeeded, {fail_count} failed")

    if logs_folder is not None:
        logs_folder = Path(logs_folder)
        logs_folder.mkdir(parents=True, exist_ok=True)
        logs_path = logs_folder / "retile_logs.csv"
        results_df.to_csv(logs_path, index=False)
        print(f"Wrote retile log: {logs_path}")

    if success_count == 0:
        raise RuntimeError("All retile tasks failed. Check the retile log for details.")

    return results_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Retile processed DEM tiles to a new tiling scheme."
    )
    parser.add_argument("--tiles-folder", required=True)
    parser.add_argument("--grids-file", required=True)
    parser.add_argument("--domain-file")
    parser.add_argument("--buffer-distance", type=float, default=0)
    parser.add_argument("--output-folder", required=True)
    parser.add_argument("--target-res", type=float, required=True)
    parser.add_argument("--nodata", type=float, default=-9999)
    parser.add_argument("--compress", default="LZW")
    parser.add_argument("--resampling", default="bilinear")
    parser.add_argument("--blocksize", type=int, default=256)
    parser.add_argument("--n-workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--pattern", default="*.tif")
    parser.add_argument("--vrt-path")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--id-field")
    parser.add_argument("--output-prefix", default="")
    parser.add_argument("--logs-folder")

    args = parser.parse_args()

    retile_tiles(
        tiles_folder=Path(args.tiles_folder),
        grids_file=Path(args.grids_file),
        domain_file=Path(args.domain_file) if args.domain_file else None,
        buffer_distance=args.buffer_distance,
        output_folder=Path(args.output_folder),
        target_res=args.target_res,
        nodata_val=args.nodata,
        compress=args.compress,
        resampling=args.resampling,
        blocksize=args.blocksize,
        n_workers=args.n_workers,
        batch_size=args.batch_size,
        pattern=args.pattern,
        vrt_path=Path(args.vrt_path) if args.vrt_path else None,
        overwrite=args.overwrite,
        id_field=args.id_field,
        output_prefix=args.output_prefix,
        logs_folder=Path(args.logs_folder) if args.logs_folder else None,
    )
