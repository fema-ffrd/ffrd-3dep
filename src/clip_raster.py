"""
clip_raster.py

For each polygon within a vector file, clip the provided raster to the polygon's extent.
Save the clipped raster as a Cloud Optimized GeoTIFF (COG) with the following naming convention: dem-<ModelName>-<Huc4Name>-<CellSize>.tif

"""

import gc
import os

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.windows import Window
from shapely import unary_union
from tqdm import tqdm


def clip_raster_to_polygons(
    raster: str,
    vector: str,
    column: str,
    cell_size: int,
    output: str,
    buffer: int = 0,
    raster_units: str = "feet",
    cache_limit: int = 512,
    chunk_size: int = 512,
):
    """
    Clip a raster to each polygon in a vector file and save as Cloud Optimized GeoTIFFs.

    Arguments:
        raster (str): Path to the input raster file.
        vector (str): Path to the input vector file.
        column (str): Name of the column in the vector file to use for naming output files.
        cell_size (int): Cell size to include in the output file name.
        output (str): Directory to save the clipped rasters.
        buffer (int, optional): Buffer around each polygon in the same units as the raster CRS. Defaults to 0.
        raster_units (str, optional): Units of the raster CRS (e.g., "feet" or "meters"). Defaults to "feet".
        cache_limit (int, optional): Maximum RAM cache for GDAL in MB. Defaults to 256 MB.
        chunk_size (int, optional): Size of the blocks for tiled output. Defaults to 512.

    Returns:
        None
    """

    if not os.path.exists(output):
        os.makedirs(output)
    if not os.path.exists(raster):
        raise ValueError(f"Raster file '{raster}' not found.")
    if not os.path.exists(vector):
        raise ValueError(f"Vector file '{vector}' not found.")
    if cache_limit is None:
        cache_limit = 256  # Default to 256 MB (in MB)
    if raster_units not in ["feet", "meters"]:
        raise ValueError("raster_units must be either 'feet' or 'meters'.")
    vector_gdf = gpd.read_file(vector)
    if column not in vector_gdf.columns:
        raise ValueError(f"Column '{column}' not found in vector file.")
    if vector_gdf.empty:
        raise ValueError("Vector file contains no features.")

    with rasterio.open(raster, sharing=False) as src:
        if vector_gdf.crs != src.crs:
            vector_gdf = vector_gdf.to_crs(src.crs)
        for idx, row in vector_gdf.iterrows():
            output_file = f"{output}/dem-{row[column]}-{cell_size}ft.tif"
            if os.path.exists(output_file):
                continue
            print(f"Clipping raster for polygon {idx} with {column} = {row[column]}")
            geom = [row["geometry"]]

            # buffer the geometry if a buffer is specified
            if buffer != 0:
                # Union all geometries into a single geometry and apply the buffer
                if raster_units == "feet":
                    buffer_miles = (
                        buffer * 5280
                    )  # Convert miles to feet assuming raster CRS is in feet
                elif raster_units == "meters":
                    buffer_miles = (
                        buffer * 1609.34
                    )  # Convert miles to meters assuming raster CRS is in meters
                geom = [unary_union(geom).buffer(buffer_miles)]

            # Get bounds: (xmin, ymin, xmax, ymax)
            v_bounds = geom[0].bounds

            # Convert geographical bounds strictly into a pixel Window (0 RAM consumed)
            raw_window = src.window(*v_bounds)

            # Correctly round the float bounds to whole-pixel integers
            target_window = raw_window.round()

            # Intersect with the actual raster boundaries to avoid out-of-bound errors
            raster_bounds_window = Window(0, 0, src.width, src.height)
            target_window = target_window.intersection(raster_bounds_window)

            # Create output profile based on the calculated pixel window
            out_transform = src.window_transform(target_window)
            out_meta = src.meta.copy()
            out_meta.update(
                {
                    "driver": "GTiff",
                    "height": target_window.height,
                    "width": target_window.width,
                    "transform": out_transform,
                    "tiled": True,
                    "blockxsize": chunk_size,
                    "blockysize": chunk_size,
                }
            )
            nodata_value = src.nodata if src.nodata is not None else 0

            # Open the destination file and stream in small chunks
            # Configure GDAL_CACHEMAX via Env to drop memory tiles aggressively
            with rasterio.Env(GDAL_CACHEMAX=cache_limit * 1024 * 1024):  # noqa: SIM117
                with rasterio.open(output_file, "w", **out_meta) as dst:
                    chunk_height = chunk_size * 2

                    # wrap in tqdm for progress visualization
                    for y_offset in tqdm(
                        range(0, target_window.height, chunk_height),
                        desc="Clipping chunks",
                    ):
                        current_height = min(
                            chunk_height, target_window.height - y_offset
                        )

                        # Define exact target window to read from the large original raster
                        read_window = Window(
                            col_off=target_window.col_off,
                            row_off=target_window.row_off + y_offset,
                            width=target_window.width,
                            height=current_height,
                        )

                        # Define matching window to write into the output raster
                        write_window = Window(
                            col_off=0,
                            row_off=y_offset,
                            width=target_window.width,
                            height=current_height,
                        )

                        # Stream ONLY this chunk's pixels into RAM
                        chunk_data = src.read(window=read_window)

                        # Calculate the exact geographical transform for just this current chunk
                        chunk_transform = src.window_transform(read_window)

                        # Create a temporary mini-mask array matching JUST this window size (e.g., 1024 x Width)
                        chunk_mask = rasterize(
                            shapes=geom,
                            out_shape=(current_height, target_window.width),
                            transform=chunk_transform,
                            fill=0,  # Outside the polygon
                            default_value=1,  # Inside the polygon
                            dtype=np.uint8,
                        )

                        # Apply the chunk mask: where the mask is 0, overwrite with NoData
                        for band_idx in range(src.count):
                            chunk_data[band_idx][chunk_mask == 0] = nodata_value

                        # Write the processed micro-chunk directly to disk
                        dst.write(chunk_data, window=write_window)

            # clean up temporary variables and free memory
            del chunk_data, chunk_mask, chunk_transform
            gc.collect()


if __name__ == "__main__":
    RASTER_PATH = "/workspaces/ffrd-3dep/data/output/mosaic/terrain_allegheny_4ft.tif"
    VECTOR_PATH = "/workspaces/ffrd-3dep/data/input/domain/ras-units_allegheny.shp"
    VECTOR_COLUMN = "ModelName"
    OUTPUT_PATH = "/workspaces/ffrd-3dep/data/output/ras_units"
    CELL_SIZE = 4
    BUFFER_MILES = 0.5
    RASTER_UNITS = "feet"
    CACHE_LIMIT = 512
    CHUNK_SIZE = 512

    clip_raster_to_polygons(
        RASTER_PATH,
        VECTOR_PATH,
        VECTOR_COLUMN,
        CELL_SIZE,
        OUTPUT_PATH,
        BUFFER_MILES,
        RASTER_UNITS,
        CACHE_LIMIT,
        CHUNK_SIZE,
    )
