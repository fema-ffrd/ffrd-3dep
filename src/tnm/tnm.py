# Standard Imports
import os
import re
import gc
import time
import urllib
from typing import Optional, Union
from pathlib import Path
from multiprocessing import get_context

# Third party imports
import subprocess
import requests
import pyproj
import pandas as pd
import numpy as np
from tqdm import tqdm
import rioxarray as rxr
from rasterio.transform import from_bounds
from rasterio.enums import Resampling
import geopandas as gpd
from shapely import wkb
from shapely.geometry import Point, box

# Determine current directory
currDir = os.path.dirname(__file__)
# Shift one level up to the source directory
srcDir = os.path.abspath(os.path.join(currDir, ".."))

BASEURL = "https://tnmaccess.nationalmap.gov/api/v1/products?"
EXPECTED_EPSG = 4326
TARGET_CRS = """PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_FFRD",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-96.0],PARAMETER["Standard_Parallel_1",29.5],PARAMETER["Standard_Parallel_2",45.5],PARAMETER["Latitude_Of_Origin",23.0],UNIT["Foot",0.3048]]"""

DATASETS_DICT = {
    "DEM_1m": "Digital Elevation Model (DEM) 1 meter",
    "DEM_5m": "Alaska IFSAR 5 meter DEM",
    "NED_1-9as": "National Elevation Dataset (NED) 1/9 arc-second",
    "NED_1-3as": "National Elevation Dataset (NED) 1/3 arc-second",
    "NED_1as": "National Elevation Dataset (NED) 1 arc-second",
    "NED_2as": "National Elevation Dataset (NED) Alaska 2 arc-second",
    "LPC": "Lidar Point Cloud (LPC)",
    "OPR": "Original Product Resolution (OPR) Digital Elevation Model (DEM)",
}
LIDARDATAYPES = {"LAS", "LAZ", "LAS,LAZ", ""}


def load_txt_file(path: str) -> list:
    """
    Load lines from a text file into a list.

    Args:
        path (str): Path to the text file.

    Returns:
        list: A list containing lines from the text file.
    """
    with open(path, "r") as file:
        lines = file.readlines()
    return [line.strip() for line in lines]


def _execute_api_request(
    api_url: str, template_query_params: dict, specific_query_params: dict
) -> requests.Response:
    """This function executes a request using python's requests.get(api,params=query) method.

    template_query_params contains standard query params stored as a dictionary, specific_query_params are
    values of that dictionary that should be updated/added for this request

    Args:
        api_url (str): Path to to api that we are querying
        template_query_params (dict): _description_
        specific_query_params (dict): _description_

    Raises:
        SystemExit: A non-success https code was recieved. The API may be invalid or temporarily down.
        SystemExit: A general error occured.

    Returns:
        requests.Response: The result of the call to request.get
    """

    # Copy the template query so as not to update it directly
    query = {k: template_query_params[k] for k in template_query_params}

    # Add any new specifics for thie query
    for key in specific_query_params:
        query[key] = specific_query_params[key]

    # Preset res to None to avoid not returning a result
    res = None
    max_attempts = 5
    base_delay = 1

    for attempt in range(max_attempts):
        try:
            res = requests.get(api_url, params=query, timeout=30)
            res.raise_for_status()  # If something failed about the query, but the request went through
            break
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            should_retry = status_code in {429, 500, 502, 503, 504}
            if should_retry and attempt < max_attempts - 1:
                delay = base_delay * (2**attempt)
                time.sleep(delay)
                continue
            raise
        except requests.exceptions.RequestException:
            if attempt < max_attempts - 1:
                delay = base_delay * (2**attempt)
                time.sleep(delay)
                continue
            raise

    # Try to parse JSON response
    try:
        response_json = res.json()
    except ValueError as e:
        # Invalid JSON response
        raise Exception(
            f"The National Map API returned invalid JSON. Response text: {res.text[:500]}. URL: {res.url}"
        ) from e

    # Check if response is a string (error message) instead of expected dict
    if isinstance(response_json, str):
        raise Exception(
            f"The API returned an unexpected string response: {response_json[:500]}. URL: {res.url}"
        )

    # Check for error in the response
    if isinstance(response_json, dict) and "error" in response_json:
        error_details = response_json.get("error", {})
        if isinstance(error_details, dict):
            error_message = error_details.get("message", "Unknown error")
        else:
            error_message = str(error_details)
        raise Exception(
            f"The API request completed with an error: {error_message}. "
            f"There is likely a problem in the parameters of the request. URL: {res.url}"
        )

    # The requests succeeded, though might not have produced any results
    return res


def _execute_TNM_api_query(
    apiURL: str,
    templateQueryParams: dict,
    specificQueryParams: dict,
    filePath: str,
    doExcludeRedundantData: bool,
    maxitems: Optional[int] = 500,
) -> list:
    """Queries the National Map API and returns list of web-hosted datasets

    Args:
        api_url (str): The API URL
        templateQueryParams (dict): Generic parameters to pass to the query for all requests.
        specificQueryParams (dict): Specific parameters to pass to the query for this request.
        filePath (str): Path to save output to
        doExcludeRedundantData (bool): When the retrieved data has the same spatial boundary,
                this option downloads only the latest version
        maxitems (int, optional): Maximum number of items to return. Defaults to 500.

    Returns:
        aws_url (list): List of TNM download paths
    """

    r = _execute_api_request(apiURL, templateQueryParams, specificQueryParams)

    try:
        x = r.json()
        items = x.get("items", [])
        total = x.get("total", 0)
        messages = x.get("messages", [])

        # Check if there are any messages from the API
        if total == 0 and messages:
            print(f"The National Map API returned: {'; '.join(messages)}")

        aws_url = [item["downloadURL"] for item in items]

        if doExcludeRedundantData and len(aws_url) > 0:
            split_urls = []
            for lst in aws_url:
                split = urllib.parse.urlparse(lst).path.rsplit(
                    "_", 1
                )  # separate the base url (contains lat/long data) from the date
                split_urls.append(split)

            drop_items = [False for i in range(len(split_urls))]
            for i, item_i in enumerate(split_urls):
                if not (drop_items[i]):
                    for j, item_j in enumerate(split_urls):
                        if not (i == j):
                            if (
                                item_i[0] == item_j[0]
                            ):  # if the base url is the same, compare the dates and keep the most recent
                                if item_j[1] < item_i[1] or item_j[1] == item_i[1]:
                                    drop_items[j] = True
                                else:
                                    drop_items[i] = True

            aws_url = [aws_url[i] for i in range(len(aws_url)) if not drop_items[i]]

        if total > len(items):
            print(
                "{} products are available; {} have been fetched.".format(
                    total, maxitems
                )
            )

        if len(aws_url) > 0 and filePath is not None:
            try:
                # If the query returned products AND filePath was specified, write to it
                with open(filePath, "a") as outputs_file:
                    for line in aws_url:
                        outputs_file.write(line + "\n")

            except Exception as e:
                print(f"Error writing AWS URLs to {filePath}: {e}")
                savepath = os.path.join(filePath, "awsPaths.txt")
                with open(savepath, "a") as outputs_file:
                    for line in aws_url:
                        outputs_file.write(line + "\n")

        if not aws_url:
            if total == 0:
                # No data available for this location - don't raise, just return None
                # Let the calling code handle this appropriately
                print(
                    f"No DEM data available for the requested location. "
                    f"The National Map returned 0 results for {specificQueryParams.get('bbox', 'unknown location')}. "
                    f"This area may not have 1m DEM coverage."
                )
            else:
                print(
                    "No products available API request to: {}, with parameters: {}".format(
                        apiURL, specificQueryParams
                    )
                )
            aws_url = None

    except Exception as e:
        print(
            "Error querying National Map API: {}. Request to: {}, with parameters: {}".format(
                e, apiURL, specificQueryParams
            )
        )
        aws_url = None

    return aws_url  # return output as a list


def _check_tnm_dataset_datatype_compatibility(dataset: str, dataType: str):
    """Checks the user-input dataset against the list of available datasets

    Args:
        dataset (str): Dataset name; must match key in DATASETS_DICT
        dataType (str): Datatype to be queried

    Returns:
        dataset_fullname (str): The name of the dataset formatted to be passed to the National Map API

    Raises:
        Exception: Input dataset is LPC and datatype is not LAS, LAZ, LAS,LAZ or left blank
        KeyError: Input dataset is not in DATASETS_DICT

    """

    if (dataset == "LPC") and (dataType not in LIDARDATAYPES):
        raise Exception(
            "Warning, {} is not available. Available datatypes for LPC are LAS, LAZ, or LAS,LAZ".format(
                dataType
            )
        )

    try:
        dataset_fullname = DATASETS_DICT[dataset]
    except KeyError as e:
        raise KeyError(
            "Warning, {} is not available. Available datasets are: {}".format(
                dataset, list(DATASETS_DICT.keys())
            )
        ) from e

    return dataset_fullname


def reprojectXYPoints(xyPoints: list, inEPSG: int, outEPSG: int) -> list:
    """Takes a list of x,y points and projects them from one coordinate reference system
    to another based on EPSG authority code

        Args:
            xyPoints (list): List of x,y points [(x1,y1),(x2,y2),...] describing longitude, latitude or northing, easting values
            inEPSG (int): Source coordinate reference system
            outEPSG (int): Target coordinate reference system
        Returns:
            outPoints (tuple): The x,y vectors transformed into the target coordinate reference system
    """

    fromCRS = pyproj.CRS("EPSG:{}".format(inEPSG))
    toCRS = pyproj.CRS("EPSG:{}".format(outEPSG))

    transformer = pyproj.Transformer.from_crs(fromCRS, toCRS, always_xy=True)

    x_prime = [None for i in range(len(xyPoints))]
    y_prime = [None for i in range(len(xyPoints))]
    for i, pt in enumerate(xyPoints):
        x_prime[i], y_prime[i] = transformer.transform(pt[0], pt[1])

    return (x_prime, y_prime)


def get_aws_paths(
    dataset: str,
    xMin: float,
    yMin: float,
    xMax: float,
    yMax: float,
    filePath: str = None,
    dataType: str = "",
    inputEPSG: int = EXPECTED_EPSG,
    doExcludeRedundantData: bool = True,
    maxitems: Optional[int] = 500,
) -> list:
    """Retrieves paths to geospatial products from TNM of a user-requested
    dataset type, delineated by a bounding box of x,y coordinates

    Args:
        dataset (str):Dataset name; must match key in DATASETS_DICT
        xMin, yMin, xMax, yMax (int OR float): longitude/latitude or easting/northing values expressed in the
            coordinate system supplied as inputEPSG
        filePath (str, optional): Path to save output to. Defaults to None, in which case paths
            are only returned as a list
        dataType (str, optional): Format of to be queried. Defaults to ''
        inputEPSG (int, optional): The source EPSG authority code for the bounding box coordinates.
            Defaults to EXPECTED_EPSG
        doExcludeRedundantData (bool, optional): When retrieved data has the same spatial boundary, this option downloads only the latest version.
            Defaults to True
        maxitems (int, optional): Maximum number of items to return. Defaults to 500

    Returns:
        aws_urls: list of urls to download
    """

    # the way the url for OPR datasets is structured, can't use exclude_redundant
    if dataset == "OPR":
        doExcludeRedundantData = False

    datasetFullName = _check_tnm_dataset_datatype_compatibility(dataset, dataType)

    if inputEPSG != EXPECTED_EPSG:
        x = [xMin, xMax]
        y = [yMin, yMax]
        geom = list(zip(x, y))
        geom_proj = reprojectXYPoints(geom, inputEPSG, EXPECTED_EPSG)
        xMin = geom_proj[0][0]
        xMax = geom_proj[0][1]
        yMin = geom_proj[1][0]
        yMax = geom_proj[1][1]

    specificQueryParams = {
        "prodFormats": dataType,
        "bbox": "{},{},{},{}".format(xMin, yMin, xMax, yMax),
        "datasets": datasetFullName,
        "offset": "0",
    }
    # Only add prodFormats if dataType is specified
    if dataType:
        specificQueryParams["prodFormats"] = dataType

    TNM_QUERY_TEMPLATE = {
        "datasets": "Digital Elevation Model (DEM) 1 meter",
        "max": str(maxitems),
        "prodFormats": "",
    }
    return _execute_TNM_api_query(
        BASEURL,
        TNM_QUERY_TEMPLATE,
        specificQueryParams,
        filePath,
        doExcludeRedundantData,
    )


def prep_gdf_from_latlon(lat: float, lon: float, radius: int):
    """
    Prepare a GeoDataFrame from a latitude and longitude point.

    This function prepares a GeoDataFrame containing a buffer around a point specified by a latitude and longitude.

    Args:
        lat (float): The latitude of the point.
        lon (float): The longitude of the point.
        radius (int): The radius of the buffer in meters.

    Returns:
        geopandas.GeoDataFrame: A GeoDataFrame containing the buffered point.
    """
    point = Point(lon, lat)
    # Create a GeoDataFrame
    gdf = gpd.GeoDataFrame([{"geometry": point}], crs="EPSG:4326")
    # Re-project to a projected CRS
    gdf = gdf.to_crs(epsg=5070)
    # Buffer the point to a circle by 10 km
    gdf["geometry"] = gdf.buffer(radius)
    # Reproject back to EPSG:4326
    gdf = gdf.to_crs(epsg=4326)
    return gdf


def create_grid_from_bbox(
    bbox: tuple, resolution: float, crs: str = "EPSG:4326"
) -> gpd.GeoDataFrame:
    """
    Create a grid of polygons covering a bounding box area.
    Returns only the grid cells that intersect with the domain boundary.

    Args:
        bbox (tuple): Bounding box as (minx, miny, maxx, maxy)
        resolution (float): Grid cell size in the units of the CRS
        crs (str): Coordinate reference system. Default is "EPSG:4326"

    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing grid polygons with an 'id' column

    Example:
        # Create 1km grid in meters (using projected CRS)
        bbox = (xmin, ymin, xmax, ymax)
        grid = create_grid_from_bbox(bbox, resolution=1000, crs="EPSG:5070")

        # Create 0.01 degree grid in lat/lon
        bbox = (-80, 40, -79, 41)
        grid = create_grid_from_bbox(bbox, resolution=0.01, crs="EPSG:4326")
    """
    minx, miny, maxx, maxy = bbox

    # Calculate number of columns and rows
    cols = int(np.ceil((maxx - minx) / resolution))
    rows = int(np.ceil((maxy - miny) / resolution))

    # Create grid cells
    grid_cells = []
    grid_ids = []

    for i in range(cols):
        for j in range(rows):
            # Calculate cell bounds
            cell_minx = minx + (i * resolution)
            cell_miny = miny + (j * resolution)
            cell_maxx = min(cell_minx + resolution, maxx)
            cell_maxy = min(cell_miny + resolution, maxy)

            # Create polygon for this cell
            cell = box(cell_minx, cell_miny, cell_maxx, cell_maxy)
            grid_cells.append(cell)
            grid_ids.append(f"grid_{i}_{j}")

    # Create GeoDataFrame
    grid_gdf = gpd.GeoDataFrame({"id": grid_ids, "geometry": grid_cells}, crs=crs)
    # filter the grids to only those that intersect with the domain boundary
    intersecting_grids = grid_gdf[grid_gdf.intersects(grid_gdf.union_all())]

    return intersecting_grids


def prep_domain(
    domain_file: str, buffer_distance: int, target_crs: str
) -> gpd.GeoDataFrame:
    """
    Prepare the domain GeoDataFrame for processing.
    Ensure all geometries are united.
    Buffer the domain boundary by the specified distance.

    Args:
        domain_file (str): Path to the domain GeoJSON file.
        buffer_distance (int): Buffer distance to apply to the domain boundary.
        target_crs (str): Target coordinate reference system.

    Returns:
        gpd.GeoDataFrame: Prepared domain GeoDataFrame
    """
    domain_gdf = gpd.read_file(domain_file)
    domain_gdf = domain_gdf.to_crs(target_crs)
    domain_gdf["geometry"] = domain_gdf["geometry"].union_all()
    domain_gdf["geometry"] = domain_gdf["geometry"].buffer(buffer_distance)
    return domain_gdf


def main_get_aws_paths(
    dataset: str,
    grids_file: str,
    domain_file: str,
    aws_output_folder: str,
    target_crs: str = TARGET_CRS,
    buffer_distance: int = 1000,
    max_retries: int = 3,
    retry_delay: int = 2,
):
    """
    Main function to get AWS S3 https paths for DEM data from The National Map (TNM) API.

    Paths are determined based on:
        1. The intersection of the native 10-km x 10-km TNM grids and the domain boundary.
        2. The most recent available DEM data for each grid cell based on metadata from the TNM API.

    Parameters:
        dataset (str): Dataset name; must match key in DATASETS_DICT.
        grids_file (str): Path to the grids parquet file.
        domain_file (str): Path to the domain GeoJSON file.
        aws_output_folder (str): Path to the folder to save the AWS paths text file.
        target_crs (str): Target coordinate reference system (e.g., FFRD_WKT).
        buffer_distance (int): Buffer distance to apply to the domain boundary.
        max_retries (int): Maximum number of retries for API requests.
        retry_delay (int): Initial delay between retries in seconds.

    Returns:
        master_aws_paths (list): List of unique AWS paths retrieved.
    """
    domain_gdf = prep_domain(domain_file, buffer_distance, target_crs)
    domain_bbox = domain_gdf.total_bounds
    domain_bbox = (
        domain_bbox[0],
        domain_bbox[1],
        domain_bbox[2],
        domain_bbox[3],
    )

    grids_gdf = gpd.read_parquet(grids_file)
    if grids_gdf.crs != target_crs:
        grids_gdf = grids_gdf.to_crs(target_crs)
    grids_gdf = gpd.sjoin(
        grids_gdf,
        domain_gdf,
        how="inner",
        predicate="intersects",
    ).drop(columns=["index_right"])
    print(f"Loaded {len(grids_gdf)} grid tiles from domain bounding box")
    grids_gdf = grids_gdf.to_crs("EPSG:4326")

    master_aws_paths = []
    for _, grid in tqdm(
        grids_gdf.iterrows(), total=len(grids_gdf), desc="Processing grids"
    ):
        # Get bounding box for this grid cell
        sub_grid = gpd.GeoDataFrame([grid], crs=grids_gdf.crs)
        grid_bounds = sub_grid.total_bounds  # (minx, miny, maxx, maxy)
        for attempt in range(max_retries):
            try:
                # Get the AWS paths for the DEM using grid bounding box
                paths = get_aws_paths(
                    dataset,
                    grid_bounds[0],
                    grid_bounds[1],
                    grid_bounds[2],
                    grid_bounds[3],
                    dataType="GeoTIFF",
                )
                if paths:
                    if len(paths) > 500:
                        print(f"Num paths: {len(paths)}")
                    master_aws_paths.extend(paths)
                break  # Exit the loop if successful
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2**attempt)  # Exponential backoff
                    print(
                        f"API timeout (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    raise Exception(
                        f"The National Map API timed out after {max_retries} attempts. The service may be experiencing high load."
                    )
            except Exception as e:
                print("An error occurred:", str(e))
    master_aws_paths = list(set(master_aws_paths))  # deduplicate paths
    master_aws_paths.sort()
    print(f"Total AWS paths retrieved: {len(master_aws_paths)}")
    # save to txt file
    os.makedirs(aws_output_folder, exist_ok=True)
    aws_output_file = os.path.join(aws_output_folder, "aws_paths.txt")
    with open(aws_output_file, "w") as f:
        for path in master_aws_paths:
            f.write("%s\n" % path)
    return master_aws_paths


def get_dem_coords(path: str) -> tuple:
    """
    Parse the tile coordinates (e.g., 'x63y499') from a USGS DEM filename.

    Args:
        path (str): USGS DEM filename or full path

    Returns:
        tuple: (x, y) as integers, or None if pattern not found
    """
    # Extract just the filename if a full path is provided
    basename = os.path.basename(path)

    # Match pattern like 'x63y499' - x followed by digits, y followed by digits
    match = re.search(r"x(\d+)y(\d+)", basename)

    if match:
        x = int(match.group(1))
        y = int(match.group(2))
        return (x, y)
    return None


def get_dem_year(path: str) -> int:
    """
    Extract the year from a USGS DEM http s3 path.

    The year may be found in the project name or the filename.
    The structure of the path is assumed to be:
    https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/{project_name}/{file_type}/{file_name}

    Args:
        path (str): Full https S3 path to the DEM file.
    Returns:
        int: Extracted year, or None if not found.
    """
    split_path = path.split("/")
    if len(split_path) < 10:
        raise ValueError(
            "Path does not have enough components to extract project name and file name."
        )

    project_name = split_path[7]
    if re.search(r"_(\d{4})_", project_name):
        year_idx = 1
        year_match = re.search(r"_(\d{4})_", project_name).group(year_idx)
    elif re.search(r"_(\d{4})", project_name):
        year_idx = 1
        year_match = re.search(r"_(\d{4})", project_name).group(year_idx)
    elif re.search(r"(\d{4})_", project_name):
        year_idx = 0
        year_match = re.search(r"(\d{4})_", project_name).group(year_idx)
    else:
        year_idx = None
        year_match = None
        print(f"No year found in project name '{project_name}'")
        return None, project_name

    if year_match:
        project_year = int(year_match)
        return project_year, project_name


def calculate_tap_bounds(bounds: tuple, resolution: float) -> tuple:
    """
    Calculate target-aligned pixel (TAP) bounds similar to GDAL's -tap flag.

    Ensures that pixel boundaries fall exactly on integer multiples of the resolution.
    This matches GDAL's -tap behavior where the origin and extent are snapped to
    multiples of the pixel size.

    Args:
        bounds (tuple): Original bounds as (minx, miny, maxx, maxy)
        resolution (float): Target pixel resolution

    Returns:
        tuple: Aligned bounds (minx, miny, maxx, maxy) snapped to resolution grid
    """
    minx, miny, maxx, maxy = bounds

    # Floor the minimum coordinates to the nearest multiple of resolution
    aligned_minx = np.floor(minx / resolution) * resolution
    aligned_miny = np.floor(miny / resolution) * resolution

    # Ceil the maximum coordinates to the nearest multiple of resolution
    aligned_maxx = np.ceil(maxx / resolution) * resolution
    aligned_maxy = np.ceil(maxy / resolution) * resolution

    return (aligned_minx, aligned_miny, aligned_maxx, aligned_maxy)


def process_single_dem(args: tuple) -> dict:
    """
    Process a single DEM file: read, clip, reproject, and save.

    This function is designed to be called in parallel via multiprocessing.
    Takes a tuple of arguments for compatibility with Pool.map().

    Args:
        args: Tuple of (path, output_path, clip_geometry_wkb, clip_crs, target_crs, target_res, nodata_val, compress)

    Returns:
        dict: Result with keys 'path', 'output_path', 'success', 'message'
    """
    (
        path,
        output_path,
        clip_geometry_wkb,
        clip_crs,
        target_crs,
        target_res,
        nodata_val,
        compress,
    ) = args

    # Deserialize geometry from WKB (multiprocessing-safe)
    clip_geometry = wkb.loads(clip_geometry_wkb)

    result = {"path": path, "output_path": output_path, "success": False, "message": ""}

    # Set GDAL memory limits to prevent runaway memory usage
    os.environ["GDAL_CACHEMAX"] = "256"  # 256 MB max cache
    os.environ["GDAL_NUM_THREADS"] = "1"  # Single thread per worker

    ds = None
    try:
        # Use chunks to avoid loading entire raster into memory
        ds = rxr.open_rasterio(path, chunks={"x": 2048, "y": 2048})
    except Exception as e:
        result["message"] = f"Error reading from s3: {e}"
        return result

    if ds is not None:
        try:
            # Get the CRS from the dataset's spatial_ref attribute
            if hasattr(ds, "spatial_ref") and "crs_wkt" in ds.spatial_ref.attrs:
                crs_wkt = ds.spatial_ref.attrs["crs_wkt"]
                ds.rio.write_crs(crs_wkt, inplace=True)
            elif hasattr(ds, "rio") and ds.rio.crs is not None:
                pass
            else:
                result["message"] = "No CRS information found"
                ds.close()
                del ds
                return result

            # Transform clip geometry to raster CRS
            raster_crs = ds.rio.crs

            # Always transform the clip geometry to ensure CRS compatibility
            temp_gdf = gpd.GeoDataFrame(geometry=[clip_geometry], crs=clip_crs)
            temp_gdf = temp_gdf.to_crs(raster_crs)
            clip_geom_transformed = temp_gdf.geometry.values
            del temp_gdf

            # Clip the raster - use drop=True to minimize output size
            try:
                ds = ds.rio.clip(clip_geom_transformed, all_touched=True, drop=True)
            except Exception as e:
                # Check if it's a "no data in bounds" error (expected when tile doesn't overlap domain)
                if "No data found in bounds" in str(e):
                    result["message"] = "Tile does not intersect with domain boundary"
                else:
                    result["message"] = f"Clip failed: {e}"
                ds.close()
                del ds
                return result

            # Load data to check if empty (after clip, should be smaller)
            ds = ds.compute()

            # Capture original nodata value before any transformations
            original_nodata = ds.rio.nodata

            # Check if clipped result is empty
            if original_nodata is not None:
                valid_count = np.count_nonzero(ds.values != original_nodata)
            else:
                valid_count = np.count_nonzero(~np.isnan(ds.values))

            if valid_count == 0:
                result["message"] = "Clip resulted in empty raster"
                del ds
                gc.collect()
                return result

            # Use target-aligned pixels (TAP) to ensure pixel boundaries fall on integer multiples
            if target_crs is not None:
                # First get bounds in target CRS to calculate aligned bounds
                ds_temp = ds.rio.reproject(target_crs)
                bounds = ds_temp.rio.bounds()
                del ds_temp

                # Calculate TAP bounds (like GDAL -tap)
                aligned_bounds = calculate_tap_bounds(bounds, target_res)

                # Calculate dimensions based on aligned bounds
                width = int((aligned_bounds[2] - aligned_bounds[0]) / target_res)
                height = int((aligned_bounds[3] - aligned_bounds[1]) / target_res)

                # Reproject with exact dimensions and resolution
                transform = from_bounds(
                    aligned_bounds[0],
                    aligned_bounds[1],
                    aligned_bounds[2],
                    aligned_bounds[3],
                    width,
                    height,
                )

                ds = ds.rio.reproject(
                    target_crs,
                    shape=(height, width),
                    transform=transform,
                    resampling=Resampling.bilinear,
                    nodata=original_nodata,  # Use original nodata during reprojection
                )

            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Convert vertical units (meters to feet) AND replace nodata values
            # Create mask for nodata pixels (using original nodata value)
            if original_nodata is not None:
                nodata_mask = ds.values == original_nodata
                valid_mask = ~nodata_mask
            else:
                nodata_mask = np.isnan(ds.values)
                valid_mask = ~nodata_mask

            # Convert valid pixels from meters to feet
            ds.values[valid_mask] = ds.values[valid_mask] * 3.28084

            # Replace old nodata values with new nodata value
            ds.values[nodata_mask] = nodata_val

            # Update nodata value in metadata
            ds.rio.write_nodata(nodata_val, inplace=True)

            # Write to GeoTIFF with compression and windowed writing
            if compress:
                ds.rio.to_raster(
                    output_path,
                    driver="COG",
                    compress=compress,
                    blocksize=256,
                )
            else:
                ds.rio.to_raster(output_path, driver="COG", blocksize=256)

            result["success"] = True
            result["message"] = "Success"

            # Explicit cleanup
            del ds
            gc.collect()

            return result

        except Exception as e:
            result["message"] = f"Error processing: {e}"
            if ds is not None:
                try:
                    ds.close()
                except Exception as e:
                    pass
                del ds
            gc.collect()
            return result

    return result


def wrapper_process_single_dem(args: tuple) -> dict:
    """
    Wrapper function to call process_single_dem with error handling.

    Args:
        args (tuple): Arguments for process_single_dem.

    Returns:
        dict: Result from process_single_dem.
    """
    try:
        result = process_single_dem(args)
        return result
    except Exception as e:
        result = {
            "path": args[0] if args else "",
            "output_path": args[1] if len(args) > 1 else "",
            "success": False,
            "message": f"{str(e)}",
        }
        return result


def main_get_dem_tiles(
    paths: list,
    tiles_folder: str,
    logs_folder: str,
    domain_file: str,
    target_crs: str = TARGET_CRS,
    target_res: float = 1,
    nodata_val: int = -9999,
    compress: str = "lzw",
    buffer_distance: int = 1000,
    n_workers: int = 4,
    batch_size: int = 20,
) -> pd.DataFrame:
    """
    Process multiple DEM paths in parallel using multiprocessing with batch processing.

    Uses process-based parallelism (not threads) to ensure complete memory cleanup
    between batches. Each worker process is terminated after each batch.

    Args:
        paths (list): List of S3 paths to DEM files
        tiles_folder (str): Output folder for processed DEM tiles
        logs_folder (str): Folder to save log files for each batch
        domain_file (str): Path to the domain file for clipping
        target_crs (str): Target CRS for reprojection
        target_res (float): Target resolution for reprojection
        nodata_val (int): NoData value to set in output files
        compress (str): Compression method
        buffer_distance (int): Buffer distance around the domain for clipping
        n_workers (int): Number of parallel workers
        batch_size (int): Number of files to process per batch (default: 20)

    Returns:
        pd.DataFrame: Results dataframe with processing status for each path
    """
    # Prepare clip geometry from domain file
    clip_gdf = prep_domain(domain_file, buffer_distance, target_crs)

    # Pre-compute unified clip geometry and serialize to WKB for multiprocessing
    clip_geometry = clip_gdf.geometry.union_all()
    clip_geometry_wkb = wkb.dumps(clip_geometry)  # Serialize for passing to workers
    clip_crs = str(clip_gdf.crs)

    # Build list of unique paths to process (deduplicate by coords/year/name)
    path_info = {}
    for p in paths:
        coords = get_dem_coords(p)
        year, name = get_dem_year(p)
        key = (coords, year, name)
        if key not in path_info:
            path_info[key] = p

    # Convert to list for batching
    items = list(path_info.items())
    total_items = len(items)
    num_batches = (total_items + batch_size - 1) // batch_size

    print(
        f"Processing {total_items} unique DEM tiles in {num_batches} batches "
        f"(batch_size={batch_size}, workers={n_workers})"
    )

    all_results = []
    logs_path = os.path.join(logs_folder, "logs.csv")
    os.makedirs(logs_folder, exist_ok=True)

    for batch_idx in range(num_batches):
        batch_start = batch_idx * batch_size
        batch_end = min(batch_start + batch_size, total_items)
        batch_items = items[batch_start:batch_end]

        print(
            f"\n--- Batch {batch_idx + 1}/{num_batches} "
            f"(items {batch_start + 1}-{batch_end} of {total_items}) ---"
        )

        # Prepare arguments for worker processes
        worker_args = []
        for (coords, year, name), p in batch_items:
            filename = os.path.basename(p)
            output_path = os.path.join(tiles_folder, filename)
            worker_args.append(
                (
                    p,
                    output_path,
                    clip_geometry_wkb,
                    clip_crs,
                    target_crs,
                    target_res,
                    nodata_val,
                    compress,
                )
            )

        # Use 'spawn' context to ensure clean process state (no inherited memory)
        ctx = get_context("spawn")

        # Create a fresh pool for each batch - this ensures complete memory cleanup
        with ctx.Pool(processes=n_workers, maxtasksperchild=1) as pool:
            batch_results = list(
                tqdm(
                    pool.imap(wrapper_process_single_dem, worker_args),
                    total=len(worker_args),
                    desc=f"Batch {batch_idx + 1}",
                )
            )

        all_results.extend(batch_results)

        # Report batch progress
        batch_success = sum(1 for r in batch_results if r["success"])
        print(
            f"Batch {batch_idx + 1} complete: {batch_success}/{len(batch_items)} succeeded"
        )

        # Append batch results to log file incrementally
        batch_df = pd.DataFrame(batch_results)
        batch_df.to_csv(
            logs_path,
            mode="a",
            header=(batch_idx == 0),
            index=False,
        )

        # Force garbage collection
        del worker_args, batch_results, batch_df
        gc.collect()

    # Convert results to DataFrame
    results_df = pd.DataFrame(all_results)

    # Summary statistics
    success_count = results_df["success"].sum()
    fail_count = len(results_df) - success_count
    print(
        f"\nAll batches complete: {success_count} succeeded, {fail_count} failed/skipped"
    )
    return results_df


def create_vrt(
    domain_id: str,
    input_folder: Union[str, Path],
    vrt_folder: Union[str, Path],
    target_res: int,
    pattern: str = "*.tif",
    resolution: Optional[str] = "average",
    nodata: Optional[float] = None,
    relative: bool = True,
) -> str:
    """
    Create a Virtual Raster Tile (VRT) file from a folder of 1m DEM .tif tiles.

    A VRT file acts as a virtual mosaic, allowing you to work with multiple
    tiles as if they were a single raster without physically merging them.

    Args:
        domain_id: Identifier for the domain/area of interest
        input_folder: Path to folder containing .tif DEM tiles
        vrt_folder: Folder for the output .vrt file
        target_res: Target resolution for the VRT
        pattern: Glob pattern to match tile files (default: "*.tif")
        resolution: Resolution strategy for overlapping areas.
                   Options: "highest", "lowest", "average", "user"
                   Default: "average"
        nodata: NoData value to use. If None, uses source file nodata.
        relative: If True, write VRT paths relative to the VRT location.

    Returns:
        str: Path to the created VRT file

    Raises:
        FileNotFoundError: If input folder doesn't exist or contains no matching files
        RuntimeError: If gdalbuildvrt command fails
    """
    input_folder = Path(input_folder)
    output_vrt = Path(vrt_folder) / f"terrain_{domain_id}_{target_res}ft.vrt"

    # Validate input folder exists
    if not input_folder.exists():
        raise FileNotFoundError(f"Input folder does not exist: {input_folder}")

    # Find all matching .tif files
    tif_files = list(input_folder.glob(pattern))

    if not tif_files:
        raise FileNotFoundError(
            f"No files matching '{pattern}' found in {input_folder}"
        )

    print(f"Found {len(tif_files)} tiles in {input_folder}")

    # Ensure output directory exists
    if not os.path.exists(vrt_folder):
        os.makedirs(vrt_folder, exist_ok=True)

    # Build gdalbuildvrt command
    cmd = ["gdalbuildvrt"]

    # Add resolution strategy
    if resolution:
        cmd.extend(["-resolution", resolution])

    # Add nodata value - this is critical for proper VRT creation
    if nodata is not None:
        cmd.extend(["-srcnodata", str(nodata)])
        cmd.extend(["-vrtnodata", str(nodata)])
    else:
        # If nodata not specified, try to use source file nodata
        # But it's better to always specify it explicitly
        pass

    # Add output file
    cmd.append(str(output_vrt))

    # Add input files
    if relative:
        base_dir = output_vrt.parent
        cmd.extend([os.path.relpath(f, start=base_dir) for f in tif_files])
    else:
        cmd.extend([str(f) for f in tif_files])

    print(f"Running: {' '.join(cmd[:5])}... [{len(tif_files)} input files]")

    # Execute command
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(output_vrt.parent) if relative else None,
    )

    if result.returncode != 0:
        raise RuntimeError(f"gdalbuildvrt failed: {result.stderr}")

    if relative and output_vrt.exists():
        import xml.etree.ElementTree as ET

        tree = ET.parse(output_vrt)
        root = tree.getroot()
        base_dir = output_vrt.parent

        for elem in root.iter("SourceFilename"):
            src_path = elem.text or ""
            if src_path:
                rel_path = os.path.relpath(src_path, start=base_dir)
                elem.text = rel_path
                elem.set("relativeToVRT", "1")

        tree.write(output_vrt, encoding="UTF-8", xml_declaration=False)

    print(f"Successfully created VRT: {output_vrt}")
    return str(output_vrt)
