"""Tests for process_single_dem reprojection behavior."""

import numpy as np
from types import SimpleNamespace

from tnm.tnm import calculate_tap_bounds, process_single_dem

TARGET_CRS = """PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_FFRD",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-96.0],PARAMETER["Standard_Parallel_1",29.5],PARAMETER["Standard_Parallel_2",45.5],PARAMETER["Latitude_Of_Origin",23.0],UNIT["Foot",0.3048]]"""
TARGET_RES = 4.0
NODATA_VAL = -9999


class _FakeGeoDataFrame:
    def __init__(self, geometry=None, crs=None):
        self._geometry = geometry or []
        self.crs = crs

    def to_crs(self, _crs):
        return self

    @property
    def geometry(self):
        return SimpleNamespace(values=self._geometry)


class _FakeRio:
    def __init__(self, dataset, bounds):
        self._dataset = dataset
        self._bounds = bounds
        self.crs = "EPSG:4326"
        self.nodata = NODATA_VAL
        self.last_reproject = None
        self.last_to_raster = None

    def write_crs(self, crs_wkt, inplace=True):
        self.crs = crs_wkt

    def clip(self, _geom, all_touched=True, drop=True):
        return self._dataset

    def bounds(self):
        return self._bounds

    def reproject(
        self, target_crs, shape=None, transform=None, resampling=None, nodata=None
    ):
        self.last_reproject = {
            "target_crs": target_crs,
            "shape": shape,
            "transform": transform,
            "resampling": resampling,
            "nodata": nodata,
        }
        self.crs = target_crs
        if shape is not None:
            self._dataset.values = np.ones(shape, dtype=float)
        return self._dataset

    def write_nodata(self, nodata, inplace=True):
        self.nodata = nodata

    def to_raster(self, *_args, **_kwargs):
        self.last_to_raster = {"args": _args, "kwargs": _kwargs}
        return None


class _FakeDataset:
    def __init__(self, bounds):
        self.values = np.ones((2, 2), dtype=float)
        self.spatial_ref = SimpleNamespace(attrs={"crs_wkt": "EPSG:4326"})
        self.rio = _FakeRio(self, bounds)

    def compute(self):
        return self

    def close(self):
        return None


def test_process_single_dem_sets_crs_and_resolution(monkeypatch, tmp_path):
    bounds = (0.2, 0.2, 10.2, 10.2)
    dataset = _FakeDataset(bounds)

    monkeypatch.setattr("tnm.tnm.rxr.open_rasterio", lambda *_args, **_kwargs: dataset)
    monkeypatch.setattr("tnm.tnm.gpd.GeoDataFrame", _FakeGeoDataFrame)
    monkeypatch.setattr("tnm.tnm.wkb.loads", lambda *_args, **_kwargs: "geom")

    output_path = tmp_path / "out" / "tile.tif"
    args = (
        "s3://fake.tif",
        str(output_path),
        b"fake",
        "EPSG:4326",
        TARGET_CRS,
        TARGET_RES,
        NODATA_VAL,
        "lzw",
    )

    result = process_single_dem(args)

    assert result["success"] is True
    assert dataset.rio.crs == TARGET_CRS

    reproj = dataset.rio.last_reproject
    assert reproj is not None
    assert reproj["target_crs"] == TARGET_CRS

    transform = reproj["transform"]
    assert transform is not None
    assert abs(transform.a - TARGET_RES) < 1e-6
    assert abs(-transform.e - TARGET_RES) < 1e-6
    assert transform.c % TARGET_RES == 0
    assert transform.f % TARGET_RES == 0


def test_tap_bounds_are_aligned():
    bounds = (0.2, 0.2, 10.2, 10.2)
    aligned = calculate_tap_bounds(bounds, TARGET_RES)
    for value in aligned:
        assert value % TARGET_RES == 0


def test_process_single_dem_sets_nodata(monkeypatch, tmp_path):
    bounds = (0.2, 0.2, 10.2, 10.2)
    dataset = _FakeDataset(bounds)

    monkeypatch.setattr("tnm.tnm.rxr.open_rasterio", lambda *_args, **_kwargs: dataset)
    monkeypatch.setattr("tnm.tnm.gpd.GeoDataFrame", _FakeGeoDataFrame)
    monkeypatch.setattr("tnm.tnm.wkb.loads", lambda *_args, **_kwargs: "geom")

    output_path = tmp_path / "out" / "tile.tif"
    nodata_val = NODATA_VAL
    args = (
        "s3://fake.tif",
        str(output_path),
        b"fake",
        "EPSG:4326",
        TARGET_CRS,
        TARGET_RES,
        nodata_val,
        "lzw",
    )

    result = process_single_dem(args)

    assert result["success"] is True
    assert dataset.rio.nodata == nodata_val


def test_process_single_dem_writes_cog(monkeypatch, tmp_path):
    bounds = (0.2, 0.2, 10.2, 10.2)
    dataset = _FakeDataset(bounds)

    monkeypatch.setattr("tnm.tnm.rxr.open_rasterio", lambda *_args, **_kwargs: dataset)
    monkeypatch.setattr("tnm.tnm.gpd.GeoDataFrame", _FakeGeoDataFrame)
    monkeypatch.setattr("tnm.tnm.wkb.loads", lambda *_args, **_kwargs: "geom")

    output_path = tmp_path / "out" / "tile.tif"
    args = (
        "s3://fake.tif",
        str(output_path),
        b"fake",
        "EPSG:4326",
        TARGET_CRS,
        TARGET_RES,
        NODATA_VAL,
        "lzw",
    )

    result = process_single_dem(args)

    assert result["success"] is True
    assert dataset.rio.last_to_raster is not None
    assert dataset.rio.last_to_raster["kwargs"].get("driver") == "COG"
