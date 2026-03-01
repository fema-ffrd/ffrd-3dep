"""Unit tests for tnm utilities."""

from unittest import mock
from pathlib import Path

import pytest

from tnm.tnm import (
    _check_tnm_dataset_datatype_compatibility,
    calculate_tap_bounds,
    create_vrt,
    get_dem_coords,
    get_dem_year,
    load_txt_file,
)


def test_load_txt_file_strips_lines(tmp_path):
    sample = tmp_path / "sample.txt"
    sample.write_text("a\n b \n\n")

    lines = load_txt_file(str(sample))

    assert lines == ["a", "b", ""]


def test_get_dem_coords_parses_xy():
    path = "USGS_1M_x63y499_2020.tif"
    assert get_dem_coords(path) == (63, 499)


def test_get_dem_coords_missing_pattern():
    path = "USGS_1M_2020.tif"
    assert get_dem_coords(path) is None


def test_get_dem_year_parses_project_name():
    path = (
        "https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/"
        "USGS_1M_19_x63y499_2020/GeoTIFF/USGS_1M_19_x63y499_2020.tif"
    )
    year, project = get_dem_year(path)
    assert year == 2020
    assert project == "USGS_1M_19_x63y499_2020"


def test_calculate_tap_bounds_snaps_to_resolution():
    bounds = (0.1, 0.1, 9.9, 9.9)
    assert calculate_tap_bounds(bounds, 1.0) == (0.0, 0.0, 10.0, 10.0)


def test_dataset_compatibility_valid():
    name = _check_tnm_dataset_datatype_compatibility("DEM_1m", "")
    assert "Digital Elevation Model" in name


def test_dataset_compatibility_invalid_dataset():
    with pytest.raises(KeyError):
        _check_tnm_dataset_datatype_compatibility("NOPE", "")


def test_dataset_compatibility_invalid_datatype():
    with pytest.raises(Exception):
        _check_tnm_dataset_datatype_compatibility("LPC", "BAD")


def test_create_vrt_missing_folder(tmp_path):
    missing = tmp_path / "missing"
    with pytest.raises(FileNotFoundError):
        create_vrt(
            domain_id="test",
            input_folder=missing,
            vrt_folder=tmp_path,
            target_res=1,
        )


def test_create_vrt_no_files(tmp_path):
    with pytest.raises(FileNotFoundError):
        create_vrt(
            domain_id="test",
            input_folder=tmp_path,
            vrt_folder=tmp_path,
            target_res=1,
        )


def test_create_vrt_runs_gdalbuildvrt(tmp_path):
    input_dir = tmp_path / "tiles"
    output_dir = tmp_path / "vrt"
    input_dir.mkdir()
    output_dir.mkdir()

    tif_path = input_dir / "tile_1.tif"
    tif_path.write_text("fake")

    completed = mock.Mock()
    completed.returncode = 0
    completed.stderr = ""

    with mock.patch("tnm.tnm.subprocess.run", return_value=completed) as run:
        output = create_vrt(
            domain_id="demo",
            input_folder=input_dir,
            vrt_folder=output_dir,
            target_res=1,
            nodata=-9999,
        )

    assert Path(output).name == "terrain_demo_1ft.vrt"
    run.assert_called_once()
