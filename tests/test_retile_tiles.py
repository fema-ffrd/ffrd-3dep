"""Unit tests for retile tiles worker behavior."""

from pathlib import Path

from shapely.geometry import box
from shapely import wkb

from tnm.retile import _retile_worker

from tests.constants import NODATA_VAL, TARGET_CRS, TARGET_RES


class _FakeCompleted:
    def __init__(self, returncode=0, stderr=""):
        self.returncode = returncode
        self.stderr = stderr


def test_retile_worker_builds_gdalwarp_command(monkeypatch, tmp_path):
    captured = {}

    def _fake_run(cmd, capture_output=True, text=True):
        captured["cmd"] = cmd
        return _FakeCompleted()

    monkeypatch.setattr("tnm.retile.subprocess.run", _fake_run)

    geom = box(0, 0, 10, 10)
    args = (
        Path("input.vrt"),
        tmp_path / "out.tif",
        wkb.dumps(geom),
        TARGET_CRS,
        TARGET_RES,
        NODATA_VAL,
        "LZW",
        "bilinear",
        False,
        256,
        512,
    )

    result = _retile_worker(args)

    assert result["success"] is True
    cmd = captured["cmd"]
    assert cmd[0] == "gdalwarp"
    assert "-of" in cmd and "COG" in cmd
    assert "-cutline" in cmd
    assert "-cutline_srs" in cmd and TARGET_CRS in cmd
    assert "-crop_to_cutline" in cmd
    assert "-dstnodata" in cmd and str(NODATA_VAL) in cmd
    assert "-r" in cmd and "bilinear" in cmd
    assert "-tap" in cmd
    assert "-tr" in cmd and str(TARGET_RES) in cmd
    assert "-co" in cmd and "COMPRESS=LZW" in cmd
    assert "-co" in cmd and "BLOCKSIZE=512" in cmd
    assert str(Path("input.vrt")) in cmd
    assert str(tmp_path / "out.tif") in cmd


def test_retile_worker_skips_empty_geometry(tmp_path):
    args = (
        Path("input.vrt"),
        tmp_path / "out.tif",
        None,
        TARGET_CRS,
        TARGET_RES,
        NODATA_VAL,
        "LZW",
        "bilinear",
        False,
        256,
        512,
    )

    result = _retile_worker(args)

    assert result["success"] is False
    assert result["message"] == "Skipped (empty geometry)"
