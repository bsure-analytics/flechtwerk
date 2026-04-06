"""Tests for fretworx CLI argument parsing."""
from fretworx.__main__ import parse_args


def test_parse_bytewax_args():
    args = parse_args(["fretworx","-w", "2", "-r", "/data", "-s", "60", "-b", "0", "ds.sumup.extractor"])
    assert args.w == 2
    assert args.state_dir == "/data"
    assert args.s == 60
    assert args.b == 0
    assert args.module == "ds.sumup.extractor"


def test_parse_fretworx_native_args():
    args = parse_args(["fretworx","--state-dir", "/data/state", "ds.ariadne.extractor"])
    assert args.state_dir == "/data/state"
    assert args.module == "ds.ariadne.extractor"


def test_parse_r_flag_maps_to_state_dir():
    args = parse_args(["fretworx","-r", "/custom/path", "ds.emotivo.transformer"])
    assert args.state_dir == "/custom/path"
    assert args.module == "ds.emotivo.transformer"


def test_parse_defaults():
    args = parse_args(["fretworx","ds.sumup.extractor"])
    assert args.w == 1
    assert args.s == 60
    assert args.b == 0
    assert args.state_dir is None
    assert args.module == "ds.sumup.extractor"
