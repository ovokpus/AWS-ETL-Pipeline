"""Streamlit smoke tests for an empty lake and a finished local run."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from streamlit.testing.v1 import AppTest

from local_emulator.pipeline import run_pipeline

FIXED_NOW = datetime(2026, 10, 29, 16, 0, 0)
APP_PATH = Path(__file__).resolve().parent.parent / "dashboard" / "app.py"


def test_empty_lake_tells_you_the_lake_is_empty(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LAKE_DIR", str(tmp_path))
    app = AppTest.from_file(str(APP_PATH))
    app.run(timeout=30)

    assert not app.exception
    message = " ".join(block.value for block in app.info).lower()
    assert "empty" in message


def test_dashboard_shows_the_sample_run(tmp_path, monkeypatch) -> None:
    run_pipeline(tmp_path, now=FIXED_NOW)
    monkeypatch.setenv("LAKE_DIR", str(tmp_path))
    app = AppTest.from_file(str(APP_PATH))
    app.run(timeout=30)

    assert not app.exception
    values = {metric.value for metric in app.metric}
    assert "6" in values
    assert any("succeeded" in block.value.lower() for block in app.success)


def test_each_section_renders_the_sample_lake(tmp_path, monkeypatch) -> None:
    run_pipeline(tmp_path, now=FIXED_NOW)
    monkeypatch.setenv("LAKE_DIR", str(tmp_path))
    app = AppTest.from_file(str(APP_PATH))
    app.run(timeout=30)

    for section in (
        "Commissions",
        "Payment advice",
        "Telco notifications",
        "Reference data",
        "Run status",
    ):
        app.sidebar.radio[0].set_value(section).run(timeout=30)
        assert not app.exception, section

    app.sidebar.radio[0].set_value("Telco notifications").run(timeout=30)
    app.selectbox[0].set_value("Cedarlink").run(timeout=30)
    assert not app.exception
    warning = " ".join(block.value for block in app.warning).lower()
    assert "header-only" in warning
