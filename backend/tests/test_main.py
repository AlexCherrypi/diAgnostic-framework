"""``python -m diagcore`` CLI: --port / DIAGCORE_PORT resolution (M0.5)."""

from __future__ import annotations

from diagcore.__main__ import DEFAULT_PORT, parse_args, resolve_port


def test_default_port_unchanged() -> None:
    assert DEFAULT_PORT == 8765  # SPEC 2 default must not change
    assert resolve_port(None, {}) == 8765


def test_env_overrides_default() -> None:
    assert resolve_port(None, {"DIAGCORE_PORT": "8766"}) == 8766


def test_cli_beats_env() -> None:
    assert resolve_port(9000, {"DIAGCORE_PORT": "8766"}) == 9000


def test_non_numeric_env_falls_back_to_default() -> None:
    assert resolve_port(None, {"DIAGCORE_PORT": "car"}) == 8765


def test_empty_env_falls_back_to_default() -> None:
    assert resolve_port(None, {"DIAGCORE_PORT": ""}) == 8765


def test_parse_args() -> None:
    assert parse_args([]).port is None  # default behavior untouched
    assert parse_args(["--port", "8766"]).port == 8766


def test_main_passes_resolved_port_to_uvicorn(monkeypatch) -> None:
    """The resolved port reaches ``uvicorn.run``; the host stays 127.0.0.1
    (SPEC 2). Core/app construction is stubbed: no server, no plugin spawn."""
    import diagcore.__main__ as main_mod

    calls: list[dict] = []
    monkeypatch.setattr(main_mod, "AppCore", lambda: object())
    monkeypatch.setattr(main_mod, "create_app", lambda core: "app")
    monkeypatch.setattr(main_mod.uvicorn, "run", lambda app, **kwargs: calls.append({"app": app, **kwargs}))

    monkeypatch.delenv("DIAGCORE_PORT", raising=False)
    main_mod.main([])  # default behavior untouched
    main_mod.main(["--port", "8766"])
    monkeypatch.setenv("DIAGCORE_PORT", "8767")
    main_mod.main([])
    main_mod.main(["--port", "8768"])  # CLI beats env

    assert [c["port"] for c in calls] == [8765, 8766, 8767, 8768]
    assert all(c["host"] == "127.0.0.1" and c["app"] == "app" for c in calls)
