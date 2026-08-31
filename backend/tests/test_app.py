"""Composition root: well-known nodes, plugin discovery, core routing."""

from __future__ import annotations

from pathlib import Path

from diagcore.app import AppCore


async def test_well_known_nodes(tmp_path: Path) -> None:
    core = AppCore(plugins_dir=tmp_path / "none")
    tree = core.tree
    assert tree.root_id == "root"
    assert tree.get("root").type == "root"
    assert tree.get("root").children == ["system", "plugins", "jobs"]
    assert tree.get("system").type == "system"
    assert tree.get("plugins").type == "plugins"
    assert tree.get("jobs").type == "jobs"
    props = tree.get("system").props
    assert set(props) == {"version", "startedAt", "pythonVersion"}


async def test_broken_manifest_yields_error_node(tmp_path: Path) -> None:
    plugins = tmp_path / "plugins"
    bad = plugins / "broken_one"
    bad.mkdir(parents=True)
    (bad / "manifest.json").write_text("{ this is not json", encoding="utf-8")

    core = AppCore(plugins_dir=plugins)
    await core.start()
    try:
        node = core.tree.get("plugin:broken_one")
        assert node is not None
        assert node.status == "error"
        assert node.error.message == "invalid plugin manifest"
        assert core.hosts == {}  # no host spawned for it
    finally:
        await core.stop()


async def test_duplicate_plugin_id_yields_error_node(tmp_path: Path) -> None:
    plugins = tmp_path / "plugins"
    manifest = '{"id": "dup", "name": "Dup", "version": "0.0.1", "entry": "plugin.py"}'
    for dir_name in ("dup_a", "dup_b"):
        plugin_dir = plugins / dir_name
        plugin_dir.mkdir(parents=True)
        (plugin_dir / "manifest.json").write_text(manifest, encoding="utf-8")
        (plugin_dir / "plugin.py").write_text("", encoding="utf-8")

    core = AppCore(plugins_dir=plugins)
    core._load_plugins_from_disk()  # no start(): don't spawn processes

    # first (sorted) directory wins; the duplicate is flagged, not silent
    assert set(core.hosts) == {"plugin:dup"}
    assert core.hosts["plugin:dup"].plugin_dir.name == "dup_a"
    broken = core.tree.get("plugin:dup_b")
    assert broken is not None
    assert broken.status == "error"
    assert "duplicate plugin id" in broken.error.message
    assert "dup_a" in broken.error.detail


async def test_core_command_unknown_action(tmp_path: Path) -> None:
    core = AppCore(plugins_dir=tmp_path / "none")
    result = await core.bus.dispatch("system", "frobnicate", {})
    assert not result.ok
    assert "unknown action" in result.error.message
