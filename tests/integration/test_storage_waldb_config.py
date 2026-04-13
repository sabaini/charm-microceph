# Copyright 2026 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Integration tests for declarative OSD/WAL/DB storage provisioning."""

import json
import logging
from pathlib import Path
from typing import Iterator

import jubilant
import pytest

from tests import helpers
from tests.helpers import lxd

logger = logging.getLogger(__name__)

APP_NAME = "microceph"


def _wait_for_active(juju_vm: jubilant.Juju, timeout: int = 300) -> None:
    """Wait for the application to become active/idle."""
    with helpers.fast_forward(juju_vm):
        helpers.wait_for_apps(juju_vm, APP_NAME, timeout=timeout)


def _set_storage_config(
    juju_vm: jubilant.Juju,
    *,
    osd_devices: str = "",
    wal_devices: str = "",
    db_devices: str = "",
    wal_size: str = "",
    db_size: str = "",
    device_add_flags: str = "",
) -> None:
    """Apply config-driven storage options."""
    juju_vm.config(
        APP_NAME,
        {
            "osd-devices": osd_devices,
            "wal-devices": wal_devices,
            "db-devices": db_devices,
            "wal-size": wal_size,
            "db-size": db_size,
            "device-add-flags": device_add_flags,
        },
    )


def _apply_storage_config_and_wait(
    juju_vm: jubilant.Juju,
    *,
    osd_devices: str,
    wal_devices: str = "",
    db_devices: str = "",
    wal_size: str = "",
    db_size: str = "",
    device_add_flags: str = "",
    timeout: int = 600,
) -> None:
    """Apply config-driven storage settings and wait for active state."""
    _set_storage_config(
        juju_vm,
        osd_devices=osd_devices,
        wal_devices=wal_devices,
        db_devices=db_devices,
        wal_size=wal_size,
        db_size=db_size,
        device_add_flags=device_add_flags,
    )
    _wait_for_active(juju_vm, timeout=timeout)


def _clear_storage_config_and_wait(juju_vm: jubilant.Juju, timeout: int = 300) -> None:
    """Clear config-driven storage settings and wait for active state."""
    _set_storage_config(juju_vm)
    status = juju_vm.status()
    unit_name = helpers.first_unit_name(status, APP_NAME)
    unit_status = status.apps[APP_NAME].units[unit_name]
    if (
        unit_status.juju_status.current == "error"
        or unit_status.workload_status.current == "error"
    ):
        juju_vm.cli("resolved", unit_name)
    _wait_for_active(juju_vm, timeout=timeout)


def _disk_list_json(juju_vm: jubilant.Juju) -> dict:
    """Return ``microceph disk list --json`` from the lead unit."""
    unit_name = helpers.first_unit_name(juju_vm.status(), APP_NAME)
    disk_list = juju_vm.ssh(unit_name, "sudo", "microceph", "disk", "list", "--json")
    return json.loads(disk_list)


def _configured_disks(juju_vm: jubilant.Juju) -> list[dict]:
    """Return configured disks from ``microceph disk list --json``."""
    return _disk_list_json(juju_vm).get("ConfiguredDisks", [])


def _configured_osd_ids(juju_vm: jubilant.Juju) -> set[int]:
    """Return configured OSD ids currently visible to MicroCeph."""
    return {int(disk["osd"]) for disk in _configured_disks(juju_vm)}


def _new_configured_osd(juju_vm: jubilant.Juju, before: set[int]) -> dict:
    """Return the single newly configured OSD after a test action."""
    new_disks = [disk for disk in _configured_disks(juju_vm) if int(disk["osd"]) not in before]
    assert len(new_disks) == 1, f"Expected exactly one new configured OSD, got: {new_disks}"
    return new_disks[0]


def _resolve_guest_path(juju_vm: jubilant.Juju, path: str) -> str:
    """Resolve a guest path to its canonical device node path."""
    unit_name = helpers.first_unit_name(juju_vm.status(), APP_NAME)
    return juju_vm.ssh(unit_name, "sudo", "readlink", "-f", path).strip()


def _resolve_osd_link(juju_vm: jubilant.Juju, osd_num: int, link_name: str) -> str:
    """Resolve an OSD data-dir symlink such as ``block.db`` or ``block.wal``."""
    unit_name = helpers.first_unit_name(juju_vm.status(), APP_NAME)
    script = (
        f'path=/var/snap/microceph/common/data/osd/ceph-{osd_num}/{link_name}; '
        'if [ -e "$path" ]; then readlink -f "$path"; fi'
    )
    return juju_vm.ssh(unit_name, "sudo", "bash", "-ec", script).strip()


def _assert_osd_device_layout(
    juju_vm: jubilant.Juju,
    *,
    osd_num: int,
    data_device: str,
    wal_device: str | None = None,
    db_device: str | None = None,
) -> None:
    """Assert that the OSD data dir links point at the expected devices."""
    block_path = _resolve_osd_link(juju_vm, osd_num, "block")
    assert block_path.startswith(
        data_device
    ), f"OSD {osd_num} data path {block_path!r} does not resolve to {data_device!r}"

    if wal_device is not None:
        wal_path = _resolve_osd_link(juju_vm, osd_num, "block.wal")
        assert wal_path, f"OSD {osd_num} is missing block.wal"
        assert wal_path.startswith(
            wal_device
        ), f"OSD {osd_num} WAL path {wal_path!r} does not resolve to {wal_device!r}"

    if db_device is not None:
        db_path = _resolve_osd_link(juju_vm, osd_num, "block.db")
        assert db_path, f"OSD {osd_num} is missing block.db"
        assert db_path.startswith(
            db_device
        ), f"OSD {osd_num} DB path {db_path!r} does not resolve to {db_device!r}"


def _assert_disk_list_excludes_devices(juju_vm: jubilant.Juju, *devices: str) -> None:
    """Assert that disk-list does not expose WAL/DB carrier devices as disks."""
    disk_list = _disk_list_json(juju_vm)
    offenders: list[tuple[str, str, str]] = []

    for section in ("ConfiguredDisks", "AvailableDisks"):
        for disk in disk_list.get(section, []):
            resolved_path = _resolve_guest_path(juju_vm, disk["path"])
            if any(resolved_path.startswith(device) for device in devices):
                offenders.append((section, disk["path"], resolved_path))

    assert not offenders, f"Disk list still exposes WAL/DB carrier devices: {offenders}"


def _create_attached_disk(
    juju_vm: jubilant.Juju,
    *,
    prefix: str,
    size: str,
    size_pattern: str,
) -> tuple[str, str, str, str]:
    """Create and attach an LXD block volume, returning cleanup metadata and path."""
    unit_name = helpers.first_unit_name(juju_vm.status(), APP_NAME)
    inst_id = lxd.get_instance_id(juju_vm, APP_NAME, unit_name)
    pool = lxd.get_lxd_storage_pool()
    model_name = juju_vm.status().model.name
    vol_name = f"{prefix}-{model_name}"

    existing_disks = set(lxd.list_disks(juju_vm, unit_name))
    lxd.create_and_attach_volume(pool, vol_name, inst_id, size=size)
    disk_path = lxd.wait_for_disk(
        juju_vm,
        unit_name,
        size_pattern=size_pattern,
        existing_disks=existing_disks,
    )
    logger.info("Attached %s volume %s at %s", prefix, vol_name, disk_path)
    return pool, vol_name, inst_id, disk_path


@pytest.fixture(scope="module")
def deployed_microceph(juju_vm: jubilant.Juju, microceph_charm: Path):
    """Deploy MicroCeph in a VM suitable for block-device integration tests."""
    helpers.deploy_microceph(
        juju_vm,
        microceph_charm,
        APP_NAME,
        config={"snap-channel": "latest/edge"},
        timeout=3600,
    )
    return APP_NAME


@pytest.fixture(scope="module")
def happy_path_devices(juju_vm: jubilant.Juju, deployed_microceph) -> Iterator[dict[str, str]]:
    """Attach distinct OSD, WAL, and DB devices for the happy-path test."""
    attached = [
        _create_attached_disk(
            juju_vm, prefix="waldb-happy-osd", size="3GB", size_pattern="3G|2.8G|2.9G"
        ),
        _create_attached_disk(
            juju_vm, prefix="waldb-happy-wal", size="4GB", size_pattern="4G|3.7G|3.8G|3.9G"
        ),
        _create_attached_disk(
            juju_vm, prefix="waldb-happy-db", size="5GB", size_pattern="5G|4.6G|4.7G|4.8G"
        ),
    ]
    try:
        yield {"osd": attached[0][3], "wal": attached[1][3], "db": attached[2][3]}
    finally:
        for pool, vol_name, inst_id, _ in reversed(attached):
            lxd.cleanup_volume(pool, vol_name, inst_id)


@pytest.fixture(scope="module")
def warning_path_devices(juju_vm: jubilant.Juju, deployed_microceph) -> Iterator[dict[str, str]]:
    """Attach fresh OSD and DB devices for warning-only WAL coverage."""
    attached = [
        _create_attached_disk(
            juju_vm, prefix="waldb-warn-osd", size="6GB", size_pattern="6G|5.5G|5.6G|5.7G"
        ),
        _create_attached_disk(
            juju_vm, prefix="waldb-warn-db", size="7GB", size_pattern="7G|6.5G|6.6G|6.7G"
        ),
    ]
    try:
        yield {"osd": attached[0][3], "db": attached[1][3]}
    finally:
        for pool, vol_name, inst_id, _ in reversed(attached):
            lxd.cleanup_volume(pool, vol_name, inst_id)


@pytest.fixture(scope="module")
def overlap_osd_device(juju_vm: jubilant.Juju, deployed_microceph) -> Iterator[str]:
    """Attach a fresh OSD device used for overlap validation testing."""
    pool, vol_name, inst_id, disk_path = _create_attached_disk(
        juju_vm,
        prefix="waldb-overlap-osd",
        size="8GB",
        size_pattern="8G|7.4G|7.5G|7.6G",
    )
    try:
        yield disk_path
    finally:
        lxd.cleanup_volume(pool, vol_name, inst_id)


@pytest.fixture(scope="module")
def action_osd_device(juju_vm: jubilant.Juju, deployed_microceph) -> Iterator[str]:
    """Attach a separate disk used to verify coexistence with add-osd."""
    pool, vol_name, inst_id, disk_path = _create_attached_disk(
        juju_vm,
        prefix="waldb-action-osd",
        size="9GB",
        size_pattern="9G|8.3G|8.4G|8.5G",
    )
    try:
        yield disk_path
    finally:
        lxd.cleanup_volume(pool, vol_name, inst_id)


@pytest.mark.abort_on_fail
def test_storage_config_snap_has_waldb_flags(juju_vm: jubilant.Juju, deployed_microceph):
    """Verify the installed snap exposes the Phase 2 disk-add DSL flags."""
    unit_name = helpers.first_unit_name(juju_vm.status(), APP_NAME)
    help_output = juju_vm.ssh(unit_name, "sudo", "microceph", "disk", "add", "--help")

    for expected_flag in (
        "--osd-match",
        "--wal-match",
        "--wal-size",
        "--wal-wipe",
        "--wal-encrypt",
        "--db-match",
        "--db-size",
        "--db-wipe",
        "--db-encrypt",
        "--dry-run",
    ):
        assert (
            expected_flag in help_output
        ), f"Snap does not support {expected_flag}. Help output:\n{help_output}"


@pytest.mark.abort_on_fail
def test_storage_config_missing_wal_size_blocks(
    juju_vm: jubilant.Juju, deployed_microceph, warning_path_devices: dict[str, str]
):
    """Minimal charm-side validation should block wal-devices without wal-size."""
    _set_storage_config(
        juju_vm,
        osd_devices=f"eq(@devnode,'{warning_path_devices['osd']}')",
        wal_devices="eq(@devnode,'/dev/does-not-exist')",
        db_devices=f"eq(@devnode,'{warning_path_devices['db']}')",
        db_size="1GiB",
        device_add_flags="wipe:osd,wipe:db",
    )

    with helpers.fast_forward(juju_vm):
        helpers.wait_for_status(juju_vm, APP_NAME, ("blocked", "error"), timeout=180)

    _clear_storage_config_and_wait(juju_vm, timeout=300)


@pytest.mark.abort_on_fail
def test_storage_config_happy_path_provisions_osd_wal_db(
    juju_vm: jubilant.Juju, deployed_microceph, happy_path_devices: dict[str, str]
):
    """Provision an OSD with distinct WAL and DB carrier devices."""
    before_osds = _configured_osd_ids(juju_vm)

    _apply_storage_config_and_wait(
        juju_vm,
        osd_devices=f"eq(@devnode,'{happy_path_devices['osd']}')",
        wal_devices=f"eq(@devnode,'{happy_path_devices['wal']}')",
        db_devices=f"eq(@devnode,'{happy_path_devices['db']}')",
        wal_size="1GiB",
        db_size="2GiB",
        device_add_flags="wipe:osd,wipe:wal,wipe:db",
        timeout=900,
    )

    helpers.assert_osd_count(juju_vm, APP_NAME, expected_osds=1, timeout=900)
    new_osd = _new_configured_osd(juju_vm, before_osds)
    _assert_osd_device_layout(
        juju_vm,
        osd_num=int(new_osd["osd"]),
        data_device=happy_path_devices["osd"],
        wal_device=happy_path_devices["wal"],
        db_device=happy_path_devices["db"],
    )
    _assert_disk_list_excludes_devices(
        juju_vm,
        happy_path_devices["wal"],
        happy_path_devices["db"],
    )


@pytest.mark.abort_on_fail
def test_storage_config_reapply_is_idempotent(
    juju_vm: jubilant.Juju, deployed_microceph, happy_path_devices: dict[str, str]
):
    """Re-applying the same config should not duplicate OSDs."""
    before_osds = _configured_osd_ids(juju_vm)

    _apply_storage_config_and_wait(
        juju_vm,
        osd_devices=f"eq(@devnode,'{happy_path_devices['osd']}')",
        wal_devices=f"eq(@devnode,'{happy_path_devices['wal']}')",
        db_devices=f"eq(@devnode,'{happy_path_devices['db']}')",
        wal_size="1GiB",
        db_size="2GiB",
        device_add_flags="wipe:osd,wipe:wal,wipe:db",
        timeout=900,
    )

    helpers.assert_osd_count(juju_vm, APP_NAME, expected_osds=1, timeout=900)
    after_osds = _configured_osd_ids(juju_vm)
    assert after_osds == before_osds, f"Reapply unexpectedly changed configured OSDs: {after_osds}"
    only_osd = next(iter(after_osds))
    _assert_osd_device_layout(
        juju_vm,
        osd_num=only_osd,
        data_device=happy_path_devices["osd"],
        wal_device=happy_path_devices["wal"],
        db_device=happy_path_devices["db"],
    )


@pytest.mark.abort_on_fail
def test_storage_config_warning_only_no_wal_match_stays_active(
    juju_vm: jubilant.Juju, deployed_microceph, warning_path_devices: dict[str, str]
):
    """A no-WAL-match warning should stay active and still add the OSD."""
    before_osds = _configured_osd_ids(juju_vm)

    _apply_storage_config_and_wait(
        juju_vm,
        osd_devices=f"eq(@devnode,'{warning_path_devices['osd']}')",
        wal_devices="eq(@devnode,'/dev/does-not-exist')",
        db_devices=f"eq(@devnode,'{warning_path_devices['db']}')",
        wal_size="1GiB",
        db_size="2GiB",
        device_add_flags="wipe:osd,wipe:db",
        timeout=900,
    )

    status = juju_vm.status()
    assert status.apps[APP_NAME].app_status.current == "active"
    helpers.assert_osd_count(juju_vm, APP_NAME, expected_osds=2, timeout=900)
    new_osd = _new_configured_osd(juju_vm, before_osds)
    _assert_osd_device_layout(
        juju_vm,
        osd_num=int(new_osd["osd"]),
        data_device=warning_path_devices["osd"],
        db_device=warning_path_devices["db"],
    )
    _assert_disk_list_excludes_devices(juju_vm, warning_path_devices["db"])


@pytest.mark.abort_on_fail
def test_storage_config_overlap_failure_blocks(
    juju_vm: jubilant.Juju, deployed_microceph, overlap_osd_device: str
):
    """Using the same fresh device for OSD and WAL matching should fail."""
    _set_storage_config(
        juju_vm,
        osd_devices=f"eq(@devnode,'{overlap_osd_device}')",
        wal_devices=f"eq(@devnode,'{overlap_osd_device}')",
        wal_size="1GiB",
        device_add_flags="wipe:osd,wipe:wal",
    )

    with helpers.fast_forward(juju_vm):
        helpers.wait_for_status(juju_vm, APP_NAME, ("blocked", "error"), timeout=300)

    status = juju_vm.status()
    unit_name = helpers.first_unit_name(status, APP_NAME)
    unit_status = status.apps[APP_NAME].units[unit_name]
    assert unit_status.workload_status.current in {"blocked", "error"}
    assert "overlap" in unit_status.workload_status.message.lower()

    _clear_storage_config_and_wait(juju_vm, timeout=300)
    helpers.assert_osd_count(juju_vm, APP_NAME, expected_osds=2, timeout=900)


@pytest.mark.abort_on_fail
def test_storage_config_clear_does_not_remove_existing_osds(
    juju_vm: jubilant.Juju, deployed_microceph
):
    """Clearing config should stop future reconciliation but keep existing OSDs."""
    _clear_storage_config_and_wait(juju_vm, timeout=300)
    helpers.assert_osd_count(juju_vm, APP_NAME, expected_osds=2, timeout=900)


@pytest.mark.abort_on_fail
def test_storage_config_coexists_with_add_osd_action(
    juju_vm: jubilant.Juju, deployed_microceph, action_osd_device: str
):
    """Config-driven OSDs should coexist with OSDs enrolled through add-osd."""
    unit_name = helpers.first_unit_name(juju_vm.status(), APP_NAME)
    task = juju_vm.run(
        unit_name,
        "add-osd",
        {"device-id": action_osd_device, "wipe": True},
        wait=900,
    )
    task.raise_on_failure()

    with helpers.fast_forward(juju_vm):
        helpers.wait_for_apps(juju_vm, APP_NAME, timeout=900)

    helpers.assert_osd_count(juju_vm, APP_NAME, expected_osds=3, timeout=900)


@pytest.mark.abort_on_fail
def test_storage_config_disk_list_still_returns_json(juju_vm: jubilant.Juju, deployed_microceph):
    """Sanity-check the deployed unit still reports disks after WAL/DB flows."""
    parsed = _disk_list_json(juju_vm)
    assert "ConfiguredDisks" in parsed
    assert len(parsed["ConfiguredDisks"]) >= 3
