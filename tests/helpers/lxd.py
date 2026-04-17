# Copyright 2025 Canonical Ltd.
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

"""LXD helper functions for functional tests."""

import json
import logging
import re
import subprocess
import time
from collections.abc import Collection

import jubilant

logger = logging.getLogger(__name__)


def get_instance_id(juju: jubilant.Juju, app_name: str, unit_name: str) -> str:
    """Get the LXD instance for a given Juju unit."""
    model_name = juju.status().model.name
    cmd = ["juju", "machines", "--format=json", "-m", model_name]
    out = subprocess.check_output(cmd)
    machines = json.loads(out)

    status = juju.status()
    machine_id = status.apps[app_name].units[unit_name].machine
    return machines["machines"][machine_id]["instance-id"]


def get_lxd_storage_pool() -> str:
    """Find a LXD storage pool or use default."""
    pool_output = subprocess.check_output(["lxc", "storage", "list", "--format=json"])
    pools = json.loads(pool_output)
    for pool in pools:
        if pool["driver"] in ["zfs", "btrfs", "lvm"]:
            return pool["name"]
    return "default"


def create_and_attach_volume(
    pool: str, vol_name: str, instance_id: str, size: str = "1GB"
) -> None:
    """Create a block volume and attach it to an instance."""
    logger.info(f"Creating volume {vol_name}")
    subprocess.run(
        ["lxc", "storage", "volume", "create", pool, vol_name, "--type=block", f"size={size}"],
        check=True,
    )

    logger.info(f"Attaching volume {vol_name} to {instance_id}")
    subprocess.run(["lxc", "storage", "volume", "attach", pool, vol_name, instance_id], check=True)


def cleanup_volume(pool: str, vol_name: str, instance_id: str) -> None:
    """Detach and delete volume."""
    logger.info("Cleaning up volume")
    subprocess.run(
        ["lxc", "storage", "volume", "detach", pool, vol_name, instance_id], check=False
    )
    subprocess.run(["lxc", "storage", "volume", "delete", pool, vol_name], check=False)


def list_disks(juju: jubilant.Juju, unit_name: str) -> dict[str, str]:
    """Return visible block disks keyed by in-guest path."""
    output = juju.ssh(unit_name, "lsblk", "-d", "-J", "-o", "PATH,SIZE,TYPE")
    blockdevices = json.loads(output).get("blockdevices", [])
    return {
        device["path"]: device["size"]
        for device in blockdevices
        if device.get("type") == "disk" and device.get("path") and device.get("size")
    }


def wait_for_disk(
    juju: jubilant.Juju,
    unit_name: str,
    size_pattern: str = "1G|953.7M",
    existing_disks: Collection[str] | None = None,
) -> str:
    """Wait for a newly attached disk matching the size pattern to appear in the unit."""
    logger.info("Waiting for disk to appear")
    size_matcher = re.compile(rf"^(?:{size_pattern})$")
    known_disks = set(existing_disks or ())
    last_seen: dict[str, str] = {}

    for _ in range(30):
        try:
            disks = list_disks(juju, unit_name)
            last_seen = disks
            candidates = {
                path: size
                for path, size in disks.items()
                if path not in known_disks and size_matcher.fullmatch(size)
            }
            if len(candidates) == 1:
                return sorted(candidates)[0]
            if candidates:
                logger.info("Found multiple matching new disks: %s", sorted(candidates))
        except Exception as e:
            logger.warning("SSH failed: %s", e)
        time.sleep(5)

    raise TimeoutError(
        f"Attached disk not found in VM; size_pattern={size_pattern!r}, "
        f"existing_disks={sorted(known_disks)}, visible_disks={last_seen}"
    )
