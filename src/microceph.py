#!/usr/bin/env python3

"""Handle Ceph commands."""

import logging
import subprocess
from socket import gethostname

logger = logging.getLogger(__name__)


def _run_cmd(cmd: list) -> str:
    """Execute provided command via subprocess."""
    try:
        process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
        logger.debug(f"Command {' '.join(cmd)} finished; Output: {process.stdout}")
        return process.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed executing cmd: {cmd}, error: {e.stderr}")
        raise e


def remove_cluster_member(name: str, is_force: bool) -> None:
    """Remove a cluster member."""
    cmd = ["microceph", "cluster", "remove", name]
    if is_force:
        cmd.append("--force")
    _run_cmd(cmd)


def get_public_address(hostname:str = None) -> str:
    """Returns MicroCeph public address for hostname, or local address."""
    name = hostname
    if not name:
        # use local name if none provided.
        name = gethostname()

    cmd = [
        "microceph",
        "cluster",
        "sql",
        f"select value from config where key = 'mon.host.{name}'",
    ]
    # NOTE: output is of the form.
    # '+-------+\n| value |\n+-------+\n+-------+\n'
    # '+------------+\n|   value    |\n+------------+\n| 10.5.2.132 |\n+------------+\n'
    output = _run_cmd(cmd).rsplit(" ", 2)

    if "value" in output[-2]:
        raise ValueError(f"Provided hostname {name} has no address record in MicroCeph.")

    return output[-2]


def is_cluster_member(hostname: str) -> bool:
    """Checks if the provided host is part of the microcluster."""
    cmd = ["microceph", "status"]
    try:
        output = _run_cmd(cmd)
        return hostname in str(output)
    except subprocess.CalledProcessError as e:
        error_not_initialised = "Daemon not yet initialized"
        if error_not_initialised in e.stderr:
            # not a cluster member if daemon not initialised.
            return False
        else:
            raise e


def add_osd_cmd(spec: str) -> None:
    """Executes MicroCeph add osd cmd with provided spec."""
    cmd = ["microceph", "disk", "add", spec]
    _run_cmd(cmd)
