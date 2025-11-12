"""Shared helper functions for integration tests."""

import contextlib
import time
from typing import Iterable, Iterator

import jubilant


@contextlib.contextmanager
def fast_forward(juju: jubilant.Juju) -> Iterator[None]:
    """Temporarily run update-status hooks every 10 seconds."""
    current_config = juju.model_config()
    previous_interval = current_config.get("update-status-hook-interval", "5m")
    juju.model_config({"update-status-hook-interval": "10s"})
    try:
        yield
    finally:
        juju.model_config({"update-status-hook-interval": previous_interval})


def wait_for_apps(
    juju: jubilant.Juju,
    *apps: str,
    timeout: int = 600,
) -> None:
    """Wait for *apps* to reach an active state."""
    juju.wait(
        lambda status: jubilant.all_active(status, *apps),
        error=jubilant.any_error,
        timeout=timeout,
    )


def first_unit_name(status: jubilant.Status, app: str) -> str:
    """Return the first unit name for *app*."""
    units = status.apps[app].units
    if not units:
        raise AssertionError(f"{app} has no units")
    return next(iter(units))


def ensure_loop_osd(
    juju: jubilant.Juju,
    app: str,
    loop_spec: str,
    unit_names: Iterable[str] | None = None,
) -> None:
    """Ensure each unit listed has a loop-backed OSD enrolled."""
    status = juju.status()
    units = status.apps[app].units
    if not units:
        raise AssertionError(f"{app} has no units to enroll OSDs on")

    targets = list(unit_names) if unit_names else list(units.keys())

    missing_units = [unit for unit in targets if unit not in units]
    if missing_units:
        raise AssertionError(f"Unknown units requested for OSD enrollment: {missing_units}")

    for unit_name in targets:
        task = juju.run(unit_name, "add-osd", {"loop-spec": loop_spec})
        task.raise_on_failure()

    with fast_forward(juju):
        wait_for_apps(juju, app, timeout=600)


def wait_for_broker_response(
    juju: jubilant.Juju,
    requirer_app: str,
    provider_app: str,
    *,
    timeout: int = 600,
    interval: int = 10,
) -> tuple[dict[str, str], str]:
    """Poll relation data until the requirer broker response is present."""
    deadline = time.time() + timeout
    last_data: dict[str, str] = {}
    broker_rsp_key = ""

    while time.time() < deadline:
        status = juju.status()
        model_name = status.model.name
        requirer_unit = first_unit_name(status, requirer_app)
        provider_unit = first_unit_name(status, provider_app)
        broker_rsp_key = f"broker-rsp-{requirer_unit.replace('/', '-')}"
        last_data = _get_relation_data(requirer_unit, provider_unit, model_name)
        if broker_rsp_key in last_data:
            return last_data, broker_rsp_key
        time.sleep(interval)

    raise AssertionError(
        f"Timed out waiting for broker response ({broker_rsp_key!r}); last data: {last_data}"
    )


def _get_relation_data(requirer_unit: str, provider_unit: str, model_name: str) -> dict[str, str]:
    """Fetch relation data between requirer and provider units."""
    from . import test_utils  # Local import to avoid circular dependencies

    return test_utils.get_relation_data(requirer_unit, "ceph", provider_unit, model_name)
