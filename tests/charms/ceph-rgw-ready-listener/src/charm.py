#!/usr/bin/env python3
"""Minimal charm that waits for ceph RGW readiness."""

from charms.sunbeam_libs.v0.service_readiness import ServiceReadinessRequirer
from ops.charm import CharmBase
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus


class CephRgwReadyListenerCharm(CharmBase):
    """Charm that blocks until the ceph-rgw-ready relation reports ready."""

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self._stored.set_default(rgw_ready=False)

        self._readiness = ServiceReadinessRequirer(self, "ceph-rgw-ready")

        self.framework.observe(
            self._readiness.on.readiness_changed, self._on_readiness_changed
        )
        self.framework.observe(self._readiness.on.goneaway, self._on_goneaway)
        self.framework.observe(self.on.start, self._update_status)
        self.framework.observe(self.on.update_status, self._update_status)

    def _on_readiness_changed(self, _):
        """Update cached readiness flag on relation data change."""
        self._stored.rgw_ready = self._readiness.service_ready
        self._update_status()

    def _on_goneaway(self, _):
        """Reset readiness when the relation departs."""
        self._stored.rgw_ready = False
        self._update_status()

    def _update_status(self, _=None):
        """Publish the current readiness over unit status."""
        relation = self.model.get_relation("ceph-rgw-ready")
        if not relation:
            self._stored.rgw_ready = False
            self.unit.status = BlockedStatus("waiting for ceph-rgw-ready")
            return

        if not self._stored.rgw_ready:
            self.unit.status = BlockedStatus("rgw not ready yet")
            return

        self.unit.status = ActiveStatus("rgw ready")


if __name__ == "__main__":
    main(CephRgwReadyListenerCharm)
