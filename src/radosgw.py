#!/usr/bin/env python3

# Copyright 2024 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Handle Charm's RADOS Gateway Events."""

import logging

from ops.charm import ActionEvent, CharmBase
from ops.framework import Object

logger = logging.getLogger(__name__)


class RadosGWHandler(Object):
    """The RadosGW class manages the rgw events."""

    name = "rgw"
    charm = None

    def __init__(self, charm: CharmBase, name="rgw"):
        super().__init__(charm, name)
        self.charm = charm
        self.name = name

        self.framework.observe(charm.on.get_rgw_endpoints_action, self._get_rgw_endpoints_action)

    def _get_rgw_endpoints_action(self, event: ActionEvent):
        """Return Rados Gateway endpoints."""
        endpoints = {
            endpoint.get("service_name"): endpoint.get("public_url")
            for endpoint in self.charm.service_endpoints
        }
        logger.debug(f"RGW endpoints: {endpoints}")
        if endpoints:
            event.set_results(endpoints)
            return

        event.set_results({"message": "Rados gateway endpoints are not set yet"})
        event.fail()
