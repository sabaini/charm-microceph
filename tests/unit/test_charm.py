# Copyright 2023 Canonical Ltd.
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

"""Tests for Openstack hypervisor charm."""

import ops_sunbeam.test_utils as test_utils

import charm


class _MicroCephCharm(charm.MicroCephCharm):
    """MicroCeph test charm."""

    def __init__(self, framework):
        """Setup event logging."""
        self.seen_events = []
        super().__init__(framework)


class TestCharm(test_utils.CharmTestCase):
    PATCHES = ["subprocess"]

    def setUp(self):
        """Setup MicroCeph Charm tests."""
        super().setUp(charm, self.PATCHES)
        with open("config.yaml", "r") as f:
            config_data = f.read()
        self.harness = test_utils.get_harness(
            _MicroCephCharm, container_calls=self.container_calls, charm_config=config_data
        )
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_all_relations(self):
        """Test all the charms relations."""
        self.harness.set_leader()
        self.harness.update_config(
            {"snap-channel": "1.0/stable", "osd-devices": "/dev/sdb /dev/sdc"}
        )
        test_utils.add_all_relations(self.harness)
        self.subprocess.run.assert_any_call(
            ["sudo", "microceph", "cluster", "bootstrap"],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )
        self.subprocess.run.assert_any_call(
            ["sudo", "microceph", "disk", "add", "/dev/sdb"],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )
        self.subprocess.run.assert_any_call(
            ["sudo", "microceph", "disk", "add", "/dev/sdc"],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )
