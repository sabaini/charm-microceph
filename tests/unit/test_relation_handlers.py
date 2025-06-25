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

import unittest

import ops_sunbeam.test_utils as test_utils
from unit import testbase

import relation_handlers


class TestRelationHelpers(testbase.TestBaseCharm):
    PATCHES = [
        "gethostname",
    ]

    def setUp(self):
        """Setup MicroCeph Charm tests."""
        super().setUp(relation_handlers, self.PATCHES)
        with open("config.yaml", "r") as f:
            config_data = f.read()
        with open("metadata.yaml", "r") as f:
            metadata = f.read()
        self.harness = test_utils.get_harness(
            testbase._MicroCephCharm,
            container_calls=self.container_calls,
            charm_config=config_data,
            charm_metadata=metadata,
        )
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_collect_peer_data(self):
        self.harness.set_leader()
        rel_id = self.add_complete_peer_relation(self.harness)
        unit_name = self.harness.model.unit.name
        # set up some initial relation data
        self.harness.update_relation_data(
            rel_id,
            "microceph/0",
            {
                unit_name: "test-hostname",
            },
        )
        self.gethostname.return_value = "test-hostname"
        change_data = relation_handlers.collect_peer_data(self.harness.model)
        self.assertNotIn(unit_name, change_data)
        self.assertEqual(change_data["public-address"], "10.0.0.10")
        self.gethostname.return_value = "changed-hostname"
        # assert that collect_peer_data raises an exception
        with self.assertRaises(relation_handlers.HostnameChangeError):
            relation_handlers.collect_peer_data(self.harness.model)


if __name__ == "__main__":
    unittest.main()
