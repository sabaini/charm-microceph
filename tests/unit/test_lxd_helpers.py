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

"""Tests for LXD integration helpers."""

import json
import unittest
from unittest.mock import MagicMock, patch

from tests.helpers import lxd


class TestLxdHelpers(unittest.TestCase):
    """Validate LXD test helpers."""

    def test_list_disks_filters_non_disk_entries(self):
        """Only whole block disks with paths should be returned."""
        juju = MagicMock()
        juju.ssh.return_value = json.dumps(
            {
                "blockdevices": [
                    {"path": "/dev/sda", "size": "16G", "type": "disk"},
                    {"path": "/dev/sda1", "size": "15G", "type": "part"},
                    {"path": "/dev/loop0", "size": "100M", "type": "loop"},
                    {"size": "6G", "type": "disk"},
                ]
            }
        )

        self.assertEqual(lxd.list_disks(juju, "microceph/0"), {"/dev/sda": "16G"})
        juju.ssh.assert_called_once_with(
            "microceph/0", "lsblk", "-d", "-J", "-o", "PATH,SIZE,TYPE"
        )

    @patch("tests.helpers.lxd.time.sleep")
    @patch("tests.helpers.lxd.list_disks")
    def test_wait_for_disk_returns_new_matching_disk(self, mock_list_disks, _mock_sleep):
        """A newly attached matching disk should be selected over existing disks."""
        mock_list_disks.side_effect = [
            {"/dev/sda": "16G", "/dev/sdb": "3G"},
            {"/dev/sda": "16G", "/dev/sdb": "3G", "/dev/sdc": "6G"},
        ]

        disk_path = lxd.wait_for_disk(
            MagicMock(),
            "microceph/0",
            size_pattern="6G|5.6G",
            existing_disks={"/dev/sda", "/dev/sdb"},
        )

        self.assertEqual(disk_path, "/dev/sdc")

    @patch("tests.helpers.lxd.time.sleep")
    @patch("tests.helpers.lxd.list_disks", return_value={"/dev/sda": "16G", "/dev/sdb": "6G"})
    def test_wait_for_disk_size_match_is_anchored(self, _mock_list_disks, _mock_sleep):
        """A 6G selector must not spuriously match the existing 16G root disk."""
        disk_path = lxd.wait_for_disk(MagicMock(), "microceph/0", size_pattern="6G")

        self.assertEqual(disk_path, "/dev/sdb")
