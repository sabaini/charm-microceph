# Copyright 2024 Canonical Ltd.
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

"""Unit tests for device-add-flags parsing."""

import unittest

from device_flags import DeviceAddFlags, parse_device_add_flags


class TestDeviceAddFlagsParsing(unittest.TestCase):
    """Tests for parse_device_add_flags function."""

    def assert_all_flags_disabled(self, flags: DeviceAddFlags):
        """Assert that all parsed flags are False."""
        self.assertFalse(flags.wipe_osd)
        self.assertFalse(flags.encrypt_osd)
        self.assertFalse(flags.wipe_wal)
        self.assertFalse(flags.encrypt_wal)
        self.assertFalse(flags.wipe_db)
        self.assertFalse(flags.encrypt_db)

    def test_empty_flags(self):
        """Empty string returns default flags."""
        self.assert_all_flags_disabled(parse_device_add_flags(""))

    def test_none_flags(self):
        """None returns default flags."""
        self.assert_all_flags_disabled(parse_device_add_flags(None))

    def test_whitespace_only(self):
        """Whitespace-only string returns default flags."""
        self.assert_all_flags_disabled(parse_device_add_flags("   "))

    def test_phase2_flags_parse_correctly(self):
        """OSD, WAL, and DB flags are parsed correctly together."""
        flags = parse_device_add_flags(
            "wipe:osd,encrypt:osd,wipe:wal,encrypt:wal,wipe:db,encrypt:db"
        )
        self.assertTrue(flags.wipe_osd)
        self.assertTrue(flags.encrypt_osd)
        self.assertTrue(flags.wipe_wal)
        self.assertTrue(flags.encrypt_wal)
        self.assertTrue(flags.wipe_db)
        self.assertTrue(flags.encrypt_db)

    def test_case_insensitive_and_empty_entries(self):
        """Parsing is case-insensitive and ignores empty comma-separated entries."""
        flags = parse_device_add_flags(" Wipe:OSD ,, encrypt:wal, ENCRYPT:db , wipe:DB ")
        self.assertTrue(flags.wipe_osd)
        self.assertFalse(flags.encrypt_osd)
        self.assertFalse(flags.wipe_wal)
        self.assertTrue(flags.encrypt_wal)
        self.assertTrue(flags.wipe_db)
        self.assertTrue(flags.encrypt_db)

    def test_unknown_phase2_typo_raises(self):
        """Unknown phase-2 flags fail fast."""
        with self.assertRaises(ValueError) as ctx:
            parse_device_add_flags("wipe:osd,encrypt:wla")
        self.assertIn("Unknown flag", str(ctx.exception))
        self.assertIn("encrypt:wla", str(ctx.exception))

    def test_invalid_flag_mixed_with_valid(self):
        """Mix of valid and invalid flags raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            parse_device_add_flags("wipe:osd,invalid:flag")
        self.assertIn("Unknown flag", str(ctx.exception))

    def test_returns_dataclass(self):
        """Result is a DeviceAddFlags dataclass."""
        flags = parse_device_add_flags("wipe:wal")
        self.assertIsInstance(flags, DeviceAddFlags)
