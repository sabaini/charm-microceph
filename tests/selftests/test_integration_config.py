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

"""Tests for configurable integration-test deployment bases."""

import pytest

from tests.conftest import (
    DEFAULT_JUJU_BASE,
    _artifact_name_for_juju_base,
    _juju_base_from_env,
)
from tests.integration.test_terraform import _terraform_apply_args
from tests.sunbeam.conftest import _sunbeam_juju_base


def test_juju_base_defaults_to_noble(monkeypatch) -> None:
    """The existing Noble deployment behavior should remain the default."""
    monkeypatch.delenv("JUJU_BASE", raising=False)

    assert _juju_base_from_env() == DEFAULT_JUJU_BASE == "ubuntu@24.04"


def test_juju_base_accepts_future_ubuntu_base(monkeypatch) -> None:
    """A syntactically valid Ubuntu base should be accepted."""
    monkeypatch.setenv("JUJU_BASE", "ubuntu@26.04")

    assert _juju_base_from_env() == "ubuntu@26.04"


@pytest.mark.parametrize(
    "juju_base",
    ["", "noble", "ubuntu:24.04", "ubuntu@24.04;echo bad", "debian@12"],
)
def test_juju_base_rejects_invalid_values(monkeypatch, juju_base: str) -> None:
    """Invalid or unsafe values should fail before model creation."""
    monkeypatch.setenv("JUJU_BASE", juju_base)

    with pytest.raises(pytest.UsageError, match="Invalid JUJU_BASE"):
        _juju_base_from_env()


def test_artifact_name_uses_selected_base() -> None:
    """Local charm deployments should select the matching built artifact."""
    assert _artifact_name_for_juju_base("ubuntu@26.04") == "microceph_ubuntu-26.04-amd64.charm"


def test_terraform_apply_uses_selected_base(monkeypatch) -> None:
    """Terraform deployments should explicitly receive the selected base."""
    monkeypatch.setattr(
        "tests.integration.test_terraform.TEST_CHARM_CHANNEL",
        "tentacle/candidate",
    )
    monkeypatch.setattr(
        "tests.integration.test_terraform.TEST_SNAP_CHANNEL",
        None,
    )

    args = _terraform_apply_args(units=3, juju_base="ubuntu@26.04")

    assert args == [
        "-var",
        "units=3",
        "-var",
        "channel=tentacle/candidate",
        "-var",
        "base=ubuntu@26.04",
    ]


def test_terraform_apply_omits_snap_channel_when_unset(monkeypatch) -> None:
    """No config var is emitted when MICROCEPH_TEST_SNAP_CHANNEL is unset.

    The Terraform module's snap-channel config default (tentacle/stable) must
    apply unchanged in that case.
    """
    monkeypatch.setattr(
        "tests.integration.test_terraform.TEST_CHARM_CHANNEL",
        "tentacle/candidate",
    )
    monkeypatch.setattr(
        "tests.integration.test_terraform.TEST_SNAP_CHANNEL",
        None,
    )

    args = _terraform_apply_args(units=1, juju_base="ubuntu@26.04")

    assert "config=" not in " ".join(args)


def test_terraform_apply_overrides_snap_channel_when_set(monkeypatch) -> None:
    """MICROCEPH_TEST_SNAP_CHANNEL overrides the module's snap-channel default."""
    monkeypatch.setattr(
        "tests.integration.test_terraform.TEST_CHARM_CHANNEL",
        "tentacle/candidate",
    )
    monkeypatch.setattr(
        "tests.integration.test_terraform.TEST_SNAP_CHANNEL",
        "tentacle/candidate",
    )

    args = _terraform_apply_args(units=1, juju_base="ubuntu@26.04")

    assert 'config={"snap-channel"="tentacle/candidate"}' in " ".join(args)


def test_terraform_apply_merges_snap_channel_with_caller_config(monkeypatch) -> None:
    """Snap-channel override coexists with per-call config such as enable-rgw."""
    monkeypatch.setattr(
        "tests.integration.test_terraform.TEST_CHARM_CHANNEL",
        "tentacle/candidate",
    )
    monkeypatch.setattr(
        "tests.integration.test_terraform.TEST_SNAP_CHANNEL",
        "tentacle/candidate",
    )

    args = _terraform_apply_args(
        units=4,
        juju_base="ubuntu@26.04",
        config={"enable-rgw": "*"},
    )

    # _format_tf_map serializes keys in sorted order.
    assert 'config={"enable-rgw"="*","snap-channel"="tentacle/candidate"}' in " ".join(args)


def test_terraform_apply_caller_config_overrides_snap_channel(monkeypatch) -> None:
    """An explicit per-call snap-channel wins over the env-derived default."""
    monkeypatch.setattr(
        "tests.integration.test_terraform.TEST_CHARM_CHANNEL",
        "tentacle/candidate",
    )
    monkeypatch.setattr(
        "tests.integration.test_terraform.TEST_SNAP_CHANNEL",
        "tentacle/candidate",
    )

    args = _terraform_apply_args(
        units=1,
        juju_base="ubuntu@26.04",
        config={"snap-channel": "reef/stable"},
    )

    assert 'config={"snap-channel"="reef/stable"}' in " ".join(args)
    assert "tentacle/candidate" not in " ".join(args).split("config=", 1)[1]


def test_sunbeam_juju_base_ignores_env(monkeypatch) -> None:
    """Sunbeam tests attach to an existing model; their base must stay fixed.

    Unlike model-creating suites, the Sunbeam suite must not honor
    ``JUJU_BASE``: the refresh artifact has to match the externally deployed
    Sunbeam model's base regardless of the operator's environment.
    """
    monkeypatch.setenv("JUJU_BASE", "ubuntu@26.04")

    assert _sunbeam_juju_base() == DEFAULT_JUJU_BASE == "ubuntu@24.04"
