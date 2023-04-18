#!/usr/bin/env python3

# Copyright 2023 Canonical Ltd.
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


"""Microceph Operator Charm.

This charm deploys and manages microceph.
"""
import logging

from ops.charm import CharmBase, RelationEvent
from ops.framework import EventBase, EventSource, ObjectEvents
from ops_sunbeam.interfaces import OperatorPeers
from ops_sunbeam.relation_handlers import BasePeerHandler

logger = logging.getLogger(__name__)


class MicroClusterNewNodeEvent(RelationEvent):
    """charm runs add-node in response to this event, passes join URL back."""


class MicroClusterNodeAddedEvent(RelationEvent):
    """charm runs join in response to this event using supplied join URL."""


class MicroClusterRemoveNodeEvent(RelationEvent):
    """charm runs remove-node to this event."""


class MicroClusterEvents(ObjectEvents):
    """Events related to MicroCluster apps."""

    add_node = EventSource(MicroClusterNewNodeEvent)
    node_added = EventSource(MicroClusterNodeAddedEvent)
    remove_node = EventSource(MicroClusterRemoveNodeEvent)


class MicroClusterPeers(OperatorPeers):
    """Interface for the microcluster peers relation."""

    on = MicroClusterEvents()

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        """Run constructor."""
        super().__init__(charm, relation_name)

        self.state.set_default(joined=False)
        self.framework.observe(charm.on[relation_name].relation_departed, self.on_departed)

    def _event_args(self, relation_event):
        return dict(
            relation=relation_event.relation,
            app=relation_event.app,
            unit=relation_event.unit,
        )

    def on_created(self, event: EventBase) -> None:
        """Handle relation created event."""
        # Do nothing or raise an event to charm?
        pass

    def on_changed(self, event: EventBase) -> None:
        """Handle relation changed event."""
        keys = [key for key in self.get_all_app_data().keys() if key.endswith(".join_token")]
        if event.unit and self.model.unit.is_leader():
            if not keys:
                logger.debug("We are the seed node.")
                # The seed node is implicitly joined, so there's no need to emit an event.
                self.state.joined = True

            if f"{event.unit.name}.join_token" in keys:
                logger.debug(f"Already added {event.unit.name} to the cluster")
                return

            logger.debug("Emitting add_node event")
            self.on.add_node.emit(**self._event_args(event))
        else:
            # Node already joined as member of cluster
            if self.state.joined:
                logger.debug(f"Node {self.model.unit.name} already joined")
                return

            # Join token not yet generated for this node
            if f"{self.model.unit.name}.join_token" not in keys:
                logger.debug(f"Join token not yet generated for node {self.model.unit.name}")
                return

            # TOCHK: Can we pull app data and unit data and emit node_added events based on them
            # do we need to save joined in unit data which might trigger relation-changed event?
            logger.debug("Emitting node_added event")
            event_args = self._event_args(event)
            event_args["unit"] = self.model.unit
            self.on.node_added.emit(**event_args)

    def on_joined(self, event: EventBase) -> None:
        """Handle relation joined event."""
        # Do nothing or raise an event to charm?
        pass

    def on_departed(self, event: EventBase) -> None:
        """Handle relation departed event."""
        if not event.unit:
            return

        if not self.model.unit.is_leader():
            return

        # TOCHK: Can we remove node which is not joined?
        logger.debug("Emitting remove_unit event")
        self.on.remove_node.emit(**self._event_args(event))


class MicroClusterPeerHandler(BasePeerHandler):
    """Base handler for managing a peers relation."""

    def setup_event_handler(self) -> None:
        """Configure event handlers for peer relation."""
        logger.debug("Setting up peer event handler")
        peer_int = MicroClusterPeers(self.charm, self.relation_name)

        self.framework.observe(peer_int.on.add_node, self._on_add_node)
        self.framework.observe(peer_int.on.node_added, self._on_node_added)
        self.framework.observe(peer_int.on.remove_node, self._on_remove_node)

        return peer_int

    def _on_add_node(self, event):
        if not self.is_leader_ready():
            logger.debug("Add node event, deferring the event as leader not ready")
            event.defer()
            return

        if not self.model.unit.is_leader():
            logger.debug("Ignoring Add node event as this is not leader unit")
            return

        self.callback_f(event)
        # The event is emitted only onto leader node or this node
        # self.interface.on.node_added.emit(**self.interface._event_args(event))

    def _on_node_added(self, event):
        if self.model.unit.name != event.unit.name:
            logger.debug("Ignoring Node Added event, event received on other node")
            return

        self.callback_f(event)

    def _on_remove_node(self, event):
        if not self.is_leader_ready():
            logger.debug("Remove node event, deferring the event as leader not ready")
            event.defer()
            return

        if not self.model.unit.is_leader():
            logger.debug("Ignoring Remove node event as this is not leader unit")
            return

        self.callback_f(event)
