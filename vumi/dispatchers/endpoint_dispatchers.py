# -*- test-case-name: vumi.dispatchers.tests.test_endpoint_dispatchers -*-

"""Basic tools for building dispatchers."""

from twisted.internet.defer import gatherResults, maybeDeferred

from vumi.worker import BaseWorker
from vumi.config import ConfigDict, ConfigList
from vumi import log


class DispatcherConfig(BaseWorker.CONFIG_CLASS):
    receive_inbound_connectors = ConfigList(
        "List of connectors that will receive inbound messages and events.",
        required=True, static=True)
    receive_outbound_connectors = ConfigList(
        "List of connectors that will receive outbound messages.",
        required=True, static=True)


class Dispatcher(BaseWorker):
    """Base class for a dispatcher."""

    CONFIG_CLASS = DispatcherConfig

    def setup_worker(self):
        d = maybeDeferred(self.setup_dispatcher)
        d.addCallback(lambda r: self.unpause_connectors())
        return d

    def teardown_worker(self):
        d = self.pause_connectors()
        d.addCallback(lambda r: self.teardown_dispatcher())
        return d

    def setup_dispatcher(self):
        """
        All dispatcher specific setup should happen in here.

        Subclasses should override this method to perform extra setup.
        """
        pass

    def teardown_dispatcher(self):
        """
        Clean-up of setup done in setup_dispatcher should happen here.
        """
        pass

    def get_configured_ri_connectors(self):
        return self.get_static_config().receive_inbound_connectors

    def get_configured_ro_connectors(self):
        return self.get_static_config().receive_outbound_connectors

    def default_errback(self, f, msg, connector_name):
        log.error(f, "Error routing message for %s" % (connector_name,))

    def process_inbound(self, config, msg, connector_name):
        raise NotImplementedError()

    def errback_inbound(self, f, msg, connector_name):
        return f

    def process_outbound(self, config, msg, connector_name):
        raise NotImplementedError()

    def errback_outbound(self, f, msg, connector_name):
        return f

    def process_event(self, config, event, connector_name):
        raise NotImplementedError()

    def errback_event(self, f, event, connector_name):
        return f

    def _mkhandler(self, handler_func, errback_func, connector_name):
        def handler(msg):
            d = maybeDeferred(self.get_config, msg)
            d.addCallback(handler_func, msg, connector_name)
            d.addErrback(errback_func, msg, connector_name)
            d.addErrback(self.default_errback, msg, connector_name)
            return d
        return handler

    def setup_connectors(self):
        def add_ri_handlers(connector, connector_name):
            connector.set_default_inbound_handler(
                self._mkhandler(
                    self.process_inbound, self.errback_inbound,
                    connector_name))
            connector.set_default_event_handler(
                self._mkhandler(
                    self.process_event, self.errback_event, connector_name))
            return connector

        def add_ro_handlers(connector, connector_name):
            connector.set_default_outbound_handler(
                self._mkhandler(
                    self.process_outbound, self.errback_outbound,
                    connector_name))
            return connector

        deferreds = []
        for connector_name in self.get_configured_ri_connectors():
            d = self.setup_ri_connector(connector_name)
            d.addCallback(add_ri_handlers, connector_name)
            deferreds.append(d)

        for connector_name in self.get_configured_ro_connectors():
            d = self.setup_ro_connector(connector_name)
            d.addCallback(add_ro_handlers, connector_name)
            deferreds.append(d)

        return gatherResults(deferreds)

    def publish_inbound(self, msg, connector_name, endpoint):
        return self.connectors[connector_name].publish_inbound(msg, endpoint)

    def publish_outbound(self, msg, connector_name, endpoint):
        return self.connectors[connector_name].publish_outbound(msg, endpoint)

    def publish_event(self, event, connector_name, endpoint):
        return self.connectors[connector_name].publish_event(event, endpoint)


class RoutingTableDispatcherConfig(Dispatcher.CONFIG_CLASS):
    routing_table = ConfigDict(
        "Routing table. Keys are connector names, values are dicts mapping "
        "endpoint names to [connector, endpoint] pairs.", required=True)


class RoutingTableDispatcher(Dispatcher):
    CONFIG_CLASS = RoutingTableDispatcherConfig

    def find_target(self, config, msg, connector_name):
        endpoint_name = msg.get_routing_endpoint()
        endpoint_routing = config.routing_table.get(connector_name)
        if endpoint_routing is None:
            log.warning("No routing information for connector '%s'" % (
                    connector_name,))
            return None
        target = endpoint_routing.get(endpoint_name)
        if target is None:
            log.warning("No routing information for endpoint '%s' on '%s'" % (
                    endpoint_name, connector_name,))
            return None
        return target

    def process_inbound(self, config, msg, connector_name):
        target = self.find_target(config, msg, connector_name)
        if target is None:
            return
        return self.publish_inbound(msg, target[0], target[1])

    def process_outbound(self, config, msg, connector_name):
        target = self.find_target(config, msg, connector_name)
        if target is None:
            return
        return self.publish_outbound(msg, target[0], target[1])

    def process_event(self, config, event, connector_name):
        target = self.find_target(config, event, connector_name)
        if target is None:
            return
        return self.publish_event(event, target[0], target[1])
