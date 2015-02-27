# -*- test-case-name: vumi.application.tests.test_sandbox -*-

"""An application for sandboxing message processing."""

import resource
import os
import pkg_resources
import logging

from twisted.internet.defer import inlineCallbacks, returnValue, succeed

from vumi.config import ConfigText, ConfigInt, ConfigList, ConfigDict
from vumi.application.base import ApplicationWorker
from vumi.application.sandbox.utils import SandboxError
from vumi.application.sandbox.protocol import SandboxProtocol
from vumi.application.sandbox.resources.utils import (
    SandboxResources, SandboxResource, SandboxCommand)
from vumi.application.sandbox.resources.logging import LoggingResource
from vumi.errors import ConfigError
from vumi import log


class JsSandboxResource(SandboxResource):
    """
    Resource that initializes a Javascript sandbox.

    Typically used alongside vumi/applicaiton/sandboxer.js which is
    a simple node.js based Javascript sandbox.

    Requires the worker to have a `javascript_for_api` method.
    """
    def sandbox_init(self, api):
        javascript = self.app_worker.javascript_for_api(api)
        app_context = self.app_worker.app_context_for_api(api)
        api.sandbox_send(SandboxCommand(cmd="initialize",
                                        javascript=javascript,
                                        app_context=app_context))


class SandboxApi(object):
    """A sandbox API instance for a particular sandbox run."""

    def __init__(self, resources, config):
        self._sandbox = None
        self._inbound_messages = {}
        self.resources = resources
        self.fallback_resource = SandboxResource("fallback", None, {})
        potential_logger = None
        if config.logging_resource:
            potential_logger = self.resources.resources.get(
                config.logging_resource)
            if potential_logger is None:
                log.warning("Failed to find logging resource %r."
                            " Falling back to Twisted logging."
                            % (config.logging_resource,))
            elif not hasattr(potential_logger, 'log'):
                log.warning("Logging resource %r has no attribute 'log'."
                            " Falling abck to Twisted logging."
                            % (config.logging_resource,))
                potential_logger = None
        self.logging_resource = potential_logger
        self.config = config

    @property
    def sandbox_id(self):
        return self._sandbox.sandbox_id

    def set_sandbox(self, sandbox):
        if self._sandbox is not None:
            raise SandboxError("Sandbox already set ("
                               "existing id: %r, new id: %r)."
                               % (self.sandbox_id, sandbox.sandbox_id))
        self._sandbox = sandbox

    def sandbox_init(self):
        for sandbox_resource in self.resources.resources.values():
            sandbox_resource.sandbox_init(self)

    def sandbox_inbound_message(self, msg):
        self._inbound_messages[msg['message_id']] = msg
        self.sandbox_send(SandboxCommand(cmd="inbound-message",
                                         msg=msg.payload))

    def sandbox_inbound_event(self, event):
        self.sandbox_send(SandboxCommand(cmd="inbound-event",
                                         msg=event.payload))

    def sandbox_send(self, msg):
        self._sandbox.send(msg)

    def sandbox_kill(self):
        self._sandbox.kill()

    def get_inbound_message(self, message_id):
        return self._inbound_messages.get(message_id)

    def log(self, msg, level):
        if self.logging_resource is None:
            # fallback to vumi.log logging if we don't
            # have a logging resource.
            return succeed(log.msg(msg, logLevel=level))
        else:
            return self.logging_resource.log(self, msg, level=level)

    @inlineCallbacks
    def dispatch_request(self, command):
        resource_name, sep, rest = command['cmd'].partition('.')
        if not sep:
            resource_name, rest = '', resource_name
        command['cmd'] = rest
        resource = self.resources.resources.get(resource_name,
                                                self.fallback_resource)
        try:
            reply = yield resource.dispatch_request(self, command)
        except Exception, e:
            # errors here are bugs in Vumi so we always log them
            # via Twisted. However, we reply to the sandbox with
            # a failure and log via the sandbox api so that the
            # sandbox owner can be notified.
            log.error()
            self.log(str(e), level=logging.ERROR)
            reply = SandboxCommand(
                reply=True,
                cmd_id=command['cmd_id'],
                success=False,
                reason=unicode(e))

        if reply is not None:
            reply['cmd'] = '%s%s%s' % (resource_name, sep, rest)
            self.sandbox_send(reply)


class SandboxConfig(ApplicationWorker.CONFIG_CLASS):

    sandbox = ConfigDict(
        "Dictionary of resources to provide to the sandbox."
        " Keys are the names of resources (as seen inside the sandbox)."
        " Values are dictionaries which must contain a `cls` key that"
        " gives the full name of the class that provides the resource."
        " Other keys are additional configuration for that resource.",
        default={}, static=True)

    executable = ConfigText(
        "Full path to the executable to run in the sandbox.")
    args = ConfigList(
        "List of arguments to pass to the executable (not including"
        " the path of the executable itself).", default=[])
    path = ConfigText("Current working directory to run the executable in.")
    env = ConfigDict(
        "Custom environment variables for the sandboxed process.", default={})
    timeout = ConfigInt(
        "Length of time the subprocess is given to process a message.",
        default=60)
    recv_limit = ConfigInt(
        "Maximum number of bytes that will be read from a sandboxed"
        " process' stdout and stderr combined.", default=1024 * 1024)
    rlimits = ConfigDict(
        "Dictionary of resource limits to be applied to sandboxed"
        " processes. Defaults are fairly restricted. Keys maybe"
        " names or values of the RLIMIT constants in"
        " Python `resource` module. Values should be appropriate integers.",
        default={})
    logging_resource = ConfigText(
        "Name of the logging resource to use to report errors detected"
        " in sandboxed code (e.g. lines written to stderr, unexpected"
        " process termination). Set to null to disable and report"
        " these directly using Twisted logging instead.",
        default=None)
    sandbox_id = ConfigText("This is set based on individual messages.")


class Sandbox(ApplicationWorker):
    """Sandbox application worker."""

    CONFIG_CLASS = SandboxConfig

    KB, MB = 1024, 1024 * 1024
    DEFAULT_RLIMITS = {
        resource.RLIMIT_CORE: (1 * MB, 1 * MB),
        resource.RLIMIT_CPU: (60, 60),
        resource.RLIMIT_FSIZE: (1 * MB, 1 * MB),
        resource.RLIMIT_DATA: (64 * MB, 64 * MB),
        resource.RLIMIT_STACK: (1 * MB, 1 * MB),
        resource.RLIMIT_RSS: (10 * MB, 10 * MB),
        resource.RLIMIT_NOFILE: (15, 15),
        resource.RLIMIT_MEMLOCK: (64 * KB, 64 * KB),
        resource.RLIMIT_AS: (196 * MB, 196 * MB),
    }

    def validate_config(self):
        config = self.get_static_config()
        self.resources = self.create_sandbox_resources(config.sandbox)
        self.resources.validate_config()

    def get_config(self, msg):
        config = self.config.copy()
        config['sandbox_id'] = self.sandbox_id_for_message(msg)
        return succeed(self.CONFIG_CLASS(config))

    def _convert_rlimits(self, rlimits_config):
        rlimits = dict((getattr(resource, key, key), value) for key, value in
                       rlimits_config.iteritems())
        for key in rlimits.iterkeys():
            if not isinstance(key, (int, long)):
                raise ConfigError("Unknown resource limit key %r" % (key,))
        return rlimits

    def setup_application(self):
        return self.resources.setup_resources()

    def teardown_application(self):
        return self.resources.teardown_resources()

    def setup_connectors(self):
        # Set the default event handler so we can handle events from any
        # endpoint.
        d = super(Sandbox, self).setup_connectors()

        def cb(connector):
            connector.set_default_event_handler(self.dispatch_event)
            return connector

        return d.addCallback(cb)

    def create_sandbox_resources(self, config):
        return SandboxResources(self, config)

    def get_executable_and_args(self, config):
        return config.executable, [config.executable] + config.args

    def get_rlimits(self, config):
        rlimits = self.DEFAULT_RLIMITS.copy()
        rlimits.update(self._convert_rlimits(config.rlimits))
        return rlimits

    def create_sandbox_protocol(self, api):
        executable, args = self.get_executable_and_args(api.config)
        rlimits = self.get_rlimits(api.config)
        spawn_kwargs = dict(
            args=args, env=api.config.env, path=api.config.path)
        return SandboxProtocol(
            api.config.sandbox_id, api, executable, spawn_kwargs, rlimits,
            api.config.timeout, api.config.recv_limit)

    def create_sandbox_api(self, resources, config):
        return SandboxApi(resources, config)

    def sandbox_id_for_message(self, msg_or_event):
        """Return a sandbox id for a message or event.

        Sub-classes may override this to retrieve an appropriate id.
        """
        return msg_or_event['sandbox_id']

    def sandbox_protocol_for_message(self, msg_or_event, config):
        """Return a sandbox protocol for a message or event.

        Sub-classes may override this to retrieve an appropriate protocol.
        """
        api = self.create_sandbox_api(self.resources, config)
        protocol = self.create_sandbox_protocol(api)
        return protocol

    def _process_in_sandbox(self, sandbox_protocol, api_callback):
        sandbox_protocol.spawn()

        def on_start(_result):
            sandbox_protocol.api.sandbox_init()
            api_callback()
            d = sandbox_protocol.done()
            d.addErrback(log.error)
            return d

        d = sandbox_protocol.started()
        d.addCallbacks(on_start, log.error)
        return d

    @inlineCallbacks
    def process_message_in_sandbox(self, msg):
        config = yield self.get_config(msg)
        sandbox_protocol = yield self.sandbox_protocol_for_message(msg, config)

        def sandbox_init():
            sandbox_protocol.api.sandbox_inbound_message(msg)

        status = yield self._process_in_sandbox(sandbox_protocol, sandbox_init)
        returnValue(status)

    @inlineCallbacks
    def process_event_in_sandbox(self, event):
        config = yield self.get_config(event)
        sandbox_protocol = yield self.sandbox_protocol_for_message(
            event, config)

        def sandbox_init():
            sandbox_protocol.api.sandbox_inbound_event(event)

        status = yield self._process_in_sandbox(sandbox_protocol, sandbox_init)
        returnValue(status)

    def consume_user_message(self, msg):
        return self.process_message_in_sandbox(msg)

    def close_session(self, msg):
        return self.process_message_in_sandbox(msg)

    def consume_ack(self, event):
        return self.process_event_in_sandbox(event)

    def consume_nack(self, event):
        return self.process_event_in_sandbox(event)

    def consume_delivery_report(self, event):
        return self.process_event_in_sandbox(event)


class JsSandboxConfig(SandboxConfig):
    "JavaScript sandbox configuration."

    javascript = ConfigText("JavaScript code to run.", required=True)
    app_context = ConfigText("Custom context to execute JS with.")
    logging_resource = ConfigText(
        "Name of the logging resource to use to report errors detected"
        " in sandboxed code (e.g. lines written to stderr, unexpected"
        " process termination). Set to null to disable and report"
        " these directly using Twisted logging instead.",
        default='log')


class JsSandbox(Sandbox):
    """
    Configuration options:

    As for :class:`Sandbox` except:

    * `executable` defaults to searching for a `node.js` binary.
    * `args` defaults to the JS sandbox script in the `vumi.application`
      module.
    * An instance of :class:`JsSandboxResource` is added to the sandbox
      resources under the name `js` if no `js` resource exists.
    * An instance of :class:`LoggingResource` is added to the sandbox
      resources under the name `log` if no `log` resource exists.
    * `logging_resource` is set to `log` if it is not set.
    * An extra 'javascript' parameter specifies the javascript to execute.
    * An extra optional 'app_context' parameter specifying a custom
      context for the 'javascript' application to execute with.

    Example 'javascript' that logs information via the sandbox API
    (provided as 'this' to 'on_inbound_message') and checks that logging
    was successful::

        api.on_inbound_message = function(command) {
            this.log_info("From command: inbound-message", function (reply) {
                this.log_info("Log successful: " + reply.success);
                this.done();
            });
        }

    Example 'app_context' that makes the Node.js 'path' module
    available under the name 'path' in the context that the sandboxed
    javascript executes in::

        {path: require('path')}
    """

    CONFIG_CLASS = JsSandboxConfig

    POSSIBLE_NODEJS_EXECUTABLES = [
        '/usr/local/bin/node',
        '/usr/local/bin/nodejs',
        '/usr/bin/node',
        '/usr/bin/nodejs',
    ]

    @classmethod
    def find_nodejs(cls):
        for path in cls.POSSIBLE_NODEJS_EXECUTABLES:
            if os.path.isfile(path):
                return path
        return None

    @classmethod
    def find_sandbox_js(cls):
        return pkg_resources.resource_filename(
            'vumi.application.sandbox', 'sandboxer.js')

    def get_js_resource(self):
        return JsSandboxResource('js', self, {})

    def get_log_resource(self):
        return LoggingResource('log', self, {})

    def javascript_for_api(self, api):
        """Called by JsSandboxResource.

        :returns: String containing Javascript for the app to run.
        """
        return api.config.javascript

    def app_context_for_api(self, api):
        """Called by JsSandboxResource

        :returns: String containing Javascript expression that returns
        addition context for the namespace the app is being run
        in. This Javascript is expected to be trusted code.
        """
        return api.config.app_context

    def get_executable_and_args(self, config):
        executable = config.executable
        if executable is None:
            executable = self.find_nodejs()

        args = [executable] + (config.args or [self.find_sandbox_js()])

        return executable, args

    def validate_config(self):
        super(JsSandbox, self).validate_config()
        if 'js' not in self.resources.resources:
            self.resources.add_resource('js', self.get_js_resource())
        if 'log' not in self.resources.resources:
            self.resources.add_resource('log', self.get_log_resource())


class JsFileSandbox(JsSandbox):

    class CONFIG_CLASS(SandboxConfig):
        javascript_file = ConfigText(
            "The file containting the Javascript to run", required=True)
        app_context = ConfigText("Custom context to execute JS with.")

    def javascript_for_api(self, api):
        return file(api.config.javascript_file).read()
