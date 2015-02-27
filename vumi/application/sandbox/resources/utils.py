from __future__ import absolute_import

import json
import logging
from uuid import uuid4

from twisted.internet.defer import inlineCallbacks, maybeDeferred

from vumi.utils import load_class_by_string, to_kwargs
from vumi.message import Message


class SandboxCommand(Message):
    @staticmethod
    def generate_id():
        return uuid4().get_hex()

    def process_fields(self, fields):
        fields = super(SandboxCommand, self).process_fields(fields)
        fields.setdefault('cmd', 'unknown')
        fields.setdefault('cmd_id', self.generate_id())
        fields.setdefault('reply', False)
        return fields

    def validate_fields(self):
        super(SandboxCommand, self).validate_fields()
        self.assert_field_present(
            'cmd',
            'cmd_id',
            'reply',
        )

    @classmethod
    def from_json(cls, json_string):
        # We override this to avoid the datetime conversions.
        return cls(_process_fields=False, **to_kwargs(json.loads(json_string)))


class SandboxResources(object):
    """Class for holding resources common to a set of sandboxes."""

    def __init__(self, app_worker, config):
        self.app_worker = app_worker
        self.config = config
        self.resources = {}

    def add_resource(self, resource_name, resource):
        """Add additional resources -- should only be called before
           calling :meth:`setup_resources`."""
        self.resources[resource_name] = resource

    def validate_config(self):
        # FIXME: The name of this method is a vicious lie.
        #        It does not validate configs. It constructs resources objects.
        #        Fixing that is beyond the scope of this commit, however.
        for name, config in self.config.iteritems():
            cls = load_class_by_string(config.pop('cls'))
            self.resources[name] = cls(name, self.app_worker, config)

    @inlineCallbacks
    def setup_resources(self):
        for resource in self.resources.itervalues():
            yield resource.setup()

    @inlineCallbacks
    def teardown_resources(self):
        for resource in self.resources.itervalues():
            yield resource.teardown()


class SandboxResource(object):
    """Base class for sandbox resources."""
    # TODO: SandboxResources should probably have their own config definitions.
    #       Is that overkill?

    def __init__(self, name, app_worker, config):
        self.name = name
        self.app_worker = app_worker
        self.config = config

    def setup(self):
        pass

    def teardown(self):
        pass

    def sandbox_init(self, api):
        pass

    def reply(self, command, **kwargs):
        return SandboxCommand(cmd=command['cmd'], reply=True,
                              cmd_id=command['cmd_id'], **kwargs)

    def reply_error(self, command, reason):
        return self.reply(command, success=False, reason=reason)

    def dispatch_request(self, api, command):
        handler_name = 'handle_%s' % (command['cmd'],)
        handler = getattr(self, handler_name, self.unknown_request)
        return maybeDeferred(handler, api, command)

    def unknown_request(self, api, command):
        api.log("Resource %s received unknown command %r from"
                " sandbox %r. Killing sandbox. [Full command: %r]"
                % (self.name, command['cmd'], api.sandbox_id, command),
                logging.ERROR)
        api.sandbox_kill()  # it's a harsh world
