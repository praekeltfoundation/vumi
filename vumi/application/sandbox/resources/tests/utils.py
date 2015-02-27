from twisted.internet.defer import inlineCallbacks

from vumi.application.sandbox.resources.utils import SandboxCommand
from vumi.application.sandbox.tests.utils import DummyAppWorker
from vumi.tests.helpers import VumiTestCase


class ResourceTestCaseBase(VumiTestCase):

    app_worker_cls = DummyAppWorker
    resource_cls = None
    resource_name = 'test_resource'
    sandbox_id = 'test_id'

    def setUp(self):
        self.app_worker = self.app_worker_cls()
        self.resource = None
        self.api = self.app_worker.create_sandbox_api()
        self.sandbox = self.app_worker.create_sandbox_protocol(self.sandbox_id,
                                                               self.api)

    def check_reply(self, reply, success=True, **kw):
        self.assertEqual(reply['success'], success)
        for key, expected_value in kw.iteritems():
            self.assertEqual(reply[key], expected_value)

    @inlineCallbacks
    def create_resource(self, config):
        if self.resource is not None:
            # clean-up any existing resource so
            # .create_resource can be called multiple times.
            yield self.resource.teardown()
        resource = self.resource_cls(self.resource_name,
                                     self.app_worker,
                                     config)
        self.add_cleanup(resource.teardown)
        yield resource.setup()
        self.resource = resource

    def dispatch_command(self, cmd, **kwargs):
        if self.resource is None:
            raise ValueError("Create a resource before"
                             " calling dispatch_command")
        msg = SandboxCommand(cmd=cmd, **kwargs)
        # round-trip message to get something more similar
        # to what would be returned by a real sandbox when
        # msgs are loaded from JSON.
        msg = SandboxCommand.from_json(msg.to_json())
        return self.resource.dispatch_request(self.api, msg)
