from collections import defaultdict


class DummyAppWorker(object):

    class DummyApi(object):
        def __init__(self):
            self.logs = []

        def set_sandbox(self, sandbox):
            self.sandbox = sandbox
            self.sandbox_id = sandbox.sandbox_id

        def log(self, message, level):
            self.logs.append((level, message))

    class DummyProtocol(object):
        def __init__(self, sandbox_id, api):
            self.sandbox_id = sandbox_id
            self.api = api
            api.set_sandbox(self)

    sandbox_api_cls = DummyApi
    sandbox_protocol_cls = DummyProtocol

    def __init__(self):
        self.mock_calls = defaultdict(list)
        self.mock_returns = {}

    def create_sandbox_api(self):
        return self.sandbox_api_cls()

    def create_sandbox_protocol(self, sandbox_id, api):
        return self.sandbox_protocol_cls(sandbox_id, api)

    def __getattr__(self, name):
        def mock_method(*args, **kw):
            self.mock_calls[name].append((args, kw))
            return self.mock_returns.get(name)
        return mock_method
