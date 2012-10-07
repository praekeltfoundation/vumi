from twisted.trial.unittest import TestCase

from vumi.middleware.manhole import ManholeMiddleware


class ManholeMiddlewareTestCase(TestCase):

    def setUp(self):
        self._middlewares = []

    def tearDown(self):
        for mw in self._middlewares:
            mw.teardown_middleware()

    def get_middleware(self, config={}):
        config = dict({
            'port': '0',
        }, **config)
        worker = object()
        mw = ManholeMiddleware("test_addr_trans", config, worker)
        mw.setup_middleware()
        self._middlewares.append(mw)
        return mw

    def test_mw(self):
        mw = self.get_middleware()
