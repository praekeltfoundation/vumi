from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase

from vumi.workers.blinkenlights import metrics


class UTCNearNow(object):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def __eq__(self, other):
        if not isinstance(other, datetime):
            return False
        now = datetime.utcnow()
        if other > now:
            return False
        if other < now - timedelta(seconds=self.threshold):
            return False
        return True


class MetricsPublisherTestCase(TestCase):

    def _get_testable_publisher(self):
        pub = metrics.MetricsPublisher()
        pub._published = []

        def _pm(msg):
            pub._published.append(msg)
        pub.publish_message = _pm
        return pub

    def assert_pub_metrics(self, pub, counters, timers):
        self.assertEquals(counters, pub.counters)
        self.assertEquals(timers, pub.timers)

    def test_counters(self):
        """
        Adding a counter to a metrics publisher should do the right thing.
        """
        pub = self._get_testable_publisher()

        self.assert_pub_metrics(pub, {}, {})
        pub.add_counter('foo')
        self.assert_pub_metrics(pub, {'foo': 1}, {})
        pub.add_counter('foo', 2)
        self.assert_pub_metrics(pub, {'foo': 3}, {})
        pub.add_counter('bar', 2)
        self.assert_pub_metrics(pub, {'foo': 3, 'bar': 2}, {})
        pub.add_counter('bar')
        self.assert_pub_metrics(pub, {'foo': 3, 'bar': 3}, {})

    def test_timers(self):
        """
        Starting and stopping timers should do the right things.
        """
        pub = self._get_testable_publisher()

        self.assert_pub_metrics(pub, {}, {})
        self.assertRaises(ValueError, pub.stop_timer, 'foo')
        self.assert_pub_metrics(pub, {}, {})
        t = UTCNearNow()
        pub.start_timer('foo')
        self.assert_pub_metrics(pub, {}, {'foo': {'start': [t], 'stop': []}})
        pub.start_timer('foo')
        self.assert_pub_metrics(pub, {}, {'foo': {'start': [t, t],
                                                  'stop': []}})
        pub.stop_timer('foo')
        self.assert_pub_metrics(pub, {}, {'foo': {'start': [t, t],
                                                  'stop': [t]}})
        pub.start_timer('bar')
        self.assert_pub_metrics(pub, {}, {'foo': {'start': [t, t],
                                                  'stop': [t]},
                                          'bar': {'start': [t],
                                                  'stop': []}})
        pub.stop_timer('foo')
        self.assert_pub_metrics(pub, {}, {'foo': {'start': [t, t],
                                                  'stop': [t, t]},
                                          'bar': {'start': [t], 'stop': []}})
        pub.stop_timer('bar')
        self.assert_pub_metrics(pub, {}, {'foo': {'start': [t, t],
                                                  'stop': [t, t]},
                                          'bar': {'start': [t], 'stop': [t]}})
        self.assertRaises(ValueError, pub.stop_timer, 'foo')

    def test_build_metrics(self):
        """
        A metrics message should be built in the correct format.
        """
        pub = self._get_testable_publisher()
        t = lambda secs: datetime(2011, 07, 07, 00, 00, secs)

        pub.counters = {'foo': 10, 'bar': 11}
        pub.timers = {
            'foot': {
                'start': [t(0), t(1)],
                'stop': [t(3), t(2)],
                },
            'bart': {
                'start': [t(4)],
                'stop': [t(5)]},
            }

        _key = lambda i: (i['name'], i['count'])
        expected = sorted([
                {'name': 'foo', 'count': 10},
                {'name': 'bar', 'count': 11},
                {'name': 'foot', 'count': 2, 'time': 4000.0},
                {'name': 'bart', 'count': 1, 'time': 1000.0},
                ], key=_key)

        self.assertEquals(expected, sorted(pub._build_metrics(), key=_key))
