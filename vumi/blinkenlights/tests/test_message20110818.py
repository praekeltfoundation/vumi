import time

import vumi.blinkenlights.message20110818 as message
from vumi.tests.helpers import VumiTestCase


class TestMessage(VumiTestCase):

    def test_to_dict(self):
        now = time.time()
        datapoint = ("vumi.w1.a_metric", now, 1.5, ("sum",))
        msg = message.MetricMessage()
        msg.append(datapoint)
        self.assertEqual(msg.to_dict(), {
            'datapoints': [datapoint],
            })

    def test_from_dict(self):
        now = time.time()
        datapoint = ("vumi.w1.a_metric", now, 1.5, ("avg",))
        msgdict = {"datapoints": [datapoint]}
        msg = message.MetricMessage.from_dict(msgdict)
        self.assertEqual(msg.datapoints(), [datapoint])

    def test_extend(self):
        now = time.time()
        datapoint = ("vumi.w1.a_metric", now, 1.5, ("min", "max"))
        msg = message.MetricMessage()
        msg.extend([datapoint, datapoint, datapoint])
        self.assertEqual(msg.datapoints(), [
            datapoint, datapoint, datapoint])
