# -*- test-case-name: vumi.blinkenlights.tests.test_message20110818 -*-


class MetricMessage(object):
    """Class representing Vumi metrics messages.

    A metrics message is a list of (metric_name, timestamp, float
    value) data points and a small amount of metadata:

    * `metric_name` is a dotted byte string,
      e.g. 'vumi.w1.my_metric'.
    * `timestamp` is a float giving seconds since the POSIX Epoch,
      e.g. time.time().
    * `value` is any float.
    """

    # TODO: This class should inherit from Vumi's standardised
    #       message class once we have one.

    def __init__(self):
        self._datapoints = []

    def append(self, datapoint):
        self._datapoints.append(datapoint)

    def extend(self, datapoints):
        self._datapoints.extend(datapoints)

    def to_dict(self):
        return {
            'datapoints': self._datapoints,
            }

    @classmethod
    def from_dict(cls, msgdict):
        msg = cls()
        msg.extend(msgdict['datapoints'])
        return msg
