# -*- encoding: utf-8 -*-

"""Tests for vumi.blinkenlights.heartbeat.metadata"""

from twisted.trial.unittest import TestCase
from vumi.blinkenlights.heartbeat import metadata


class TestMetadata(TestCase):

    def setUp(self):
        pass

    def test_produce(self):
        md = metadata.HeartBeatMetadata(None)

        self.assertEqual(md.produce(), {'type': 'undefined'})
