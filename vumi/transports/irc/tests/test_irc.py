"""Tests for vumi.transports.irc.irc."""

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.tests.utils import get_stubbed_worker
from vumi.transports.irc.workers import MemoWorker
