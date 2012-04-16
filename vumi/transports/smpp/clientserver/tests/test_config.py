"""Tests for vumi.transports.smpp.clientserver.config."""

from twisted.trial import unittest
from vumi.transports.smpp.clientserver.config import ClientConfig


class ClientConfigTestCase(unittest.TestCase):

    def test_minimal_instantiation(self):
        config1 = ClientConfig(
            host='localhost',
            port=2775,
            system_id='test_system',
            password='password',
            )
        config2 = ClientConfig(
            host='localhost',
            port=2775,
            system_id='test_system',
            password='password',
            system_type="",
            interface_version="34",
            dest_addr_ton=0,
            dest_addr_npi=0,
            registered_delivery=0,
            )
        self.assertEqual(config1, config2)

    def test_instantiation_extended(self):
        client_config = ClientConfig.from_config({
            "host": 'localhost',
            "port": 2775,
            "system_id": 'test_system',
            "password": 'password',
            "system_type": "some_type",
            "interface_version": "34",
            "dest_addr_ton": 1,
            "dest_addr_npi": 1,
            "registered_delivery": 1,
            "smpp_bind_timeout": 33,
            "some_garbage_param_that_should_not_be_here": "foo",
            })
        expected_config = ClientConfig(
            host='localhost',
            port=2775,
            system_id='test_system',
            password='password',
            system_type="some_type",
            interface_version="34",
            dest_addr_ton=1,
            dest_addr_npi=1,
            registered_delivery=1,
            smpp_bind_timeout=33,
            )
        self.assertEqual(client_config, expected_config)

    def test_string_values_are_converted(self):
        client_config = ClientConfig.from_config({
            "host": 'localhost',
            "port": 2775,
            "system_id": 'test_system',
            "password": 'password',
            "dest_addr_ton": "1",
            "dest_addr_npi": "1",
            "registered_delivery": "1",
            "smpp_bind_timeout": "33",
            "initial_reconnect_delay": "1.5",
            })
        self.assertEqual(client_config.dest_addr_ton, 1)
        self.assertEqual(client_config.dest_addr_npi, 1)
        self.assertEqual(client_config.registered_delivery, 1)
        self.assertEqual(client_config.smpp_bind_timeout, 33)
        self.assertEqual(client_config.initial_reconnect_delay, 1.5)
