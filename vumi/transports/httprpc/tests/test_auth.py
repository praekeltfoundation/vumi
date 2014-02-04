# -*- coding: utf-8 -*-

"""Tests for vumi.transports.httprpc.auth."""

from twisted.web.resource import IResource
from twisted.cred.credentials import UsernamePassword
from twisted.cred.error import UnauthorizedLogin

from vumi.tests.helpers import VumiTestCase
from vumi.transports.httprpc.auth import HttpRpcRealm, StaticAuthChecker


class TestHttpRpcRealm(VumiTestCase):
    def mk_realm(self):
        resource = object()
        return resource, HttpRpcRealm(resource)

    def test_resource_interface(self):
        user, mind = object(), object()
        expected_resource, realm = self.mk_realm()
        interface, resource, cleanup = realm.requestAvatar(
            user, mind, IResource)
        self.assertEqual(interface, IResource)
        self.assertEqual(resource, expected_resource)
        self.assertEqual(cleanup(), None)

    def test_unknown_interface(self):
        user, mind = object(), object()
        expected_resource, realm = self.mk_realm()
        self.assertRaises(NotImplementedError,
                          realm.requestAvatar, user, mind, *[])


class TestStaticAuthChecker(VumiTestCase):
    def test_valid_credentials(self):
        checker = StaticAuthChecker("user", "pass")
        creds = UsernamePassword("user", "pass")
        self.assertEqual(checker.requestAvatarId(creds),
                         "user")

    def test_invalid_credentials(self):
        checker = StaticAuthChecker("user", "pass")
        creds = UsernamePassword("user", "bad-pass")
        self.assertRaises(UnauthorizedLogin, checker.requestAvatarId, creds)
