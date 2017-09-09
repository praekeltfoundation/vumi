# -*- coding: utf-8 -*-
# -*- test-case-name: vumi.transports.httprpc.tests.test_auth -*-

from zope.interface import implementer

from twisted.cred import portal, checkers, credentials, error
from twisted.web import resource


@implementer(portal.IRealm)
class HttpRpcRealm(object):

    def __init__(self, resource):
        self._resource = resource

    def requestAvatar(self, user, mind, *interfaces):
        if resource.IResource in interfaces:
            return (resource.IResource, self._resource, lambda: None)
        raise NotImplementedError()


@implementer(checkers.ICredentialsChecker)
class StaticAuthChecker(object):
    """Checks that a username and password matches given static values.
    """

    credentialInterfaces = (credentials.IUsernamePassword,)

    def __init__(self, username, password):
        self._username = username
        self._password = password

    def requestAvatarId(self, credentials):
        authorized = all((credentials.username == self._username,
                          credentials.password == self._password))
        if not authorized:
            raise error.UnauthorizedLogin()
        return self._username
