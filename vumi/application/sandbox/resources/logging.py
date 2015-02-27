from __future__ import absolute_import

import logging

from twisted.internet.defer import succeed, inlineCallbacks, returnValue

from vumi.application.sandbox.resources.utils import SandboxResource
from vumi import log


class LoggingResource(SandboxResource):
    """
    Resource that allows a sandbox to log messages via Twisted's
    logging framework.
    """
    def log(self, api, msg, level):
        """Logs a message via vumi.log (i.e. Twisted logging).

        Sub-class should override this if they wish to log messages
        elsewhere. The `api` parameter is provided for use by such
        sub-classes.

        The `log` method should always return a deferred.
        """
        return succeed(log.msg(msg, logLevel=level))

    @inlineCallbacks
    def handle_log(self, api, command, level=None):
        """
        Log a message at the specified severity level.

        The other log commands are identical except that ``level`` need not
        be specified. Using the log-level specific commands is preferred.

        Command fields:
            - ``level``: The severity level to log at. Must be an integer
              log level. Default severity is the ``INFO`` log level.
            - ``msg``: The message to log.

        Reply fields:
            - ``success``: ``true`` if the operation was successful, otherwise
              ``false``.

        Example:

        .. code-block:: javascript

            api.request(
                'log.log',
                {level: 20,
                 msg: 'Abandon ship!'},
                function(reply) {
                    api.log_info('New value: ' +
                                 reply.value);
                }
            );
        """
        level = command.get('level', level)
        if level is None:
            level = logging.INFO
        msg = command.get('msg')
        if msg is None:
            returnValue(self.reply(command, success=False,
                                   reason="Value expected for msg"))
        if not isinstance(msg, basestring):
            msg = str(msg)
        elif isinstance(msg, unicode):
            msg = msg.encode('utf-8')
        yield self.log(api, msg, level)
        returnValue(self.reply(command, success=True))

    def handle_debug(self, api, command):
        """
        Logs a message at the ``DEBUG`` log level.

        See :func:`handle_log` for details.
        """
        return self.handle_log(api, command, level=logging.DEBUG)

    def handle_info(self, api, command):
        """
        Logs a message at the ``INFO`` log level.

        See :func:`handle_log` for details.
        """
        return self.handle_log(api, command, level=logging.INFO)

    def handle_warning(self, api, command):
        """
        Logs a message at the ``WARNING`` log level.

        See :func:`handle_log` for details.
        """
        return self.handle_log(api, command, level=logging.WARNING)

    def handle_error(self, api, command):
        """
        Logs a message at the ``ERROR`` log level.

        See :func:`handle_log` for details.
        """
        return self.handle_log(api, command, level=logging.ERROR)

    def handle_critical(self, api, command):
        """
        Logs a message at the ``CRITICAL`` log level.

        See :func:`handle_log` for details.
        """
        return self.handle_log(api, command, level=logging.CRITICAL)
