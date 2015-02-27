from vumi.application.sandbox.resources.utils import SandboxResource


class OutboundResource(SandboxResource):
    """
    Resource that provides the ability to send outbound messages.

    Includes support for replying to the sender of the current message,
    replying to the group the current message was from and sending messages
    that aren't replies.
    """

    def handle_reply_to(self, api, command):
        """
        Sends a reply to the individual who sent a received message.

        Command fields:
            - ``content``: The body of the reply message.
            - ``in_reply_to``: The ``message id`` of the message being replied
              to.
            - ``continue_session``: Whether to continue the session (if any).
              Defaults to ``true``.

        Reply fields:
            - ``success``: ``true`` if the operation was successful, otherwise
              ``false``.

        Example:

        .. code-block:: javascript

            api.request(
                'outbound.reply_to',
                {content: 'Welcome!',
                 in_reply_to: '06233d4eede945a3803bf9f3b78069ec'},
                function(reply) { api.log_info('Reply sent: ' +
                                               reply.success); });
        """
        content = command['content']
        continue_session = command.get('continue_session', True)
        orig_msg = api.get_inbound_message(command['in_reply_to'])
        d = self.app_worker.reply_to(orig_msg, content,
                                     continue_session=continue_session)
        d.addCallback(lambda r: self.reply(command, success=True))
        return d

    def handle_reply_to_group(self, api, command):
        """
        Sends a reply to the group from which a received message was sent.

        Command fields:
            - ``content``: The body of the reply message.
            - ``in_reply_to``: The ``message id`` of the message being replied
              to.
            - ``continue_session``: Whether to continue the session (if any).
              Defaults to ``true``.

        Reply fields:
            - ``success``: ``true`` if the operation was successful, otherwise
              ``false``.

        Example:

        .. code-block:: javascript

            api.request(
                'outbound.reply_to_group',
                {content: 'Welcome!',
                 in_reply_to: '06233d4eede945a3803bf9f3b78069ec'},
                function(reply) { api.log_info('Reply to group sent: ' +
                                               reply.success); });
        """
        content = command['content']
        continue_session = command.get('continue_session', True)
        orig_msg = api.get_inbound_message(command['in_reply_to'])
        d = self.app_worker.reply_to_group(orig_msg, content,
                                           continue_session=continue_session)
        d.addCallback(lambda r: self.reply(command, success=True))
        return d

    def handle_send_to(self, api, command):
        """
        Sends a message to a specified address.

        Command fields:
            - ``content``: The body of the reply message.
            - ``to_addr``: The address of the recipient (e.g. an MSISDN).
            - ``endpoint``: The name of the endpoint to send the message via.
              Optional (default is ``"default"``).

        Reply fields:
            - ``success``: ``true`` if the operation was successful, otherwise
              ``false``.

        Example:

        .. code-block:: javascript

            api.request(
                'outbound.send_to',
                {content: 'Welcome!', to_addr: '+27831234567',
                 endpoint: 'default'},
                function(reply) { api.log_info('Message sent: ' +
                                               reply.success); });
        """
        content = command['content']
        to_addr = command['to_addr']
        endpoint = command.get('endpoint', 'default')
        d = self.app_worker.send_to(to_addr, content, endpoint=endpoint)
        d.addCallback(lambda r: self.reply(command, success=True))
        return d
