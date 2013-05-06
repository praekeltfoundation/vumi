# -*- test-case-name: vumi.transports.api.tests.test_oldapi -*-
import json
import re
from base64 import b64decode

from twisted.python import log
from twisted.web import http

from vumi.transports.httprpc import HttpRpcTransport


class OldSimpleHttpTransport(HttpRpcTransport):
    """
    Maintains the API used by the old Django based
    method of loading SMS's into VUMI over HTTP

    Configuration options:

    web_path : str
        The path relative to the host where this listens
    web_port : int
        The port this listens on
    transport_name : str
        The name this transport instance will use to create it's queues
    identities : dictionary
        user : str
        password : str
        default_transport : str
    """

    def validate_config(self):
        super(OldSimpleHttpTransport, self).validate_config()
        self.identities = self.config.get('identities', {})

    def get_health_response(self):
        return json.dumps({})

    def get_credentials(self, request):
        auth_header = 'Authorization'
        headers = request.requestHeaders
        if headers.hasHeader(auth_header):
            auth = headers.getRawHeaders(auth_header)[0]
            creds = b64decode(auth.split(' ')[-1])
            return creds.split(':')
        else:
            return '', ''

    def is_authorized(self, username, password):
        return self.identities.get(username) == password

    def handle_outbound_message(self, message):
        log.msg("OldSimpleHttpTransport consuming %s" % (message))
        return self.publish_ack(user_message_id=message['message_id'],
            sent_message_id=message['message_id'])

    def check_authorization(self, request):
        username, password = self.get_credentials(request)
        if self.identities and not self.is_authorized(username, password):
            return False, username
        return True, username

    def handle_raw_inbound_message(self, request_id, request):
        authorized, username = self.check_authorization(request)
        if not authorized:
            return self.finish_request(request_id, 'Not Authorized',
                                        code=http.UNAUTHORIZED)
        message = request.args.get('message', [None])[0]
        to_msisdns = request.args.get('to_msisdn', [])
        from_msisdn = request.args.get('from_msisdn', [None])[0]
        return_list = []
        for to_msisdn in to_msisdns:
            message_id = self.generate_message_id()
            content = message
            to_addr = to_msisdn
            from_addr = from_msisdn
            log.msg(
                'OldSimpleHttpTransport sending from %s to %s message "%s"' % (
                    from_addr, to_addr, content))

            self.publish_message(
                message_id=message_id,
                content=content,
                to_addr=to_addr,
                from_addr=from_addr,
                provider='vumi',
                transport_type='old_simple_http',
                transport_metadata={
                    'http_user': username,
                }
            )
            return_list.append({
                "message": message,
                "to_msisdn": to_msisdn,
                "from_msisdn": from_msisdn,
                "id": message_id,
                })
        return self.finish_request(request_id, json.dumps(return_list))


class OldTemplateHttpTransport(OldSimpleHttpTransport):

    def handle_outbound_message(self, message):
        log.msg("OldTemplateHttpTransport consuming %s" % (message))

    def extract_template_args(self, args, length):
        template_args = []
        for i in range(length):
            template_args.append({})
        for k, v in args.items():
            if k.startswith("template_"):
                for i, x in enumerate(v):
                    template_args[i][k] = x
        return template_args

    def handle_raw_inbound_message(self, request_id, request):
        authorized, username = self.check_authorization(request)
        if not authorized:
            return self.finish_request(request_id, 'Not Authorized',
                                        code=http.UNAUTHORIZED)
        opener = re.compile('{{ *')
        closer = re.compile(' *}}')
        template = request.args.get('template', [None])[0]
        template = opener.sub('%(template_', template)
        template = closer.sub(')s', template)
        to_msisdns = request.args.get('to_msisdn', [])
        from_msisdn = request.args.get('from_msisdn', [None])[0]
        template_args = self.extract_template_args(request.args,
                                                   len(to_msisdns))
        return_list = []
        for i, to_msisdn in enumerate(to_msisdns):
            message_id = self.generate_message_id()
            message = content = template % template_args[i]
            to_addr = to_msisdn
            from_addr = from_msisdn
            log.msg(('OldTemplateHttpTransport sending from %s to %s '
                     'message "%s"') % (from_addr, to_addr, content))
            self.publish_message(
                message_id=message_id,
                content=content,
                to_addr=to_addr,
                from_addr=from_addr,
                provider='vumi',
                transport_type='old_template_http',
                transport_metadata={
                    'http_user': username,
                }
            )
            return_list.append({
                "message": message,
                "to_msisdn": to_msisdn,
                "from_msisdn": from_msisdn,
                "id": message_id,
                })
        return self.finish_request(request_id, json.dumps(return_list))
