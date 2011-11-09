# -*- test-case-name: vumi.transports.vodacom_messaging.tests.test_vodacom_messaging -*-

from twisted.python import log

from vumi.message import TransportUserMessage
from vumi.transports.httprpc import HttpRpcTransport


class VodacomMessagingTransport(HttpRpcTransport):

    def handle_raw_inbound_message(self, msgid, request):
        content = str(request.args.get('request', [None])[0])
        msisdn = str(request.args.get('msisdn', [None])[0])
        ussd_session_id = str(request.args.get('ussdSessionId', [None])[0])
        provider = str(request.args.get('provider', [None])[0])
        if content.startswith(self.config.get('ussd_string_prefix')):
            session_event = TransportUserMessage.SESSION_NEW
            to_addr = content
        else:
            session_event = TransportUserMessage.SESSION_RESUME
            to_addr = ''
        transport_metadata = {'session_id': ussd_session_id}
        self.publish_message(
                message_id=msgid,
                content=content,
                to_addr=to_addr,
                from_addr=msisdn,
                provider=provider,
                session_event=session_event,
                transport_name=self.transport_name,
                transport_type=self.config.get('transport_type'),
                transport_metadata=transport_metadata,
                )

    def finishRequest(self, uuid, content, session_event):
        vmr = VodacomMessagingResponse(
                self.config['web_host'],
                self.config['web_path'])
        vmr.set_headertext(str(content))
        if session_event is None:
            vmr.accept_freetext()
        data = str(vmr)

        log.msg("VodacomMessagingTransport.finishRequest with data:", repr(data))
        log.msg(repr(self.requests))
        request = self.requests.get(uuid)
        if request:
            request.write(data)
            request.finish()
            del self.requests[uuid]


class VodacomMessagingResponse(object):
    def __init__(self, web_host, web_path):
        self.web_host = web_host
        self.web_path = web_path
        self.freetext_option = None
        self.template_freetext_option_string = ('<option'
                ' command="1"'
                ' order="1"'
                ' callback="http://%(web_host)s%(web_path)s"'
                ' display="False"'
                ' ></option>')
        self.option_list = []
        self.template_numbered_option_string = ('<option'
                ' command="%(order)s"'
                ' order="%(order)s"'
                ' callback="http://%(web_host)s%(web_path)s"'
                ' display="True"'
                ' >%(text)s</option>')

    def set_headertext(self, headertext):
        self.headertext = headertext

    def add_option(self, text, order=None):
        self.freetext_option = None
        dict = {'text': str(text)}
        if order:
            dict['order'] = int(order)
        else:
            dict['order'] = len(self.option_list) + 1
        dict.update({
            'web_path': self.web_path,
            'web_host': self.web_host})
        self.option_list.append(dict)

    def accept_freetext(self):
        self.option_list = []
        self.freetext_option = self.template_freetext_option_string % {
            'web_path': self.web_path,
            'web_host': self.web_host}

    def __str__(self):
        headertext = '\t<headertext>%s</headertext>\n' % self.headertext
        options = ''
        if self.freetext_option or len(self.option_list) > 0:
            options = '\t<options>\n'
            for o in self.option_list:
                options += ('\t\t' + self.template_numbered_option_string % o
                            + '\n')
            if self.freetext_option:
                options += '\t\t' + self.freetext_option + '\n'
            options += '\t</options>\n'
        response = '<request>\n' + headertext + options + '</request>'
        return response
