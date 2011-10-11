from vumi.message import TransportUserMessage
from vumi.transports.httprpc.transport import HttpRpcTransport


class VodaMessHttpRpcTransport(HttpRpcTransport):

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


class VodacomMessagingResponse(object):
    def __init__(self, config):
        self.config = config
        self.context = ''
        self.freetext_option = None
        self.template_freetext_option_string = ('<option'
                ' command="1"'
                ' order="1"'
                ' callback="http://%(web_host)s%(web_path)s?'
                           'context=%(context)s"'
                ' display="False"'
                ' ></option>')
        self.option_list = []
        self.template_numbered_option_string = ('<option'
                ' command="%(order)s"'
                ' order="%(order)s"'
                ' callback="http://%(web_host)s%(web_path)s?'
                           'context=%(context)s"'
                ' display="True"'
                ' >%(text)s</option>')

    def set_headertext(self, headertext):
        self.headertext = headertext

    def set_context(self, context):
        """
        context is a unique identifier for the state that generated
        the message the user is responding to
        """
        self.context = context
        if self.freetext_option:
            self.accept_freetext()
        count = 0
        while count < len(self.option_list):
            self.option_list[count].update({'context': self.context})
            count += 1

    def add_option(self, text, order=None):
        self.freetext_option = None
        dict = {'text': str(text)}
        if order:
            dict['order'] = int(order)
        else:
            dict['order'] = len(self.option_list) + 1
        dict.update({
            'web_path': self.config['web_path'],
            'web_host': self.config['web_host'],
            'context': self.context})
        self.option_list.append(dict)

    def accept_freetext(self):
        self.option_list = []
        self.freetext_option = self.template_freetext_option_string % {
            'web_path': self.config['web_path'],
            'web_host': self.config['web_host'],
            'context': self.context}

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
