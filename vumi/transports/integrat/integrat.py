from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from vumi.utils import http_request, normalize_msisdn
from vumi.message import TransportUserMessage
from vumi.workers.integrat.utils import HigateXMLParser
from vumi.transports import Transport

hxg = HigateXMLParser()


class IntegratHttpResource(Resource):

    # TODO: if both open and new are sent by integrat,
    #       do we need to remove one of them?
    EVENT_TYPE_MAP = {
        'new': TransportUserMessage.SESSION_NEW,
        'open': TransportUserMessage.SESSION_NEW,
        'close': TransportUserMessage.SESSION_CLOSE,
        'resume': TransportUserMessage.SESSION_RESUME,
        }

    def __init__(self, publish_message):
        self.publish_message

    def render(self, request):
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        hxg_msg = hxg.parse(request.content.read())
        text = hxg_msg.get('USSText', '').strip()
        if hxg_msg['EventType'] == 'Request':
            if text == 'REQ':
                session_event = TransportUserMessage.SESSION_NEW
            else:
                session_event = TransportUserMessage.SESSION_RESUME
        else:
            event_type = hxg_msg['EventType'].lower()
            session_event = self.EVENT_TYPE_MAP.get(event_type,
                    TransportUserMessage.SESSION_RESUME)

        transport_metadata = {
            'session_id': hxg_msg['SessionID'],
            }
        self.publish_message(
            from_addr=normalize_msisdn(hxg_msg['MSISDN']),
            to_addr=normalize_msisdn(hxg_msg['ConnStr']),
            session_event=session_event,
            content=text,
            transport_metadata=transport_metadata,
            )
        return ''


class HealthResource(Resource):
    isLeaf = True

    def render(self, request):
        request.setResponseCode(http.OK)
        return 'OK'


class IntegratTransport(Transport):

    def validate_config(self):
        """
        Transport-specific config validation happens in here.
        """
        self.web_path = self.config['web_path']
        self.web_port = self.config['web_port']
        self.integrat_url = self.config['url']
        self.integrat_username = self.config['username']
        self.integrat_password = self.config['password']

    def setup_transport(self):
        """
        All transport_specific setup should happen in here.
        """
        self.web_resource = yield self.start_web_resources(
            [
                (IntegratHttpResource(self.publish_message), self.web_path),
                (HealthResource(), 'health'),
            ],
            self.web_port,
        )

    @inlineCallbacks
    def handle_outbound_message(self, message):
        text = message['content']
        if text is None:
            text = ''
        flags = '0'
        if message['session_event'] == message.SESSION_CLOSE:
            flags = '1'
        session_id = message['transport_metadata']['session_id']
        yield http_request(self.integrat_url, hxg.build({
            'Flags': flags,
            'SessionID': session_id,
            'Type': 'USSReply',
            'USSText': text,
            'Password': self.integrat_password,
            'UserID': self.integrat_username,
        }), headers={
            'Content-Type': ['text/xml; charset=utf-8']
        })
