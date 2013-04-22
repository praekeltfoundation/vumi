# -*- test-case-name: vumi.transports.integrat.tests.test_integrat -*-

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from vumi.utils import http_request, normalize_msisdn
from vumi.message import TransportUserMessage
from vumi.transports.integrat.utils import HigateXMLParser
from vumi.transports import Transport

hxg = HigateXMLParser()


class IntegratHttpResource(Resource):
    isLeaf = True

    # map events to session event types
    EVENT_TYPE_MAP = {
        'open': TransportUserMessage.SESSION_NEW,
        'close': TransportUserMessage.SESSION_CLOSE,
        'resume': TransportUserMessage.SESSION_RESUME,
        }

    # Integrat sends both 'new' and 'open' events but
    # we only pass 'open' events on ('open' is the more
    # complete and reliable of the two in Integrat's case).
    EVENTS_TO_SKIP = set(['new'])

    def __init__(self, transport_name, transport_type, publish_message):
        self.transport_name = transport_name
        self.transport_type = transport_type
        self.publish_message = publish_message

    def render(self, request):
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        hxg_msg = hxg.parse(request.content.read())

        if hxg_msg.get('Type') != 'OnUSSEvent':
            # TODO: add support for non-USSD messages
            return ''

        text = hxg_msg.get('USSText', '').strip()

        if hxg_msg['EventType'] == 'Request':
            if text == 'REQ':
                # This indicates a new session event but Integrat
                # also sends a non-request message with type 'open'
                # below (and that is the one we use to trigger Vumi's
                # new session message.
                return ''
            else:
                session_event = TransportUserMessage.SESSION_RESUME
        else:
            event_type = hxg_msg['EventType'].lower()
            if event_type in self.EVENTS_TO_SKIP:
                return ''
            session_event = self.EVENT_TYPE_MAP.get(event_type,
                    TransportUserMessage.SESSION_RESUME)

        if session_event != TransportUserMessage.SESSION_RESUME:
            text = None

        transport_metadata = {
            'session_id': hxg_msg['SessionID'],
            }
        self.publish_message(
            from_addr=normalize_msisdn(hxg_msg['MSISDN']),
            to_addr=hxg_msg['ConnStr'],
            session_event=session_event,
            content=text,
            transport_name=self.transport_name,
            transport_type=self.transport_type,
            transport_metadata=transport_metadata,
            )
        return ''


class HealthResource(Resource):
    isLeaf = True

    def render(self, request):
        request.setResponseCode(http.OK)
        request.do_not_log = True
        return 'OK'


class IntegratTransport(Transport):
    """Integrat USSD transport over HTTP."""

    def validate_config(self):
        """
        Transport-specific config validation happens in here.
        """
        self.web_path = self.config['web_path']
        self.web_port = int(self.config['web_port'])
        self.integrat_url = self.config['url']
        self.integrat_username = self.config['username']
        self.integrat_password = self.config['password']
        self.transport_type = self.config.get('transport_type', 'ussd')

    @inlineCallbacks
    def setup_transport(self):
        """
        All transport_specific setup should happen in here.
        """
        self.web_resource = yield self.start_web_resources(
            [
                (IntegratHttpResource(self.transport_name,
                    self.transport_type, self.publish_message), self.web_path),
                (HealthResource(), 'health'),
            ],
            self.web_port,
        )

    @inlineCallbacks
    def teardown_transport(self):
        yield self.web_resource.loseConnection()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        text = message['content']
        if text is None:
            text = ''
        flags = '0'
        if message['session_event'] == message.SESSION_CLOSE:
            flags = '1'
        session_id = message['transport_metadata']['session_id']
        response = yield http_request(self.integrat_url, hxg.build({
            'Flags': flags,
            'SessionID': session_id,
            'Type': 'USSReply',
            'USSText': text,
            'Password': self.integrat_password,
            'UserID': self.integrat_username,
        }), headers={
            'Content-Type': ['text/xml; charset=utf-8']
        })
        error = hxg.parse_response(response)
        if not error:
            yield self.publish_ack(user_message_id=message['message_id'],
                    sent_message_id=message['message_id'])
        else:
            yield self.publish_nack(user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                reason=', '.join([': '.join(ef.items()[0])
                            for ef in error['error_fields']]))
