# -*- test-case-name: vumi.transports.mtech_ussd.tests.test_mtech_ussd -*-

from xml.etree import ElementTree as ET

import redis

from vumi.message import TransportUserMessage
from vumi.transports.httprpc import HttpRpcTransport
from vumi.application.session import SessionManager


class MtechUssdTransport(HttpRpcTransport):

    def setup_transport(self):
        super(MtechUssdTransport, self).setup_transport()
        self.redis_config = self.config.get('redis', {})
        self.r_prefix = "mtech_ussd:%s" % self.transport_name
        session_timeout = int(self.config.get("ussd_session_timeout", 600))
        self.r_server = self.connect_to_redis()
        self.session_manager = SessionManager(
            self.r_server, self.r_prefix, max_session_length=session_timeout)

    def teardown_transport(self):
        self.session_manager.stop()
        super(MtechUssdTransport, self).teardown_transport()

    def connect_to_redis(self):
        return redis.Redis(**self.redis_config)

    def save_session(self, session_id, from_addr, to_addr):
        return self.session_manager.create_session(
            session_id, from_addr=from_addr, to_addr=to_addr)

    def handle_raw_inbound_message(self, msgid, request):
        body = ET.fromstring(request.content.read())

        # We always get these.
        session_id = body.find('session_id').text
        page_id = body.find('page_id').text
        content = body.find('data').text

        if page_id == '0':
            # This is a new session.
            session = self.save_session(
                session_id,
                from_addr=body.find('mobile_number').text,
                to_addr=body.find('gate').text)  # ???
            session_event = TransportUserMessage.SESSION_NEW
        else:
            # This is an existing session.
            session = self.session_manager.load_session(session_id)
            session_event = TransportUserMessage.SESSION_RESUME

        transport_metadata = {'session_id': session_id}
        self.publish_message(
                message_id=msgid,
                content=content,
                to_addr=session['to_addr'],
                from_addr=session['from_addr'],
                session_event=session_event,
                transport_name=self.transport_name,
                transport_type=self.config.get('transport_type'),
                transport_metadata=transport_metadata,
                )

    def handle_outbound_message(self, message):
        mur = MtechUssdResponse(message['transport_metadata']['session_id'])
        mur.add_text(message['content'])
        if message['session_event'] != TransportUserMessage.SESSION_CLOSE:
            mur.add_freetext_option()
        self.finish_request(message['in_reply_to'],
                            unicode(mur).encode('utf-8'))


class MtechUssdResponse(object):
    def __init__(self, session_id):
        self.session_id = session_id
        self.title = None
        self.text = []
        self.nav = []

    def add_title(self, title):
        self.title = title

    def add_text(self, text):
        self.text.append(text)

    def add_menu_item(self, text, option):
        self.nav.append({
                'text': text,
                'pageId': 'index%s' % (option,),
                'accesskey': option,
                })

    def add_freetext_option(self):
        self.nav.append({'text': None, 'pageId': 'indexX', 'accesskey': '*'})

    def to_xml(self):
        page = ET.fromstring('<page version="2.0" />')
        ET.SubElement(page, "session_id").text = self.session_id

        if self.title is not None:
            ET.SubElement(page, "title").text = self.title

        for text in self.text:
            lines = text.split('\n')
            div = ET.SubElement(page, "div")
            div.text = lines.pop(0)
            for line in lines:
                ET.SubElement(div, "br").tail = line

        if self.nav:
            nav = ET.SubElement(page, "navigation")
            for link in self.nav:
                ET.SubElement(
                    nav, "link", pageId=link['pageId'],
                    accesskey=link['accesskey']).text = link['text']

        return ET.tostring(page, encoding="UTF-8")

    def __str__(self):
        return self.to_xml()
