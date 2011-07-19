import json

from twisted.trial.unittest import TestCase
from vumi.workers.integrat.utils import *


class HigateTestCase(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_higate_xml_parser(self):
        hxp = HigateXMLParser()

        OnUSSEvent_xml = '''
        <Message>
            <Version Version="1.0"/>
            <Response Type="OnUSSEvent">
                <SystemID>Higate</SystemID>
                <UserID>LoginName</UserID>
                <Service>SERVICECODE</Service>
                <Network ID="1" MCC="655" MNC="001"/>
                <OnUSSEvent Type="Request">
                    <USSContext SessionID="16502" NetworkSID="310941653" MSISDN="27821234567" Script="testscript" ConnStr="*120*99*123#"/>
                    <USSText Type="TEXT">REQ</USSText>
                </OnUSSEvent>
            </Response>
        </Message>
        '''

        OnUSSEvent_dict = {'Script': 'testscript', 'MSISDN': '27821234567', 'Text': 'REQ', 'NetworkSID': '310941653', 'ConnStr': '*120*99*123#', 'SessionID': '16502', 'Type': 'OnUSSEvent'}

        print hxp.parse(OnUSSEvent_xml)
        self.assertEquals(hxp.parse(OnUSSEvent_xml), OnUSSEvent_dict)


        OnReceiveSMS_xml = '''
        <Message>
            <Version Version="1.0"/>
            <Response Type="OnReceiveSMS">
                <SystemID>Higate</SystemID>
                <UserID>Client1</UserID>
                <Service>SRC0123</Service>
                <Network ID="1" MCC="655" MNC="001"/>
                <OnReceiveSMS SeqNo="576674646"
                              Sent="20100614135709"
                              FromAddr="27829023456"
                              ToAddr="27829020203777"
                              ToTag="777"
                              Value="0"
                              NetworkID="1"
                              AdultRating="0"
                              DataCoding="8"
                              EsmClass="128" >
                    <Content Type="HEX">06052677F6A565 ...etc</Content>
                </OnReceiveSMS>
            </Response>
        </Message>
        '''

        OnReceiveSMS_dict = {'NetworkID': '1', 'FromAddr': '27829023456', 'SeqNo': '576674646', 'AdultRating': '0', 'hex': '06052677F6A565 ...etc', 'Value': '0', 'ToTag': '777', 'ToAddr': '27829020203777', 'EsmClass': '128', 'DataCoding': '8', 'Type': 'OnReceiveSMS', 'Sent': '20100614135709'}

        print hxp.parse(OnReceiveSMS_xml)
        self.assertEquals(hxp.parse(OnReceiveSMS_xml), OnReceiveSMS_dict)

        OnResult_xml = '''
        <Message>
            <Version Version="1.0" />
            <Response Type="OnResult"
                      TOC="SMS"
                      RefNo="2313344"
                      SeqNo="8199250">
                <SystemID>Higate</SystemID>
                <UserID>Http001</UserID>
                <Service>HC001</Service>
                <NetworkID>1</NetworkID>
                <OnResult Flags="0"
                          Code="3"
                          SubCode="0"
                          Text="Acknowledged" />
            </Response>
        </Message>
        '''

        OnResult_dict = {'Code': '3', 'Text': 'Acknowledged', 'SubCode': '0', 'RefNo': '2313344', 'Flags': '0', 'SeqNo': '8199250', 'TOC': 'SMS', 'Type': 'OnResult'}

        print hxp.parse(OnResult_xml)
        self.assertEquals(hxp.parse(OnResult_xml), OnResult_dict)


