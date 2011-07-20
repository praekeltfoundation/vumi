import json

from twisted.python import log
from twisted.trial.unittest import TestCase
from vumi.workers.integrat.utils import *


class HigateXMLTestCases(TestCase):

    def setUp(self):
        self.dolog = True
        self.hxp = HigateXMLParser()

    def tearDown(self):
        del self.hxp

    def testOnResult(self):
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
        if self.dolog:
            log.msg("OnResult -> %s" % (repr(self.hxp.parse(OnResult_xml))))
        self.assertEquals(self.hxp.parse(OnResult_xml), OnResult_dict)


    def testOnReceiveSMS(self):
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
        if self.dolog:
            log.msg("OnReceiveSMS -> %s" % (repr(self.hxp.parse(OnReceiveSMS_xml))))
        self.assertEquals(self.hxp.parse(OnReceiveSMS_xml), OnReceiveSMS_dict)


    def testOnOBSResponse(self):
        OnOBSResponse_xml = '''
        <Message>
            <Version Version="1.0"/>
            <Response Type="OnOBSResponse" RefNo="123" SeqNo="1234568">
                <SystemID>Higate</SystemID>
                <UserID>LoginName</UserID>
                <Service>SERVICECODE</Service>
                <NetworkID>2</NetworkID>
                <Flags>32</Flags>
                <ResultCode>6</ResultCode>
                <ResultText>An exception occured in : setErrorVaribles : ControlException on control eventChargeValidation[ORA-0</ResultText>
                <OnOBSResponse Type="TEXT"></OnOBSResponse>
            </Response>
        </Message>
        '''
        OnOBSResponse_dict = {}
        if self.dolog:
            log.msg("OnOBSResponse -> %s" % (repr(self.hxp.parse(OnOBSResponse_xml))))
        self.assertEquals(self.hxp.parse(OnOBSResponse_xml), OnOBSResponse_dict)


    def testOnLBSResponse(self):
        OnLBSResponse_xml = '''
        <Message>
            <Version Version="1.0"/>
            <Response Type="OnLBSResponse" RefNo="123" SeqNo="548245219">
                <SystemID>Higate</SystemID>
                <UserID>LoginName</UserID>
                <Service>SERVICECODE</Service>
                <NetworkID>1</NetworkID>
                <Flags>4096</Flags>
                <ResultCode>4</ResultCode>
                <ResultText>Receipted</ResultText>
                <OnLBSResponse Type="XML">
                    <LBS>
                        <AuthRef/>
                        <SubService/>
                        <Result>
                            <Param>
                                <Lat>-25955564</Lat>
                                <Lon>28133442</Lon>
                                <Accuracy>High</Accuracy>
                                <DateTime>2009-01-27T13:17:28.000Z</DateTime>
                                <ERange>0</ERange>
                                <Zone>Vod:0:0</Zone>
                                <RC>0</RC>
                            </Param>
                        </Result>
                    </LBS>
                </OnLBSResponse>
            </Response>
        </Message>
        '''
        OnLBSResponse_dict = {}
        if self.dolog:
            log.msg("OnLBSResponse -> %s" % (repr(self.hxp.parse(OnLBSResponse_xml))))
        self.assertEquals(self.hxp.parse(OnLBSResponse_xml), OnLBSResponse_dict)


    def testOnUSSEvent(self):
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
        if self.dolog:
            log.msg("OnUSSEvent -> %s" % (repr(self.hxp.parse(OnUSSEvent_xml))))
        self.assertEquals(self.hxp.parse(OnUSSEvent_xml), OnUSSEvent_dict)


    def testUSSReply(self):
        USSReply_xml = '''
        <Message>
        <Version Version="1.0"/>
         <Request Type="USSReply" SessionID="223665" Flags="0">
               <UserID Orientation="TR">LoginName</UserID>
               <Password>xxxxxxxx</Password>
               <USSText Type="TEXT">Welcome the this USSD session</USSText>
         </Request>
        </Message>
        '''
        USSReply_dict = {}
        if self.dolog:
            log.msg("USSReply -> %s" % (repr(self.hxp.parse(USSReply_xml))))
        self.assertEquals(self.hxp.parse(USSReply_xml), USSReply_dict)


