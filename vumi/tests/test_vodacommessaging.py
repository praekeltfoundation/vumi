import re
from  xml.etree import ElementTree

from twisted.python import log
from twisted.trial.unittest import TestCase
from vumi.workers.httprpc.transport import VodacomMessagingResponse


class VodacomMessagingResponseTest(TestCase):
    '''
    '''

    def setUp(self):
        self.config = {
                'web_host': 'vumi.p.org',
                'web_path': '/api/v1/ussd/vmes/'}

    def tearDown(self):
        pass

    def stdXML(self, obj):
        string = ElementTree.tostring(ElementTree.fromstring(str(obj)))
        return re.sub(r'\n\s*', '', string)

    def testMakeEndMessage(self):
        vmr = VodacomMessagingResponse(self.config)
        vmr.set_headertext("Goodbye")
        ref = '''
            <request>
                <headertext>Goodbye</headertext>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

    def testMakeFreetextMessage(self):
        vmr = VodacomMessagingResponse(self.config)
        vmr.set_headertext("Please enter your name")
        vmr.accept_freetext()
        ref = '''
            <request>
                <headertext>Please enter your name</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        command="1"
                        display="False"
                        order="1" />
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))
        vmr.set_context('username')
        ref = '''
            <request>
                <headertext>Please enter your name</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context=username"
                        command="1"
                        display="False"
                        order="1" />
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

    def testMakeOptionMessage(self):
        vmr = VodacomMessagingResponse(self.config)
        vmr.set_headertext("Pick a card")
        vmr.accept_freetext()
        vmr.add_option("Ace of diamonds")
        vmr.add_option("2 of clubs")
        vmr.add_option("3 of hearts")
        ref = '''
            <request>
                <headertext>Pick a card</headertext>
                <options>
                    <option
                        command="1"
                        order="1"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        display="True"
                        >Ace of diamonds</option>
                    <option
                        command="2"
                        order="2"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        display="True"
                        >2 of clubs</option>
                    <option
                        command="3"
                        order="3"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        display="True"
                        >3 of hearts</option>
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

        vmr.accept_freetext()
        ref = '''
            <request>
                <headertext>Pick a card</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        command="1"
                        display="False"
                        order="1" />
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

        vmr.add_option("King of spades")
        vmr.add_option("Queen of diamonds")
        vmr.add_option("Joker")
        ref = '''
            <request>
                <headertext>Pick a card</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        command="1"
                        display="True"
                        order="1"
                        >King of spades</option>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        command="2"
                        display="True"
                        order="2"
                        >Queen of diamonds</option>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        command="3"
                        display="True"
                        order="3"
                        >Joker</option>
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

        vmr.set_context('pickcard')
        ref = '''
            <request>
                <headertext>Pick a card</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context=pickcard"
                        command="1"
                        display="True"
                        order="1"
                        >King of spades</option>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context=pickcard"
                        command="2"
                        display="True"
                        order="2"
                        >Queen of diamonds</option>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context=pickcard"
                        command="3"
                        display="True"
                        order="3"
                        >Joker</option>
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

