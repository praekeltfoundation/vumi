from  xml.etree import ElementTree

class HigateXMLParser():

    def parse(self, xmlstring):
        element = ElementTree.fromstring(xmlstring)

        messagedict = {}
        try:
            responselist = element.find("Response").items()
            for i in responselist:
                messagedict[i[0]] = i[1]
        except Exception, e:
            pass
        try:
            requestlist = element.find("Request").items()
            for i in requestlist:
                messagedict[i[0]] = i[1]
        except Exception, e:
            pass

        ##############  Conditional checks ##########################

        if messagedict.get('Type') == "OnResult":
            resultlist = element.find("Response").find("OnResult").items()
            for i in resultlist:
                messagedict[i[0]] = i[1]

        if messagedict.get('Type') == "SendSMS":
            pass #TODO

        if messagedict.get('Type') == "OnReceiveSMS":
            receivelist = element.find("Response").find("OnReceiveSMS").items()
            hex = element.find("Response").find("OnReceiveSMS").find("Content").findtext("")
            messagedict['hex'] = hex
            for i in receivelist:
                messagedict[i[0]] = i[1]

        if messagedict.get('Type') == "OnOBSResponse":
            pass #TODO

        if messagedict.get('Type') == "OnLBSResponse":
            pass #TODO

        if messagedict.get('Type') == "OnUSSEvent":
            contextlist = element.find("Response").find("OnUSSEvent").find("USSContext").items()
            text = element.find("Response").find("OnUSSEvent").find("USSText").findtext("")
            messagedict['TEXT'] = text
            for i in contextlist:
                messagedict[i[0]] = i[1]

        if messagedict.get('Type') == "USSReply":
            pass #TODO

        #############################################################

        return messagedict
