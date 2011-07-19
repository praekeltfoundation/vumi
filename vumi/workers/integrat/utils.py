from  xml.etree import ElementTree

class HigateXMLParser():

    def parse(self, xmlstring):
        element = ElementTree.fromstring(xmlstring)

        messagedict = {}
        responselist = element.find("Response").items()
        for i in responselist:
            messagedict[i[0]] = i[1]

        if messagedict.get('Type') == "OnResult":
            resultlist = element.find("Response").find("OnResult").items()
            for i in resultlist:
                messagedict[i[0]] = i[1]
            return messagedict

        if messagedict.get('Type') == "OnReceiveSMS":
            receivelist = element.find("Response").find("OnReceiveSMS").items()
            hex = element.find("Response").find("OnReceiveSMS").find("Content").findtext("")
            messagedict['hex'] = hex
            for i in receivelist:
                messagedict[i[0]] = i[1]
            return messagedict

        if messagedict.get('Type') == "OnUSSEvent":
            contextlist = element.find("Response").find("OnUSSEvent").find("USSContext").items()
            text = element.find("Response").find("OnUSSEvent").find("USSText").findtext("")
            messagedict['Text'] = text
            for i in contextlist:
                messagedict[i[0]] = i[1]
            return messagedict

        return messagedict
