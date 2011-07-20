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
            USSText = element.find("Response").find("OnUSSEvent").find("USSText").findtext("")
            messagedict['USSText'] = USSText
            for i in contextlist:
                messagedict[i[0]] = i[1]

        if messagedict.get('Type') == "USSReply":
            UserID = element.find("Request").find("UserID").findtext('')
            Password = element.find("Request").find("Password").findtext('')
            USSText = element.find("Request").find("USSText").findtext('')
            messagedict['UserID'] = UserID
            messagedict['Password'] = Password
            messagedict['USSText'] = USSText

        #############################################################

        return messagedict


    def build(self, messagedict):
        message = ElementTree.Element("Message")
        version = ElementTree.SubElement(message, "Version")
        version.set("Version","1.0")

        ##############  Conditional checks ##########################

        if messagedict.get("Type") == "USSReply":
            request = ElementTree.SubElement(message, "Request")
            request.set("Type", messagedict.get("Type"))
            request.set("SessionID", messagedict.get("SessionID", ""))
            request.set("Flags", messagedict.get("Flags", "0"))
            userid = ElementTree.SubElement(request, "UserID")
            userid.set("Orientation", "TR")
            userid.text = messagedict.get("UserID", "")
            password = ElementTree.SubElement(request, "Password")
            password.text = messagedict.get("Password", "")
            usstext = ElementTree.SubElement(request, "USSText")
            usstext.set("Type", "TEXT")
            usstext.text = messagedict.get("USSText", "")

        #############################################################

        return ElementTree.tostring(message)


