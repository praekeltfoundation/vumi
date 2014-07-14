# -*- test-case-name: vumi.transports.integrat.tests.test_utils -*-

from xml.etree import ElementTree


def safetext(element):
    return element.text or ''


class HigateXMLParser(object):

    def parse(self, xmlstring):
        element = ElementTree.fromstring(xmlstring)

        messagedict = {}
        try:
            responselist = element.find("Response").items()
            for i in responselist:
                messagedict[i[0]] = i[1]
        except Exception:
            pass
        try:
            requestlist = element.find("Request").items()
            for i in requestlist:
                messagedict[i[0]] = i[1]
        except Exception:
            pass

        ##############  Conditional checks ##########################

        if messagedict.get('Type') == "OnResult":
            resultlist = element.find("Response").find("OnResult").items()
            for i in resultlist:
                messagedict[i[0]] = i[1]

        if messagedict.get('Type') == "SendSMS":
            pass  # TODO

        if messagedict.get('Type') == "OnReceiveSMS":
            receivelist = element.find("Response").find("OnReceiveSMS").items()
            hex = safetext(element.find("Response").find("OnReceiveSMS").find(
                "Content"))
            messagedict['hex'] = hex
            for i in receivelist:
                messagedict[i[0]] = i[1]

        if messagedict.get('Type') == "OnOBSResponse":
            pass  # TODO

        if messagedict.get('Type') == "OnLBSResponse":
            pass  # TODO

        if messagedict.get('Type') == "OnUSSEvent":
            contextlist = element.find("Response").find("OnUSSEvent").find(
                "USSContext").items()
            if element.find("Response").find("OnUSSEvent").find(
                    "USSText") is not None:
                USSText = safetext(element.find("Response").find(
                        "OnUSSEvent").find("USSText"))

                messagedict['USSText'] = USSText

            messagedict['EventType'] = element.find("Response").find(
                "OnUSSEvent").attrib['Type']

            for i in contextlist:
                messagedict[i[0]] = i[1]

        if messagedict.get('Type') == "USSReply":
            UserID = safetext(element.find("Request").find("UserID"))
            Password = safetext(element.find("Request").find("Password"))
            USSText = safetext(element.find("Request").find("USSText"))
            messagedict['UserID'] = UserID
            messagedict['Password'] = Password
            messagedict['USSText'] = USSText

        #############################################################

        return messagedict

    def parse_response(self, xmlstring):
        element = ElementTree.fromstring(xmlstring)
        status_code = int(element.get('status_code'))
        if not status_code:
            return {}

        data = element.find('Data')
        error_elements = element.findall('Data/field')
        messagedict = {
            'status_code': status_code,
            'error': data.get('name'),
            'error_fields': [{f.get('name'): f.get('value')}
                                for f in error_elements],
        }

        return messagedict

    def build(self, messagedict):
        message = ElementTree.Element("Message")
        version = ElementTree.SubElement(message, "Version")
        version.set("Version", "1.0")

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
