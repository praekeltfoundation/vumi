import re

from twisted.web.template import Element, renderer, XMLFile, flattenString
from twisted.python.filepath import FilePath

from vumi.utils import PkgResources


MXIT_RESOURCES = PkgResources(__name__)


class ResponseParser(object):

    HEADER_PATTERN = r'^(.*)[\r\n]{1,2}\d?'
    ITEM_PATTERN = r'^(\d+)\. (.+)$'

    def __init__(self, content):
        header_match = re.match(self.HEADER_PATTERN, content)
        if header_match:
            [self.header] = header_match.groups()
            self.items = re.findall(self.ITEM_PATTERN, content, re.MULTILINE)
        else:
            self.header = content
            self.items = []

    @classmethod
    def parse(cls, content):
        p = cls(content)
        return p.header, p.items


class MxitResponse(Element):
    loader = XMLFile(FilePath(MXIT_RESOURCES.path('templates/response.xml')))

    def __init__(self, message, loader=None):
        self.header, self.items = ResponseParser.parse(
            message['content'] or u'')
        super(MxitResponse, self).__init__(loader or self.loader)

    @renderer
    def render_header(self, request, tag):
        return tag(self.header)

    @renderer
    def render_body(self, request, tag):
        if not self.items:
            return ''
        return tag

    @renderer
    def render_item(self, request, tag):
        for index, text in self.items:
            yield tag.clone().fillSlots(index=str(index), text=text)

    def flatten(self):
        return flattenString(None, self)
