from collections import namedtuple
import xml.etree.ElementTree as ET

OPERA_TIMESTAMP_FORMAT = "%Y%m%dT%H:%M:%S"


def parse_receipts_xml(receipt_xml_data):
    tree = ET.fromstring(receipt_xml_data)
    return map(receipt_to_namedtuple, tree.findall('receipt'))


def receipt_element_to_dict(element):
    """
    Turn an ElementTree element '<data><el>1</el></data>' into {el: 1}.
    Not recursive!

    >>> data = ET.fromstring("<data><el>1</el></data>")
    >>> receipt_element_to_dict(data)
    {'el': '1'}
    >>>

    """
    return dict([(child.tag, child.text) for child in element.getchildren()])


def receipt_to_namedtuple(element):
    """
    Turn an ElementTree element into an object with named params.
    Not recursive!

    >>> data = ET.fromstring("<data><el>1</el></data>")
    >>> receipt_to_namedtuple(data)
    data(el='1')

    """
    d = receipt_element_to_dict(element)
    klass = namedtuple(element.tag, d.keys())
    return klass._make(d.values())


def parse_post_event_xml(post_event_xml_data):
    tree = ET.fromstring(post_event_xml_data)
    fields = tree.findall('field')
    return dict([(field.attrib['name'], field.text) for field in fields])
