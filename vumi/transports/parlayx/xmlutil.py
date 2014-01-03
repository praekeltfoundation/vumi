# -*- test-case-name: vumi.transports.parlayx.tests.test_xmlutil -*-
"""
XML convenience types and functions.

============
Introduction
============

In this domain-specific language, building on concepts from ``lxml.builder``,
the main goal is to improve the readability and structure of code that needs to
create XML documents programmatically, in particular XML that makes use of XML
namespaces. There are three main parts to consider in achieving this, starting
from the bottom working our way up.


-----------
1. Elements
-----------

`ElementMaker`, which `Element` is an instance of, is a basic XML element
factory that produces ElementTree element instances. The name (or tag) of the
element is specified as the first parameter to the `ElementMaker.element`
method. Children can be provided as positional parameters, a child is: text; an
ElementTree Element instance; a `dict` that will be applied as XML attributes
to the element; or a callable that returns any of the previous items.
Additionally, XML attributes can be passed as Python keyword arguments.

As a convenience, calling an `ElementMaker` instance is the same as invoking
the `ElementMaker.element` method.

    >>> from xml.etree.ElementTree import tostring
    >>> tostring(
    ... Element('parent', {'attr': 'value'},
    ...     Element('child1', 'content', attr2='value2')))
    '<parent attr="value"><child1 attr2="value2">content</child1></parent>'


------------------
2. Qualified names
------------------

`QualifedName` is a type that fills two roles: a way to represent a qualified
XML element name, either including a namespace or as a name in the local XML
namespace (element names that include a namespace are stored in Clark's
notation, e.g. ``{http://example.com}tag``); and an ElementTree element
factory.

As a convenience, calling an `QualifiedName` instance is the same as invoking
the `QualifiedName.element` method. While this bears some similarity to
`ElementMaker`, it plays an important role for `Namespace`.

    >>> from xml.etree.ElementTree import tostring
    >>> tostring(
    ... QualifiedName('{http://example.com}parent')(
    ...     QualifiedName('child1', 'content')))
    '<ns0:parent xmlns:ns0="http://example.com"><child1>content</child1>'
    '</ns0:parent>'


-------------
3. Namespaces
-------------

Again, `Namespace` fills two roles: a way to represent an XML namespace, with
an optional XML namespace prefix; and a `QualifiedName` factory.

Attribute access on a `Namespace` instance will produce a new `QualifiedName`
instance whose element name will be the name of the accessed attribute
qualified in the `Namespace`'s specified XML namespace. `LocalNamespace` is
a convenience for ``Namespace(None)``, which produces `QualifiedName` instances
in the local XML namespace.

    >>> from xml.etree.ElementTree import tostring
    >>> NS = Namespace('http://example.com', 'ex')
    >>> tostring(
    ... NS.parent({'attr': 'value'},
    ...     NS.child1('content'),
    ...     LocalNamespace.child2('content2')))
    '<ex:parent xmlns:ex="http://example.com" attr="value">'
    '<ex:child1>content</ex:child1><child2>content2</child2></ex:parent>'

XML attributes may be qualified too:

    >>> from xml.etree.ElementTree import tostring
    >>> NS = Namespace('http://example.com', 'ex')
    >>> tostring(
    ... NS.parent({NS.attr: 'value'}))
    '<ex:parent xmlns:ex="http://example.com" ex:attr="value" />'
"""
from collections import defaultdict
from xml.etree import ElementTree as etree

try:
    from xml.etree.ElementTree import register_namespace
    register_namespace  # For Pyflakes.
except ImportError:
    # This doesn't exist before Python 2.7, see
    # http://effbot.org/zone/element-namespaces.htm#element-tree-representation

    def register_namespace(prefix, uri):
        etree._namespace_map[uri] = prefix

try:
    from xml.etree.ElementTree import ParseError
    ParseError  # For Pyflakes.
except ImportError:
    from xml.parsers.expat import ExpatError as ParseError
    ParseError  # For Pyflakes.


class Namespace(object):
    """
    XML namespace.

    Attribute access on `Namespace` instances will produce `QualifiedName`
    instances in this XML namespace. If `uri` is `None`, the names will be in
    the XML local namespace.

    :ivar __uri: XML namespace URI, or `None` for the local namespace.
    :ivar __prefix: XML namespace prefix, or `None` for no predefined prefix.
    """
    def __init__(self, uri, prefix=None):
        # We want to avoid polluting the instance dict as much as possible,
        # since attribute access is how we produce QualifiedNames.
        self.__uri = uri
        self.__prefix = prefix
        if self.__prefix is not None:
            register_namespace(self.__prefix, self.__uri)

    def __str__(self):
        return self.__uri

    def __repr__(self):
        return '<%s uri=%r prefix=%r>' % (
            type(self).__name__, self.__uri, self.__prefix)

    def __eq__(self, other):
        if not isinstance(other, Namespace):
            return False
        return other.__uri == self.__uri and other.__prefix == self.__prefix

    def __getattr__(self, tag):
        if self.__uri is None:
            qname = QualifiedName(tag)
        else:
            qname = QualifiedName(self.__uri, tag)
        # Put this into the instance dict, to avoid doing this again for the
        # same result.
        setattr(self, tag, qname)
        return qname


class QualifiedName(etree.QName, object):
    """
    A qualified XML name.

    As a convenience, calling a `QualifiedName` instance is the same as
    invoking `QualifiedName.element` on an instance.

    :ivar text: Qualified name in Clark's notation.
    """
    def __repr__(self):
        xmlns, local = split_qualified(self.text)
        return '<%s xmlns=%r local=%r>' % (type(self).__name__, xmlns, local)

    def __eq__(self, other):
        if not isinstance(other, etree.QName):
            return False
        return other.text == self.text

    def element(self, *children, **attrib):
        """
        Create an ElementTree element.

        The element tag name will be this qualified name's
        `QualifiedName.text` value.

        :param *children: Child content or elements.
        :param **attrib: Element XML attributes.
        :return: ElementTree element.
        """
        return Element(self.text, *children, **attrib)

    __call__ = element


class ElementMaker(object):
    """
    An ElementTree element factory.

    As a convenience, calling an `ElementMaker` instance is the same as
    invoking `ElementMaker.element` on an instance.
    """
    def __init__(self, typemap=None):
        """
        :param typemap:
            Mapping of Python types to callables, taking an ElementTree element
            and some child value. This map will be consulted in
            `ElementMaker.element` for child items.
        """
        self._makeelement = etree.Element
        self._typemap = {
            list: self._add_children,
            dict: self._set_attributes,
            unicode: self._add_text,
            str: self._add_text}
        if typemap is not None:
            self._typemap.update(typemap)

    def _add_children(self, elem, children):
        """
        Add children to an element.
        """
        for child in children:
            self._handle_child(elem, child)

    def _set_attributes(self, elem, attrib):
        """
        Set XML attributes on an element.

        :param elem: Parent ElementTree element.

        :param attrib:
            Mapping of text attribute names, or `xml.etree.ElementTree.QName`
            instances, to attribute values.
        """
        for k, v in attrib.items():
            # XXX: Do something smarter with k and v? lxml does some
            # transformation stuff.
            elem.set(k, v)

    def _add_text(self, elem, text):
        """
        Add text content to an element.

        :param elem: Parent ElementTree element.

        :param text: Text content to add.
        """
        # If the element has any children we need to add the text to the
        # tail.
        if len(elem):
            elem[-1] = (elem[-1].tail or '') + text
        else:
            elem.text = (elem.text or '') + text

    def _handle_child(self, parent, child):
        """
        Add a child element to a parent element.

        Child elements can be any of the following:

        * A callable, that will be called with no parameters;
        * An ElementTree element;
        * `str` or `unicode` text content;
        * A `list` containing any of the above.
        """
        if callable(child):
            child = child()
        t = self._typemap.get(type(child))
        if t is None:
            if etree.iselement(child):
                parent.append(child)
                return
            raise TypeError('Unknown child type: %r' % (child,))

        v = t(parent, child)
        if v is not None:
            self._handle_child(parent, v)

    def element(self, tag, *children, **attrib):
        """
        Create an ElementTree element.

        :param tag: Tag name or `QualifiedName` instance.
        :param *children: Child content or elements.
        :param **attrib: Element XML attributes.
        :return: ElementTree element.
        """
        if isinstance(tag, etree.QName):
            tag = tag.text

        elem = self._makeelement(tag)

        if attrib:
            self._set_attributes(elem, attrib)

        for child in children:
            self._handle_child(elem, child)

        return elem

    __call__ = element


def elemfind(elem, path):
    """
    Helper version of `xml.etree.ElementTree.Element.find` that understands
    `xml.etree.ElementTree.QName`.
    """
    return next(iter(elemfindall(elem, path)), None)


def elemfindall(elem, path):
    """
    Helper version of `xml.etree.ElementTree.Element.findall` that understands
    `xml.etree.ElementTree.QName`.
    """
    if isinstance(path, etree.QName):
        path = path.text
    return elem.findall(path)


def split_qualified(fqname):
    """
    Split a fully qualified element name, in Clark's notation, into its URI and
    local name components.

    :param fqname: Fully qualified name in Clark's notation.
    :return: 2-tuple containing the namespace URI and local tag name.
    """
    if fqname and fqname[0] == '{':
        return tuple(fqname[1:].split('}'))
    return None, fqname


def gettext(elem, path, default=None, parse=None):
    """
    Get the text of an `ElementTree` element and optionally transform it.

    If `default` and `parse` are not `None`, `parse` will be called with
    `default`.

    :param elem:
        ElementTree element to find `path` on.

    :type path: unicode
    :param path:
        Path to the sub-element.

    :param default:
        A default value to use if the `text` attribute on the found element
        is `None`, or the element is not found; defaults to `None`.

    :type  parse: callable
    :param parse:
        A callable to transform the found element's text.
    """
    return next(gettextall(elem, path, default, parse), default)


def gettextall(elem, path, default=None, parse=None):
    """
    Get the text of all matching `ElementTree` elements and optionally
    transform them.

    If `default` and `parse` are not `None`, `parse` will be called with
    `default`.

    :param elem:
        ElementTree element to find `path` on.

    :type path: unicode
    :param path:
        Path to the sub-elements.

    :param default:
        A default value to use if the `text` attribute found on an element
        is `None`, or an element is not found; defaults to `None`.

    :type  parse: callable
    :param parse:
        A callable to transform the found element's text.
    """
    es = elemfindall(elem, path)
    for e in es:
        if e is None or e.text is None:
            result = default
        else:
            result = unicode(e.text).strip()

        if result is not None and parse is not None:
            result = parse(result)

        yield result


def element_to_dict(root):
    """
    Convert an ElementTree element into a dictionary structure.

    Text content is stored against a special key, ``#text``, unless the element
    contains only text and no attributes.

    Attributes are converted into dictionaries of the attribute name, prefixed
    with ``@``, keyed against the attribute value, which are keyed against the
    root element's name.

    Child elements are recursively turned into dictionaries. Child elements
    with the same name are coalesced into a ``list``.

    :param root: ElementTree element root to convert into a ``dict``.
    :return: ``dict`` representation of `root`.
    """
    d = {root.tag: {} if root.attrib else None}
    children = root.getchildren()
    if children:
        dd = defaultdict(list)
        for child_dict in map(element_to_dict, children):
            for k, v in child_dict.iteritems():
                dd[k].append(v)
        d = {root.tag: dict((k, v[0] if len(v) == 1 else v)
                            for k, v in dd.iteritems())}

    if root.attrib:
        d[root.tag].update(
            ('@' + str(k), v) for k, v in root.attrib.iteritems())

    if root.text:
        text = root.text.strip()
        if children or root.attrib:
            if text:
                d[root.tag]['#text'] = text
        else:
            d[root.tag] = text

    return d


LocalNamespace = Namespace(None)
Element = ElementMaker()
parse_document = etree.parse
fromstring = etree.fromstring
tostring = etree.tostring


__all__ = [
    'Namespace', 'QualifiedName', 'ElementMaker', 'elemfind', 'elemfindall',
    'split_qualified', 'gettext', 'gettextall', 'LocalNamespace', 'Element',
    'parse_document', 'fromstring', 'tostring', 'element_to_dict']
