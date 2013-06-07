from twisted.python.constants import Names, NamedConstant
from twisted.trial.unittest import TestCase

from vumi.transports.parlayx.xmlutil import (
    Namespace, QualifiedName, ElementMaker, LocalNamespace, split_qualified,
    gettext, tostring)



class NamespaceTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.xmlutil.Namespace`.
    """
    def test_str(self):
        """
        ``str(Namespace)`` produces the Namespace URI.
        """
        uri = 'http://example.com'
        self.assertEqual(uri, str(Namespace(uri)))


    def test_repr(self):
        """
        ``repr(Namespace)`` produces self-explanatory human-readable output.
        """
        self.assertEqual(
            '<Namespace uri=None prefix=None>',
            repr(Namespace(None)))
        self.assertEqual(
            "<Namespace uri='http://example.com' prefix=None>",
            repr(Namespace('http://example.com')))
        self.assertEqual(
            "<Namespace uri='http://example.com' prefix='ex'>",
            repr(Namespace('http://example.com', 'ex')))


    def test_equality(self):
        """
        Two `Namespace` instances created with the same values compare equal to
        one another.
        """
        self.assertEqual(
            Namespace('http://example.com'),
            Namespace('http://example.com'))
        self.assertEqual(
            Namespace('http://example.com', 'ex'),
            Namespace('http://example.com', 'ex'))
        self.assertNotEqual(
            Namespace('http://example.com'),
            Namespace('http://example.com', 'ex'))
        self.assertNotEqual(
            Namespace('http://example.com/'),
            Namespace('http://example.com'))


    def test_qualified_name(self):
        """
        `Namespace.__getattr__` produces qualified `QualifiedName` instances if
        `Namespace.__uri` is not `None`.
        """
        uri = 'http://example.com'
        ns = Namespace(uri)
        self.assertEqual(
            QualifiedName(uri, 'foo'),
            ns.foo)


    def test_local_name(self):
        """
        `Namespace.__getattr__` produces local `QualifiedName` instances if
        `Namespace.__uri` is `None`.
        """
        ns = Namespace(None)
        self.assertEqual(
            QualifiedName('foo'),
            ns.foo)



class QualifiedNameTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.xmlutil.QualifiedName`.
    """
    def test_repr(self):
        """
        ``repr(QualifiedName)`` produces self-explanatory human-readable
        output.
        """
        self.assertEqual(
            "<QualifiedName xmlns=None local='tag'>",
            repr(QualifiedName('tag')))
        self.assertEqual(
            "<QualifiedName xmlns='http://example.com' local='tag'>",
            repr(QualifiedName('http://example.com', 'tag')))


    def test_equality(self):
        """
        Two `QualifiedName` instances created with the same values compare
        equal to one another.
        """
        self.assertEqual(
            QualifiedName('tag'),
            QualifiedName('tag'))
        self.assertEqual(
            QualifiedName('http://example.com', 'tag'),
            QualifiedName('http://example.com', 'tag'))
        # Parameters are internally converted to Clark notation anyway.
        self.assertEqual(
            QualifiedName('http://example.com', 'tag'),
            QualifiedName('{http://example.com}tag'))
        self.assertNotEqual(
            QualifiedName('tag'),
            QualifiedName('http://example.com', 'tag'))
        self.assertNotEqual(
            QualifiedName('http://example.com/', 'tag'),
            QualifiedName('http://example.com', 'tag'))


    def test_element(self):
        """
        `QualifiedName` instances are callable and produce ElementTree
        elements.
        """
        qname = QualifiedName('tag')
        self.assertEqual(
            '<tag />',
            tostring(qname()))
        self.assertEqual(
            '<tag>hello</tag>',
            tostring(qname(u'hello')))
        self.assertEqual(
            '<tag key="value">hello</tag>',
            tostring(qname('hello', key='value')))
        self.assertEqual(
            '<tag key="value">hello</tag>',
            tostring(qname('hello', dict(key='value'))))



class ElementMakerTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.xmlutil.ElementMaker`.
    """
    def test_unknown_child_type(self):
        """
        `ElementMaker` instances raise `TypeError` when called with children of
        unmapped types.
        """
        E = ElementMaker()
        exc = self.assertRaises(TypeError, E, 'tag', None)
        self.assertEqual('Unknown child type: None', str(exc))


    def test_simple(self):
        """
        Calling `ElementMaker` instances produces ElementTree elements,
        children and attributes can be provided too.
        """
        E = ElementMaker()
        self.assertEqual(
            '<tag />',
            tostring(E('tag')))
        self.assertEqual(
            '<tag>hello</tag>',
            tostring(E('tag', 'hello')))
        self.assertEqual(
            '<tag key="value">hello</tag>',
            tostring(E('tag', 'hello', key='value')))
        self.assertEqual(
            '<tag key="value">hello</tag>',
            tostring(E('tag', 'hello', dict(key='value'))))


    def test_callable(self):
        """
        Providing a callable child will result in that child being called, with
        no arguments, to produce the actual child value.
        """
        E = ElementMaker()
        self.assertEqual(
            '<tag><child /></tag>',
            tostring(E('tag', LocalNamespace.child)))
        self.assertEqual(
            '<tag>hello</tag>',
            tostring(E('tag', lambda: 'hello')))


    def test_nested(self):
        """
        Children can themselves be ElementTree elements, resulting in nested
        elements.
        """
        E = ElementMaker()
        self.assertEqual(
            '<tag><child /></tag>',
            tostring(E('tag', E('child'))))
        self.assertEqual(
            '<tag><child>hello</child></tag>',
            tostring(E('tag', E('child', 'hello'))))
        self.assertEqual(
            '<tag><child key="value">hello</child></tag>',
            tostring(E('tag', E('child', 'hello', key='value'))))
        self.assertEqual(
            '<tag><child key="value">hello</child></tag>',
            tostring(E('tag', E('child', 'hello', dict(key='value')))))


    def test_namespaced(self):
        """
        Tags that are `QualifiedName` instances or use Clark notation produce
        namespaced XML elements.
        """
        E = ElementMaker()
        self.assertEqual(
            '<ns0:tag xmlns:ns0="http://example.com" />',
            tostring(E('{http://example.com}tag')))
        self.assertEqual(
            '<ns0:tag xmlns:ns0="http://example.com" />',
            tostring(QualifiedName('http://example.com', 'tag')()))
        ns = Namespace('http://example.com', 'ex')
        self.assertEqual(
            '<ex:tag xmlns:ex="http://example.com" />',
            tostring(ns.tag()))


    def test_namespaced_attributes(self):
        """
        XML attributes that are `QualifiedName` instances or use Clark notation
        produce namespaced XML element attributes.
        """
        ns = Namespace('http://example.com', 'ex')
        attrib = {ns.key: 'value'}
        self.assertEqual(
            '<ex:tag xmlns:ex="http://example.com" ex:key="value" />',
            tostring(ns.tag(attrib)))
        attrib = {'{http://example.com}key': 'value'}
        self.assertEqual(
            '<ex:tag xmlns:ex="http://example.com" ex:key="value" />',
            tostring(ns.tag(attrib)))


    def test_typemap(self):
        """
        Providing a type map to `ElementMaker` allows the caller to specify how
        to serialize types other than strings and dictionaries.
        """
        E = ElementMaker(typemap={
            float: lambda e, v: '%0.2f' % (v,),
            int: lambda e, v: LocalNamespace.int(str(v))})
        self.assertEqual(
            '<tag>2.50</tag>',
            tostring(E('tag', 2.5)))
        self.assertEqual(
            '<tag><int>42</int></tag>',
            tostring(E('tag', 42)))



class MetasyntacticVariables(Names):
    """
    Metasyntactic variable names.
    """
    Foo = NamedConstant()



class GetTextTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.xmlutil.gettext`.
    """
    def setUp(self):
        self.root = LocalNamespace.top(
            LocalNamespace.a('hello'),
            LocalNamespace.b('42'),
            LocalNamespace.c('Foo'),
            LocalNamespace.d,
            LocalNamespace.sub(
                LocalNamespace.e('world'),
                LocalNamespace.f))


    def test_simple(self):
        """
        Getting a sub-element with a `text` attribute returns the text as a
        `unicode` object.
        """
        res = gettext(self.root, u'a')
        self.assertIdentical(unicode, type(res))
        self.assertEquals(res, u'hello')

        res = gettext(self.root, u'sub/e')
        self.assertIdentical(unicode, type(res))
        self.assertEquals(res, u'world')


    def test_default(self):
        """
        Getting a sub-element without a `text` attribute, or attempting to get
        a sub-element that does not exist, results in the `default` parameter
        to `gettext` being used, defaulting to `None`.
        """
        self.assertIdentical(gettext(self.root, u'd'), None)
        self.assertEquals(gettext(self.root, u'd', default=42), 42)

        self.assertIdentical(gettext(self.root, u'sub/f'), None)
        res = gettext(self.root, u'sub/f', default='a')
        self.assertIdentical(str, type(res))
        self.assertEquals(res, 'a')

        self.assertIdentical(gettext(self.root, u'haha_what'), None)
        self.assertEquals(gettext(self.root, u'haha_what', default=42), 42)


    def test_parse(self):
        """
        Specifying a `parse` callable results in that being called to transform
        the element text.
        """
        self.assertEquals(
            42,
            gettext(self.root, u'b', parse=int))
        self.assertEquals(
            MetasyntacticVariables.Foo,
            gettext(self.root, u'c',
                    parse=MetasyntacticVariables.lookupByName))
        self.assertRaises(ValueError,
            gettext, self.root, u'c', parse=int)


    def test_parseWithDefault(self):
        """
        In the event that a default value is specified and a `parse` callable
        given, and the default value is used, the default value will be passed
        to the callable.
        """
        self.assertEquals(
            42,
            gettext(self.root, u'b', default=3.1415, parse=int))
        self.assertEquals(
            21,
            gettext(self.root, u'd', default=21, parse=int))
        self.assertRaises(ValueError,
            gettext, self.root, u'd', default='foo', parse=int)



class SplitQualifiedTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.xmlutil.split_qualified`.
    """
    def test_local(self):
        """
        `split_qualified` splits a local XML name into `None` and the tag name.
        """
        self.assertEquals((None, 'tag'), split_qualified('tag'))


    def test_qualified(self):
        """
        `split_qualified` splits a qualified XML name into a URI and the tag
        name.
        """
        self.assertEquals(
            ('http://example.com', 'tag'),
            split_qualified('{http://example.com}tag'))
