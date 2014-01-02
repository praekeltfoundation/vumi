from vumi.tests.helpers import VumiTestCase

from vumi.transports.mtn_nigeria import utils


class TestUtils(VumiTestCase):
    def test_xml_string_to_collection(self):
        self.assertEqual(
            utils.xml_string_to_collection('<a></a>'),
            ('a', ''))

        self.assertEqual(
            utils.xml_string_to_collection('<a><b>foo</b></a>'),
            ('a', [('b', 'foo')]))

        self.assertEqual(
            utils.xml_string_to_collection(
                '<a><b><c><d>foo</d><e>bar</e></c></b></a>'),
            ('a', [
                ('b', [
                    ('c', [
                        ('d', 'foo'),
                        ('e', 'bar')
                    ])
                ])
            ]))

    def test_collection_to_xml_string(self):
        self.assertEqual(
            utils.collection_to_xml_string('a', ''),
            '<a></a>')

        self.assertEqual(
            utils.collection_to_xml_string('a', [('b', 'foo')]),
            '<a><b>foo</b></a>')

        self.assertEqual(
            utils.collection_to_xml_string(
                'a', [
                    ('b', [
                        ('c', [
                            ('d', 'foo'),
                            ('e', 'bar')
                        ])
                    ])
                ]),
            '<a><b><c><d>foo</d><e>bar</e></c></b></a>')
