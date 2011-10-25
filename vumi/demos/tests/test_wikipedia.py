"""Tests for vumi.demos.wikipedia."""

from pkg_resources import resource_string

from twisted.trial.unittest import TestCase
from vumi.demos.wikipedia import OpenSearch, pretty_print_results


class WikipediaTestCase(TestCase):
    def setUp(self):
        self.sample_xml = resource_string(__name__, "wikipedia_sample.xml")

    def tearDown(self):
        pass

    def test_open_search_results(self):
        os = OpenSearch()
        results = os.parse_xml(self.sample_xml)
        self.assertTrue(len(results), 2)
        self.assertTrue(all(isinstance(result, dict) for result in results))
        self.assertEquals(results[0], {
            'text': 'Africa',
            'description': ''.join(["Africa is the world's second largest ",
                                    "and second most populous continent, ",
                                    "after Asia. "]),
            'url': 'http://en.wikipedia.org/wiki/Africa',
            'image': {
                'source': 'http://upload.wikimedia.org/wikipedia/commons/'
                          'thumb/8/86/Africa_%28orthographic_projection%29'
                          '.svg/50px-Africa_%28orthographic_projection%29'
                          '.svg.png',
                'width': '50',
                'height': '50'
            }
        })

    def test_pretty_print_results(self):
        results = OpenSearch().parse_xml(self.sample_xml)
        self.assertEquals(pretty_print_results(results), '\n'.join([
            '1. Africa',
            '2. Race and ethnicity in the United States Census',
            '3. African American',
            '4. African people',
        ]))
