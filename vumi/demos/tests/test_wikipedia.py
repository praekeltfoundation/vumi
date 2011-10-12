"""Tests for vumi.demos.wikipedia."""

import time

from twisted.trial.unittest import TestCase
from vumi.demos.wikipedia import (OpenSearch, pretty_print_results,
                                  SessionManager)
from vumi.tests.utils import FakeRedis


class WikipediaTestCase(TestCase):
    def setUp(self):
        self.sample_xml = """<?xml version="1.0"?>
        <SearchSuggestion xmlns="http://opensearch.org/searchsuggest2" version="2.0">
            <Query xml:space="preserve">africa</Query>
            <Section>
                <Item>
                    <Text xml:space="preserve">Africa</Text>
                    <Description xml:space="preserve">Africa is the world's second largest and second most populous continent, after Asia. </Description>
                    <Url xml:space="preserve">http://en.wikipedia.org/wiki/Africa</Url>
                    <Image source="http://upload.wikimedia.org/wikipedia/commons/thumb/8/86/Africa_%28orthographic_projection%29.svg/50px-Africa_%28orthographic_projection%29.svg.png" width="50" height="50"/>
                </Item>
                <Item>
                    <Text xml:space="preserve">Race and ethnicity in the United States Census</Text>
                    <Description xml:space="preserve">Race and ethnicity in the United States Census, as defined by the Federal Office of Management and Budget (OMB) and the United States Census Bureau, are self-identification data items in which residents choose the race or races with which they most closely identify, and indicate whether or not they are of Hispanic or Latino origin (ethnicity).
        	</Description>
                    <Url xml:space="preserve">http://en.wikipedia.org/wiki/Race_and_ethnicity_in_the_United_States_Census</Url>
                </Item>
                <Item>
                    <Text xml:space="preserve">African American</Text>
                    <Description xml:space="preserve">African Americans (also referred to as Black Americans or Afro-Americans, and formerly as American Negroes) are citizens or residents of the United States who have origins in any of the black populations of Africa. </Description>
                    <Url xml:space="preserve">http://en.wikipedia.org/wiki/African_American</Url>
                    <Image source="http://upload.wikimedia.org/wikipedia/commons/thumb/4/42/Jesse_Owens1.jpg/36px-Jesse_Owens1.jpg" width="36" height="49"/>
                </Item>
                <Item>
                    <Text xml:space="preserve">African people</Text>
                    <Description xml:space="preserve">African people refers to natives, inhabitants, or citizen of Africa and to people of African descent. </Description>
                    <Url xml:space="preserve">http://en.wikipedia.org/wiki/African_people</Url>
                </Item>
            </Section>
        </SearchSuggestion>"""

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


class SessionManagerTestCase(TestCase):
    def setUp(self):
        self.sm = SessionManager(db=0, prefix="test:")
        self.sm.r_server = FakeRedis()  # stub out redis

    def tearDown(self):
        self.sm.stop()

    def test_active_sessions(self):
        def get_sessions():
            return sorted(self.sm.active_sessions())

        def ids():
            return [x[0] for x in get_sessions()]

        self.assertEqual(ids(), [])
        self.sm.create_session("u1")
        self.assertEqual(ids(), ["u1"])
        self.sm.create_session("u2")
        self.assertEqual(ids(), ["u1", "u2"])

        s1, s2 = get_sessions()
        self.assertTrue(s1[1]['created_at'] < s2[1]['created_at'])

    def test_schedule_session_expiry(self):
        self.sm.max_session_length = 60.0
        self.sm.create_session("u1")

    def test_create_and_retreive_session(self):
        session = self.sm.create_session("u1")
        self.assertEqual(sorted(session.keys()), ['created_at'])
        self.assertTrue(time.time() - session['created_at'] < 10.0)
        loaded = self.sm.load_session("u1")
        self.assertEqual(loaded, session)

    def test_save_session(self):
        test_session = {"foo": 5, "bar": "baz"}
        self.sm.create_session("u1")
        self.sm.save_session("u1", test_session)
        session = self.sm.load_session("u1")
        self.assertTrue(session.pop('created_at') is not None)
        self.assertEqual(session, test_session)
