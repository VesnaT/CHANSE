import unittest

from scripts.twitter_scraper import _create_query, SYSTEM_KEYWORDS, \
    VALUE_KEYWORDS


class TestTwitterScraper(unittest.TestCase):
    def test_create_query_2(self):
        queries = _create_query(2, SYSTEM_KEYWORDS, VALUE_KEYWORDS)
        self._assert_not_in(queries)
        query = "(algorithm OR artificial intelligence) (fair OR transparent)"
        self.assertEqual(queries[0], query)
        query = "(targeted) (control OR standard)"
        self.assertEqual(queries[-1], query)

    def test_create_query_3(self):
        queries = _create_query(3, SYSTEM_KEYWORDS, VALUE_KEYWORDS)
        self._assert_not_in(queries)
        query = "(algorithm OR artificial intelligence OR AI) " \
                "(fair OR transparent OR responsib)"
        self.assertEqual(queries[0], query)
        query = "(targeted) (control OR standard)"
        self.assertEqual(queries[-1], query)

    def test_create_query_4(self):
        queries = _create_query(4, SYSTEM_KEYWORDS, VALUE_KEYWORDS)
        self._assert_not_in(queries)
        query = "(algorithm OR artificial intelligence OR AI OR automated) " \
                "(fair OR transparent OR responsib OR competit)"
        self.assertEqual(queries[0], query)
        query = "(targeted) (control OR standard)"
        self.assertEqual(queries[-1], query)

    def test_create_query(self):
        queries = _create_query(13, SYSTEM_KEYWORDS, VALUE_KEYWORDS)
        self._assert_not_in(queries)
        query = "(computer vision OR digitalisation OR digitalization OR " \
                "information model OR sensor OR decision support OR " \
                "calculation OR smart OR electrification OR app OR targeted)" \
                " (supplement OR workload OR speed OR distribut OR risk OR " \
                "identity OR security OR safety OR flow OR seamlessness OR " \
                "control OR standard)"
        self.assertEqual(queries[-1], query)
        self.assertEqual(len(queries), 9)
        self.assertEqual(max(map(len, queries)), 367)

    def _assert_not_in(self, queries):
        for query in queries:
            self.assertNotIn("()", query)
            self.assertNotIn("OR OR", query)


if __name__ == '__main__':
    unittest.main()
