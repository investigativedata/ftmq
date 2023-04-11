from unittest import TestCase

from followthemoney import model

from ftmq.exceptions import ValidationError
from ftmq.query import Query
from ftmq.util import read_proxies


def get_proxies():
    proxies = []
    with open("tests/fixtures/ec_meetings.ftm.json", "rb") as f:
        proxies.extend(read_proxies(f))
    with open("tests/fixtures/eu_authorities.ftm.json") as f:
        proxies.extend(read_proxies(f))
    return proxies


class ProxyTestCase(TestCase):
    proxies = get_proxies()

    def test_proxy_filter_dataset(self):
        q = Query()
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(45189, len(result))

        q = q.where(dataset="eu_authorities")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(151, len(result))

    def test_proxy_filter_schema(self):
        q = Query().where(schema="Event")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(34975, len(result))

        q = Query().where(schema="Organization")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(7097, len(result))

        q = Query().where(schema="Organization", include_matchable=True)
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(7351, len(result))

        q = Query().where(schema="LegalEntity")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(0, len(result))

        q = Query().where(schema="LegalEntity", include_matchable=True)
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(8142, len(result))

        q = Query().where(schema="LegalEntity", include_descendants=True)
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(8142, len(result))

        q = Query().where(schema=model.get("Person"))
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(791, len(result))

        # invalid
        with self.assertRaises(ValidationError):
            q = Query().where(schema="Invalid schema")

    def test_proxy_filter_property(self):
        q = Query().where(prop="jurisdiction", value="eu")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(254, len(result))

        q = Query().where(prop="date", value="2022", operator="gte")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(3575, len(result))

        q = Query().where(prop="date", value="2022", operator="gt")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(3575, len(result))

        # chained same props as AND
        q = q.where(prop="date", value="2023", operator="lt")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(3499, len(result))

        q = Query().where(prop="date", value=2023, operator="gte")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(76, len(result))

        q = Query().where(prop="date", value=True, operator="null")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(34975, len(result))

        q = Query().where(prop="date", value=False, operator="null")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(34975, len(result))

    def test_proxy_filters_combined(self):
        q = Query().where(prop="jurisdiction", value="eu")
        q = q.where(schema="Event")
        result = list(filter(q.apply, self.proxies))
        self.assertEqual(0, len(result))
