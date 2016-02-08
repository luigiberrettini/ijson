import asyncio
import functools
import unittest

from ijson.backends import asyncio as asyncio_backend
from .test_common import (
    INCOMPLETE_JSONS,
    INT_NUMBERS_JSON,
    INVALID_JSONS,
    JSON,
    JSON_EVENTS,
    SCALAR_JSON,
    STRINGS_JSON,
)


def run_in_loop(f):
    @functools.wraps(f)
    def wrapper(testcase, *args, **kwargs):
        coro = asyncio.coroutine(f)
        future = asyncio.wait_for(coro(testcase, *args, **kwargs), timeout=5)
        return testcase.loop.run_until_complete(future)
    return wrapper


class MetaAioTestCase(type):

    def __new__(cls, name, bases, attrs):
        for key, obj in attrs.items():
            if key.startswith('test_'):
                attrs[key] = run_in_loop(obj)
        return super().__new__(cls, name, bases, attrs)


class AsyncReadable(object):

    def __init__(self, file):
        self.data = file

    @asyncio.coroutine
    def read(self, size=None):
        data = self.data.read(size)
        return data


class AsyncioParse(unittest.TestCase):

    backend = asyncio_backend

    def setUp(self):
        if self.backend is None:
            raise unittest.SkipTest('asyncio support required')
        self.loop = asyncio.get_event_loop()

    def tearDown(self):
        self.loop.close()

    @asyncio.coroutine
    def list(self, coro):
        items = []
        while True:
            try:
                item = yield from coro.next()
            except self.backend.StopAsyncIteration:
                break
            else:
                items.append(item)
        return items

    def test_basic_parse(self):
        events = yield from self.list(
            self.backend.basic_parse(AsyncReadable(BytesIO(JSON)))
        )
        self.assertEqual(events, JSON_EVENTS)

    def test_scalar(self):
        events = yield from self.list(
            self.backend.basic_parse(BytesIO(SCALAR_JSON))
        )
        self.assertEqual(events, [('number', 0)])

    def test_strings(self):
        events = yield from self.list(
            self.backend.basic_parse(BytesIO(STRINGS_JSON))
        )
        strings = [value for event, value in events if event == 'string']
        self.assertEqual(strings, ['', '"', '\\', '\\\\', '\b\f\n\r\t'])
        self.assertTrue(('map_key', 'special\t') in events)

    def test_int_numbers(self):
        events = yield from self.list(
            self.backend.basic_parse(BytesIO(INT_NUMBERS_JSON))
        )
        numbers = [value for event, value in events if event == 'number']
        self.assertTrue(all(type(n) is int  for n in numbers))

    def test_invalid(self):
        for json in INVALID_JSONS:
            with self.assertRaises(common.JSONError):
                yield from self.list(self.backend.basic_parse(BytesIO(json)))

    def test_incomplete(self):
        for json in INCOMPLETE_JSONS:
            with self.assertRaises(common.IncompleteJSONError):
                yield from self.list(self.backend.basic_parse(BytesIO(json)))

    def test_utf8_split(self):
        buf_size = JSON.index(b'\xd1') + 1
        try:
            yield from self.list(
                self.backend.basic_parse(BytesIO(JSON), buf_size=buf_size)
            )
        except UnicodeDecodeError:
            self.fail('UnicodeDecodeError raised')

    def test_lazy(self):
        # shouldn't fail since iterator is not exhausted
        yield from self.backend.basic_parse(BytesIO(INVALID_JSONS[0]))
        self.assertTrue(True)

    def test_boundary_lexeme(self):
        buf_size = JSON.index(b'false') + 1
        events = yield from self.list(
            self.backend.basic_parse(BytesIO(JSON), buf_size=buf_size)
        )
        self.assertEqual(events, JSON_EVENTS)

    def test_boundary_whitespace(self):
        buf_size = JSON.index(b'   ') + 1
        events = yield from self.list(
            self.backend.basic_parse(BytesIO(JSON), buf_size=buf_size)
        )
        self.assertEqual(events, JSON_EVENTS)

    def test_api(self):
        value = yield from self.list(self.backend.items(BytesIO(JSON), ''))
        self.assertTrue(value)
        value = yield from self.list(self.backend.parse(BytesIO(JSON)))
        self.assertTrue(value)


if __name__ == '__main__':
    unittest.main()
