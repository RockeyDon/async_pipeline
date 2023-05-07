import asyncio
from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


class DuplicateTest(TestCase):

    @hp.given(data=st.lists(st.characters()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_duplicate(self, data):
        expect = self._hash_expect(data)
        pp = Pipe()
        stage = pp.duplicate(src=pp.data(data))
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_duplicate_kwargs(self, data):
        expect = []
        _cache = set()
        for datum in data:
            _f = self._func(datum, 10)
            if _f in _cache:
                expect.append(datum)
            _cache.add(_f)
        pp = Pipe()
        stage = pp.duplicate(src=pp.data(data), func=self._func, func_kw={'y': 10})
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_duplicate_async(self, data):
        expect = []
        _cache = set()
        for datum in data:
            _f = self._func(datum, 20)
            if _f in _cache:
                expect.append(datum)
            _cache.add(_f)
        pp = Pipe()
        stage = pp.duplicate(src=pp.data(data), func=self._a_func, func_kw={'y': 20})
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.characters()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_duplicate_pipe(self, data):
        expect = self._hash_expect(data)
        pp = Pipe()
        end = pp.data(data) | pp.duplicate() | pp.result()
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.characters()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_duplicate_pipe_from_iter(self, data):
        expect = self._hash_expect(data)
        pp = Pipe()
        end = data | pp.duplicate() | pp.result()
        pp.run()
        self.assertEqual(expect, end.result)

    @staticmethod
    def _hash_expect(data):
        expect = []
        _cache = set()
        for datum in data:
            _f = hash(datum)
            if _f in _cache:
                expect.append(datum)
            _cache.add(_f)
        return expect

    @staticmethod
    def _func(x, y):
        return x % y

    @staticmethod
    async def _a_func(x, y):
        await asyncio.sleep(0.0)
        return x % y
