import asyncio
from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


class DistinctTest(TestCase):

    @hp.given(data=st.lists(st.characters()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_distinct(self, data):
        expect = list(set(data))
        expect.sort()
        pp = Pipe()
        stage = pp.distinct(src=pp.data(data))
        end = pp.result(src=stage)
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.characters()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_distinct_kwargs(self, data):
        expect = []
        _cache = set()
        for datum in data:
            _f = self._func(datum, 10)
            if _f not in _cache:
                _cache.add(_f)
                expect.append(datum)
        pp = Pipe()
        stage = pp.distinct(src=pp.data(data), func=self._func, func_kw={'y': 10})
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.characters()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_distinct_async(self, data):
        expect = []
        _cache = set()
        for datum in data:
            _f = self._func(datum, 20)
            if _f not in _cache:
                _cache.add(_f)
                expect.append(datum)
        pp = Pipe()
        stage = pp.distinct(src=pp.data(data), func=self._a_func, func_kw={'y': 20})
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.characters()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_distinct_pipe(self, data):
        expect = list(set(data))
        expect.sort()
        pp = Pipe()
        end = pp.data(data) | pp.distinct() | pp.result()
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.characters()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_distinct_pipe_from_iter(self, data):
        expect = list(set(data))
        expect.sort()
        pp = Pipe()
        end = data | pp.distinct() | pp.result()
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @staticmethod
    def _func(x, y):
        return x + str(y)

    @staticmethod
    async def _a_func(x, y):
        await asyncio.sleep(0.0)
        return x + str(y)
