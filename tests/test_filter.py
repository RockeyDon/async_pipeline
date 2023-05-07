import asyncio
from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


class FilterTest(TestCase):

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_filter_positive(self, data):
        pp = Pipe()
        stage = pp.filter(src=pp.data(data), func=lambda x: x > 0)
        end = pp.result(src=stage)
        pp.run()
        expect = [datum for datum in data if datum > 0]
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_filter_kwargs(self, data):
        pp = Pipe()
        stage = pp.filter(src=pp.data(data), func=lambda x, y: x > y, func_kw={'y': 5})
        end = pp.result(src=stage)
        pp.run()
        expect = [datum for datum in data if datum > 5]
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_filter_async(self, data):
        pp = Pipe()
        stage = pp.filter(src=pp.data(data), func=self._negative)
        end = pp.result(src=stage)
        pp.run()
        expect = [datum for datum in data if datum < 0]
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_filter_pipe(self, data):
        pp = Pipe()
        end = pp.data(data) | pp.filter(func=lambda x: x > 0) | pp.result()
        pp.run()
        expect = [datum for datum in data if datum > 0]
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_filter_pipe_from_iter(self, data):
        pp = Pipe()
        end = data | pp.filter(func=lambda x: x > 0) | pp.result()
        pp.run()
        expect = [datum for datum in data if datum > 0]
        self.assertEqual(expect, end.result)

    @staticmethod
    async def _negative(x):
        await asyncio.sleep(0.0)
        return x < 0
