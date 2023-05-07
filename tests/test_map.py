import asyncio
from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


# noinspection Duplicates
class MapTest(TestCase):

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_id(self, data):
        pp = Pipe()
        stage = pp.map(src=pp.data(data), func=lambda x: x)
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_id_async(self, data):
        pp = Pipe()
        stage = pp.map(src=pp.data(data), func=self._id)
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_plus(self, data):
        pp = Pipe()
        stage = pp.map(src=pp.data(data), func=lambda x: x + 10)
        end = pp.result(src=stage)
        pp.run()
        expect = [datum + 10 for datum in data]
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_kwargs(self, data):
        pp = Pipe()
        stage = pp.map(src=pp.data(data), func=lambda x, y: x + y, func_kw={'y': 100})
        end = pp.result(src=stage)
        pp.run()
        expect = [datum + 100 for datum in data]
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_pipe(self, data):
        pp = Pipe()
        end = pp.data(data) | pp.map(func=lambda x: x) | pp.result()
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_pipe_from_iter(self, data):
        pp = Pipe()
        end = data | pp.map(func=lambda x: x) | pp.result()
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_pipe_from_async_iter(self, data):
        pp = Pipe()
        a_data = self._async_yield(data)
        end = a_data | pp.map(func=lambda x: x) | pp.result()  # noqa
        pp.run()
        self.assertEqual(data, end.result)

    @staticmethod
    async def _id(x):
        await asyncio.sleep(0.0)
        return x

    @staticmethod
    async def _async_yield(data):
        for datum in data:
            await asyncio.sleep(0.0)
            yield datum
