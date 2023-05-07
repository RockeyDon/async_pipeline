import asyncio
from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


class EachTest(TestCase):

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_plus(self, data):
        expect = [{'val': datum + 1} for datum in data]
        data = [{'val': datum} for datum in data]
        pp = Pipe()
        stage = pp.each(src=pp.data(data), func=self._add)
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual([], end.result)
        self.assertEqual(expect, data)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_async(self, data):
        expect = [{'val': datum + 10} for datum in data]
        data = [{'val': datum} for datum in data]
        pp = Pipe()
        stage = pp.each(src=pp.data(data), func=self._a_add)
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual([], end.result)
        self.assertEqual(expect, data)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_kwargs(self, data):
        expect = [{'val': datum + 100} for datum in data]
        data = [{'val': datum} for datum in data]
        pp = Pipe()
        stage = pp.each(src=pp.data(data), func=self._add_kw, func_kw={'y': 100})
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual([], end.result)
        self.assertEqual(expect, data)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_pipe(self, data):
        expect = [{'val': datum + 1} for datum in data]
        data = [{'val': datum} for datum in data]
        pp = Pipe()
        end = pp.data(data) | pp.each(func=self._add) | pp.result()
        pp.run()
        self.assertEqual([], end.result)
        self.assertEqual(expect, data)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_pipe_from_iter(self, data):
        expect = [{'val': datum + 1} for datum in data]
        data = [{'val': datum} for datum in data]
        pp = Pipe()
        end = data | pp.each(func=self._add) | pp.result()  # noqa
        pp.run()
        self.assertEqual([], end.result)
        self.assertEqual(expect, data)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_pipe_from_async_iter(self, data):
        expect = [{'val': datum + 1} for datum in data]
        data = [{'val': datum} for datum in data]
        pp = Pipe()
        a_data = self._async_yield(data)
        end = a_data | pp.each(func=self._add) | pp.result()  # noqa
        pp.run()
        self.assertEqual([], end.result)
        self.assertEqual(expect, data)

    @staticmethod
    def _add(x):
        x['val'] = x['val'] + 1

    @staticmethod
    def _add_kw(x, y):
        x['val'] = x['val'] + y

    @staticmethod
    async def _a_add(x):
        await asyncio.sleep(0.0)
        x['val'] = x['val'] + 10

    @staticmethod
    async def _async_yield(data):
        for datum in data:
            await asyncio.sleep(0.0)
            yield datum
