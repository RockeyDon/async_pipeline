import asyncio
from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


class DataTest(TestCase):

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_data_int(self, data):
        pp = Pipe()
        stage = pp.data(data)
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data1=st.lists(st.integers()), data2=st.lists(st.characters()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_data_mix(self, data1, data2):
        pp = Pipe()
        data = data1 + data2
        stage = pp.data(data)
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_data_async(self, data):
        pp = Pipe()
        a_data = self._async_yield(data)
        stage = pp.data(a_data)
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_data_pipe(self, data):
        pp = Pipe()
        end = pp.data(data) | pp.result()
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_data_pipe_from_iter(self, data):
        pp = Pipe()
        end = data | pp.result()
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_data_pipe_from_async_iter(self, data):
        pp = Pipe()
        a_data = self._async_yield(data)
        end = a_data | pp.result()  # noqa
        pp.run()
        self.assertEqual(data, end.result)

    @staticmethod
    async def _async_yield(data):
        for datum in data:
            await asyncio.sleep(0.0)
            yield datum

    def test_data_int_async(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._data_int())

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_result_output(self, data):
        expect = []
        pp = Pipe()
        stage = pp.data(data)
        end = pp.result(src=stage, output=expect)
        pp.run()
        self.assertEqual(expect, end.result)

    async def _data_int(self):
        data = list(range(10))
        pp = Pipe()
        stage = pp.data(data)
        end = pp.result(src=stage)
        await pp.async_run()
        self.assertEqual(data, end.result)
