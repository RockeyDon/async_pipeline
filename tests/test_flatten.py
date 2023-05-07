import asyncio
from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


class FlattenTest(TestCase):

    @hp.given(data=st.lists(st.lists(st.integers())))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flatten(self, data):
        expect = [elem for datum in data for elem in datum]
        self._flatten(data, expect)

    @hp.given(data=st.lists(st.lists(st.lists(st.integers()))))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flatten_nest(self, data):
        expect = [elem for datum in data for elem in datum]
        self._flatten(data, expect)

    @hp.given(data=st.lists(st.lists(st.integers())))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flatten_async(self, data):
        expect = [elem for datum in data for elem in datum]
        data = [self._async_yield(datum) for datum in data]
        self._flatten(data, expect)

    def _flatten(self, data, expect):
        pp = Pipe()
        stage = pp.flatten(src=pp.data(data))
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.lists(st.integers())))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flatten_pipe(self, data):
        pp = Pipe()
        end = pp.data(data) | pp.flatten() | pp.result()
        pp.run()
        expect = [elem for datum in data for elem in datum]
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.lists(st.integers())))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flatten_pipe_from_iter(self, data):
        pp = Pipe()
        end = data | pp.flatten() | pp.result()
        pp.run()
        expect = [elem for datum in data for elem in datum]
        self.assertEqual(expect, end.result)

    @staticmethod
    async def _async_yield(data):
        for datum in data:
            await asyncio.sleep(0.0)
            yield datum
