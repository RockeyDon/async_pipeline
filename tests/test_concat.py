import asyncio
from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


# noinspection Duplicates
class ConcatTest(TestCase):

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat(self, data):
        expect = data + [2 * datum for datum in data]
        expect.sort()
        pp = Pipe()
        stage11 = pp.map(src=pp.data(data), func=lambda x: x)
        stage12 = pp.map(src=pp.data(data), func=lambda x: 2 * x)
        stage2 = pp.concat(stage11, stage12)
        end = pp.result(src=stage2)
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_one(self, data):
        pp = Pipe()
        stage1 = pp.map(src=pp.data(data), func=lambda x: x)
        stage2 = pp.concat(stage1)
        end = pp.result(src=stage2)
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_three(self, data):
        expect = data + [2 * datum for datum in data] + [datum + 3 for datum in data]
        expect.sort()
        pp = Pipe()
        stage11 = pp.map(src=pp.data(data), func=lambda x: x)
        stage12 = pp.map(src=pp.data(data), func=lambda x: 2 * x)
        stage13 = pp.map(src=pp.data(data), func=lambda x: x + 3)
        stage2 = pp.concat(stage11, stage12, stage13)
        end = pp.result(src=stage2)
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_nest(self, data):
        expect = data + [2 * datum for datum in data] + \
                 [datum + 1 for datum in data] + \
                 [3 * datum for datum in data]
        expect.sort()
        pp = Pipe()
        stage11 = pp.map(src=pp.data(data), func=lambda x: x)
        stage12 = pp.map(src=pp.data(data), func=lambda x: 2 * x)
        stage21 = pp.concat(stage11, stage12)
        stage13 = pp.map(src=pp.data(data), func=lambda x: x + 1)
        stage14 = pp.map(src=pp.data(data), func=lambda x: 3 * x)
        stage22 = pp.concat(stage13, stage14)
        stage3 = pp.concat(stage21, stage22)
        end = pp.result(src=stage3)
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_nest_pipe(self, data):
        expect = data + [2 * datum for datum in data] + \
                 [datum + 1 for datum in data] + \
                 [3 * datum for datum in data]
        expect.sort()
        pp = Pipe()
        end = pp.concat(
            pp.concat(
                data | pp.map(func=lambda x: x),
                data | pp.map(func=lambda x: 2 * x)
            ),
            pp.concat(
                data | pp.map(func=lambda x: x + 1),
                data | pp.map(func=lambda x: 3 * x)
            )
        ) | pp.result()
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_nest_mix(self, data):
        expect = data + [2 * datum for datum in data] + \
                 [datum + 1 for datum in data]
        expect.sort()
        pp = Pipe()
        stage11 = pp.map(src=pp.data(data), func=lambda x: x)
        stage12 = pp.map(src=pp.data(data), func=lambda x: 2 * x)
        stage21 = pp.concat(stage11, stage12)
        stage13 = pp.map(src=pp.data(data), func=lambda x: x + 1)
        stage3 = pp.concat(stage21, stage13)
        end = pp.result(src=stage3)
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_nest_mix_pipe(self, data):
        expect = data + [2 * datum for datum in data] + \
                 [datum + 1 for datum in data]
        expect.sort()
        pp = Pipe()
        end = pp.concat(
            pp.concat(
                data | pp.map(func=lambda x: x),
                data | pp.map(func=lambda x: 2 * x)
            ),
            data | pp.map(func=lambda x: x + 1)
        ) | pp.result()
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data1=st.lists(st.integers()), data2=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_pipe(self, data1, data2):
        expect = data1 + data2
        expect.sort()
        pp = Pipe()
        end = pp.concat(pp.data(data1), pp.data(data2)) | pp.result()
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data1=st.lists(st.integers()), data2=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_pipe_from_iter(self, data1, data2):
        expect = data1 + data2
        expect.sort()
        pp = Pipe()
        end = pp.concat(data1, data2) | pp.result()
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data1=st.lists(st.integers()), data2=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_pipe_from_async_iter(self, data1, data2):
        expect = data1 + data2
        expect.sort()
        pp = Pipe()
        end = pp.concat(self._async_yield(data1), self._async_yield(data2)) | pp.result()
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @staticmethod
    async def _async_yield(data):
        for datum in data:
            await asyncio.sleep(0.0)
            yield datum
