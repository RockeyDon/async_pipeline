from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


# noinspection Duplicates
class MultiplyTest(TestCase):

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_multiply(self, data):
        expect2 = [2 * datum for datum in data]
        pp = Pipe()
        stage1 = pp.data(data)
        stage2 = pp.multiply(pp.map(func=lambda x: x), pp.map(func=lambda x: 2 * x), src=stage1)
        end1 = pp.result(src=stage2[0])
        end2 = pp.result(src=stage2[1])
        pp.run()
        self.assertEqual(data, end1.result)
        self.assertEqual(expect2, end2.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_multiply_one(self, data):
        pp = Pipe()
        stage1 = pp.data(data)
        stage2 = pp.multiply(pp.map(func=lambda x: x), src=stage1)
        end = pp.result(src=stage2[0])
        pp.run()
        self.assertEqual(data, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_multiply_three(self, data):
        expect2 = [2 * datum for datum in data]
        expect3 = [datum + 3 for datum in data]
        pp = Pipe()
        stage1 = pp.data(data)
        stage2 = pp.multiply(
            pp.map(func=lambda x: x),
            pp.map(func=lambda x: 2 * x),
            pp.map(func=lambda x: x + 3),
            src=stage1
        )
        end1 = pp.result(src=stage2[0])
        end2 = pp.result(src=stage2[1])
        end3 = pp.result(src=stage2[2])
        pp.run()
        self.assertEqual(data, end1.result)
        self.assertEqual(expect2, end2.result)
        self.assertEqual(expect3, end3.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_multiply_nest(self, data):
        expect2 = [2 * datum for datum in data]
        expect3 = [datum + 1 for datum in data]
        expect4 = [3 * datum for datum in data]
        pp = Pipe()
        stage1 = pp.data(data)
        stage2 = pp.multiply(
            pp.multiply(
                pp.map(func=lambda x: x),
                pp.map(func=lambda x: 2 * x),
            ),
            pp.multiply(
                pp.map(func=lambda x: x + 1),
                pp.map(func=lambda x: 3 * x),
            ),
            src=stage1
        )
        end1 = pp.result(src=stage2[0][0])
        end2 = pp.result(src=stage2[0][1])
        end3 = pp.result(src=stage2[1][0])
        end4 = pp.result(src=stage2[1][1])
        pp.run()
        self.assertEqual(data, end1.result)
        self.assertEqual(expect2, end2.result)
        self.assertEqual(expect3, end3.result)
        self.assertEqual(expect4, end4.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_multiply_nest_mix(self, data):
        expect2 = [2 * datum for datum in data]
        expect3 = [datum + 1 for datum in data]
        pp = Pipe()
        stage = data | pp.multiply(
            pp.multiply(
                pp.map(func=lambda x: x),
                pp.map(func=lambda x: 2 * x)
            ),
            pp.map(func=lambda x: x + 1)
        )
        end1 = pp.result(src=stage[0][0])
        end2 = pp.result(src=stage[0][1])
        end3 = pp.result(src=stage[1])
        pp.run()
        self.assertEqual(data, end1.result)
        self.assertEqual(expect2, end2.result)
        self.assertEqual(expect3, end3.result)
