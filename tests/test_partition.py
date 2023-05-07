from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


# noinspection Duplicates
class PartitionTest(TestCase):

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_partition(self, data):
        expect1 = [datum + 1 for datum in data if datum > 0]
        expect2 = [datum - 2 for datum in data if datum < 0]
        pp = Pipe()
        stage1 = pp.partition(src=pp.data(data), cases={'p': 'elem > 0', 'n': 'elem < 0'})
        stage21 = pp.map(src=stage1['p'], func=lambda x: x + 1)
        stage22 = pp.map(src=stage1['n'], func=lambda x: x - 2)
        end1 = pp.result(src=stage21)
        end2 = pp.result(src=stage22)
        pp.run()
        self.assertEqual(expect1, end1.result)
        self.assertEqual(expect2, end2.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_default_partition(self, data):
        expect = [datum + 1 for datum in data]
        pp = Pipe()
        stage1 = pp.partition(src=pp.data(data), cases={'d': '1'})
        stage2 = pp.map(src=stage1['d'], func=lambda x: x + 1)
        end = pp.result(src=stage2)
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_partition_pipe(self, data):
        expect1 = [datum + 1 for datum in data if datum > 0]
        expect2 = [datum - 2 for datum in data if datum < 0]
        pp = Pipe()
        stage1 = pp.data(data) | pp.partition(cases={'p': 'elem > 0', 'n': 'elem < 0'})
        stage21 = pp.map(src=stage1['p'], func=lambda x: x + 1)
        stage22 = pp.map(src=stage1['n'], func=lambda x: x - 2)
        end1 = pp.result(src=stage21)
        end2 = pp.result(src=stage22)
        pp.run()
        self.assertEqual(expect1, end1.result)
        self.assertEqual(expect2, end2.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_partition_pipe_from_iter(self, data):
        expect1 = [datum + 1 for datum in data if datum > 0]
        expect2 = [datum - 2 for datum in data if datum < 0]
        pp = Pipe()
        stage1 = data | pp.partition(cases={'p': 'elem > 0', 'n': 'elem < 0'})
        stage21 = pp.map(src=stage1['p'], func=lambda x: x + 1)
        stage22 = pp.map(src=stage1['n'], func=lambda x: x - 2)
        end1 = pp.result(src=stage21)
        end2 = pp.result(src=stage22)
        pp.run()
        self.assertEqual(expect1, end1.result)
        self.assertEqual(expect2, end2.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_pipe_partition(self, data):
        expect1 = [datum + 1 for datum in data if datum > 0]
        expect2 = [datum - 2 for datum in data if datum < 0]
        pp = Pipe()
        stage = pp.data(data) | pp.partition({
            'p': {
                'match': 'elem > 0',
                'handle': pp.map(func=lambda x: x + 1)
            },
            'n': {
                'match': 'elem < 0',
                'handle': pp.map(func=lambda x: x - 2)
            }
        })
        end1 = pp.result(src=stage['p'])
        end2 = pp.result(src=stage['n'])
        pp.run()
        self.assertEqual(expect1, end1.result)
        self.assertEqual(expect2, end2.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_pipe_partition_pipe(self, data):
        expect1 = [datum + 1 for datum in data if datum > 0]
        expect2 = [datum - 2 for datum in data if datum < 0]
        pp = Pipe()
        stage = pp.data(data) | pp.partition({
            'p': {
                'match': 'elem > 0',
                'handle': pp.map(func=lambda x: x + 1) | pp.result()
            },
            'n': {
                'match': 'elem < 0',
                'handle': pp.map(func=lambda x: x - 2) | pp.result()
            }
        })
        pp.run()
        self.assertEqual(expect1, stage['p'].result)
        self.assertEqual(expect2, stage['n'].result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_pipe_partition_nest(self, data):
        expect1 = [datum + 1 for datum in data if datum > 0]
        expect2 = [datum + 2 for datum in data if (datum < 0 and datum % 2 == 1)]
        expect3 = [datum + 3 for datum in data if (datum < 0 and datum % 2 == 0)]
        pp = Pipe()
        stage = pp.data(data) | pp.partition({
            'p': {
                'match': 'elem > 0',
                'handle': pp.map(func=lambda x: x + 1) | pp.result()
            },
            'n': {
                'match': 'elem < 0',
                'handle': pp.partition({
                    'o': {
                        'match': 'elem % 2 == 1',
                        'handle': pp.map(func=lambda x: x + 2) | pp.result()
                    },
                    'e': {
                        'match': 'elem % 2 == 0',
                        'handle': pp.map(func=lambda x: x + 3) | pp.result()
                    }
                })
            }
        })
        pp.run()
        self.assertEqual(expect1, stage['p'].result)
        self.assertEqual(expect2, stage['n']['o'].result)
        self.assertEqual(expect3, stage['n']['e'].result)
