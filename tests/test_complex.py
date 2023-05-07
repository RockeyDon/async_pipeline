import asyncio
from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


# noinspection Duplicates
class ComplexTest(TestCase):

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_linear(self, data):
        expect = [
                     datum + 2
                     for datum in data
                     if datum > 2
                 ][:10]
        pp = Pipe()
        stage1 = pp.data(data)
        stage2 = pp.filter(src=stage1, func=lambda x: x > 2)
        stage3 = pp.map(src=stage2, func=lambda x: x + 2)
        stage4 = pp.limit(src=stage3, num=10)
        end = pp.result(src=stage4)
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_level(self, data):
        expect31 = [datum + 2 for datum in data if datum > 2]
        expect32 = [datum + 5 for datum in data if datum > 2]
        expect33 = [datum - 12 for datum in data if datum < 10]
        expect34 = [datum * 9 for datum in data if datum < 10]
        _cache51 = set()
        expect51 = []
        for datum in expect31 + expect33:
            _f = hash(datum)
            if _f not in _cache51:
                expect51.append(datum)
                _cache51.add(_f)
        _cache52 = set()
        expect52 = []
        for datum in expect32 + expect34:
            _f = hash(datum)
            if _f in _cache52:
                expect52.append(datum)
            _cache52.add(_f)
        expect = expect51 + expect52
        expect.sort()
        pp = Pipe()
        stage1 = pp.data(data)
        stage21 = pp.filter(src=stage1, func=lambda x: x > 2)
        stage22 = pp.filter(src=stage1, func=lambda x: x < 10)
        stage31 = pp.map(src=stage21, func=lambda x: x + 2)
        stage32 = pp.map(src=stage21, func=lambda x: x + 5)
        stage33 = pp.map(src=stage22, func=lambda x: x - 12)
        stage34 = pp.map(src=stage22, func=lambda x: x * 9)
        stage41 = pp.concat(stage31, stage33)
        stage42 = pp.concat(stage32, stage34)
        stage51 = pp.distinct(src=stage41)
        stage52 = pp.duplicate(src=stage42)
        stage6 = pp.concat(stage51, stage52)
        end = pp.result(src=stage6)
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_non_linear(self, data):
        expect22 = [datum for datum in data if datum < -10]
        expect31 = [datum + 2 for datum in data if datum > 2]
        expect32 = [datum + 5 for datum in data if datum > 2]
        _cache51 = set()
        expect51 = []
        for datum in expect31 + expect32:
            _f = hash(datum)
            if _f not in _cache51:
                expect51.append(datum)
                _cache51.add(_f)
        _cache52 = set()
        expect52 = []
        for datum in expect22:
            _f = hash(datum)
            if _f in _cache52:
                expect52.append(datum)
            _cache52.add(_f)
        expect = expect51 + expect52
        expect.sort()
        pp = Pipe()
        stage1 = pp.data(data)
        stage21 = pp.filter(src=stage1, func=lambda x: x > 2)
        stage22 = pp.filter(src=stage1, func=lambda x: x < -10)
        stage31 = pp.map(src=stage21, func=lambda x: x + 2)
        stage32 = pp.map(src=stage21, func=lambda x: x + 5)
        stage41 = pp.concat(stage31, stage32)
        stage51 = pp.distinct(src=stage41)
        stage52 = pp.duplicate(src=stage22)
        stage6 = pp.concat(stage51, stage52)
        end = pp.result(src=stage6)
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_non_linear_pipe(self, data):
        expect22 = [datum for datum in data if datum < -10]
        expect31 = [datum + 2 for datum in data if datum > 2]
        expect32 = [datum + 5 for datum in data if datum > 2]
        _cache51 = set()
        expect51 = []
        for datum in expect31 + expect32:
            _f = hash(datum)
            if _f not in _cache51:
                expect51.append(datum)
                _cache51.add(_f)
        _cache52 = set()
        expect52 = []
        for datum in expect22:
            _f = hash(datum)
            if _f in _cache52:
                expect52.append(datum)
            _cache52.add(_f)
        expect = expect51 + expect52
        expect.sort()
        pp = Pipe()
        stage1 = data | pp.multiply(
            pp.filter(func=lambda x: x > 2) | pp.multiply(
                pp.map(func=lambda x: x + 2),
                pp.map(func=lambda x: x + 5)
            ),
            pp.filter(func=lambda x: x < -10)
        )
        end = pp.concat(
            pp.concat(stage1[0][0], stage1[0][1]) | pp.distinct(),
            pp.duplicate(src=stage1[1])
        ) | pp.result()
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_timing(self, data):
        exp1 = [datum + 2 for datum in data]
        exp2 = [datum * 2 for datum in data]
        pp = Pipe()
        stage1 = pp.data(data)
        stage21 = pp.map(src=stage1, func=lambda x: x + 2)
        stage22 = pp.map(src=stage1, func=lambda x: x * 2)
        stage3 = pp.concat(stage21, stage22)
        end = pp.result(src=stage3)
        pp.run()
        self.assertEqual(exp1 + exp2, end.result)

    @hp.given(data=st.lists(st.integers(), min_size=10))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_timing_async(self, data):
        exp1 = [datum + 2 for datum in data]
        exp2 = [datum * 2 for datum in data]
        pp = Pipe()
        stage1 = pp.data(data)
        stage21 = pp.map(src=stage1, func=self._timing1)
        stage22 = pp.map(src=stage1, func=self._timing2)
        stage3 = pp.concat(stage21, stage22)
        end = pp.result(src=stage3)
        pp.run()
        self.assertNotEqual(exp1 + exp2, end.result)
        self.assertNotEqual(exp2 + exp1, end.result)

    @hp.given(data=st.lists(st.characters()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_non_linear2(self, data):
        expect = []
        for c in data:
            expect.extend([
                f'{c}_2_31_41_6', f'{c}_2_31_42_5_6', f'{c}_2_32_5_6'
            ])
        expect.sort()
        pp = Pipe()
        stage1 = pp.data(data)
        stage2 = pp.map(src=stage1, func=lambda x: x + '_2')
        stage31 = pp.map(src=stage2, func=lambda x: x + '_31')
        stage32 = pp.map(src=stage2, func=lambda x: x + '_32')
        stage41 = pp.map(src=stage31, func=lambda x: x + '_41')
        stage42 = pp.map(src=stage31, func=lambda x: x + '_42')
        stage5 = pp.concat(stage32, stage42) | pp.map(func=lambda x: x + '_5')
        stage6 = pp.concat(stage41, stage5) | pp.map(func=lambda x: x + '_6')
        end = pp.result(src=stage6)
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_non_linear2_pipe(self, data):
        expect = []
        for i in data:
            if (i > 0) and (i % 2 == 1):
                expect.append(f'{i}_2_31_41_6')
            elif (i > 0) and (i % 2 == 0):
                expect.append(f'{i}_2_31_42_5_6')
            elif i < 0:
                expect.append(f'{i}_2_32_5_6')
        expect.sort()
        pp = Pipe()
        stage1 = pp.data(data) | pp.map(func=lambda x: str(x) + '_2') | pp.partition({
            '31': {
                'match': "int(elem.split('_')[0]) > 0",
                'handle': pp.map(func=lambda x: str(x) + '_31') | pp.partition({
                    '41': {
                        'match': "int(elem.split('_')[0]) % 2 == 1",
                        'handle': pp.map(func=lambda x: str(x) + '_41')
                    },
                    '42': {
                        'match': "int(elem.split('_')[0]) % 2 == 0",
                        'handle': pp.map(func=lambda x: str(x) + '_42')
                    }
                })
            },
            '32': {
                'match': "int(elem.split('_')[0]) < 0",
                'handle': pp.map(func=lambda x: str(x) + '_32')
            }
        })
        stage2 = pp.concat(stage1['32'], stage1['31']['42']) | pp.map(func=lambda x: str(x) + '_5')
        end = pp.concat(stage2, stage1['31']['41']) | pp.map(func=lambda x: str(x) + '_6') | pp.result()
        pp.run()
        end.result.sort()
        self.assertEqual(expect, end.result)

    @hp.given(data1=st.lists(st.integers()), data2=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_result_output(self, data1, data2):
        pp1 = Pipe()
        pp2 = Pipe()
        end1 = pp1.data(data1) | pp1.result()
        end2 = pp2.data(data2) | pp2.result()
        pp1.run()
        pp2.run()
        self.assertEqual(data1, end1.result)
        self.assertEqual(data2, end2.result)

    @staticmethod
    async def _timing1(x):
        await asyncio.sleep(0.00)
        return x + 2

    @staticmethod
    async def _timing2(x):
        await asyncio.sleep(0.00)
        return x * 2
