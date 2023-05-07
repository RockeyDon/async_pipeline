from unittest import TestCase

from async_pipeline.pipe import Pipe
from tests.hp_conf import hp, st, MAX_EXAMPLES


class LimitTest(TestCase):

    @hp.given(data=st.lists(st.integers()), limit=st.integers())
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_limit(self, data, limit):
        expect = [datum for index, datum in enumerate(data) if index < limit]
        pp = Pipe()
        stage = pp.limit(src=pp.data(data), num=limit)
        end = pp.result(src=stage)
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()), limit=st.integers())
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_duplicate_pipe(self, data, limit):
        expect = [datum for index, datum in enumerate(data) if index < limit]
        pp = Pipe()
        end = pp.data(data) | pp.limit(num=limit) | pp.result()
        pp.run()
        self.assertEqual(expect, end.result)

    @hp.given(data=st.lists(st.integers()), limit=st.integers())
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_duplicate_pipe_from_iter(self, data, limit):
        expect = [datum for index, datum in enumerate(data) if index < limit]
        pp = Pipe()
        end = data | pp.limit(num=limit) | pp.result()
        pp.run()
        self.assertEqual(expect, end.result)
