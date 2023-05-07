from unittest import TestCase

from async_pipeline.pipe import Pipe


# noinspection Duplicates
class ExceptionTest(TestCase):

    def test_ror(self):
        pp = Pipe()
        data = 1
        with self.assertRaises(Exception) as ctx:
            data | pp.result()
        self.assertTrue("Requires Stage or Iterable object" in str(ctx.exception))
        pp.cancel()

    def test_double_run(self):
        pp = Pipe()
        pp.data(range(10)) | pp.result()
        pp.run()
        with self.assertRaises(Exception) as ctx:
            pp.run()
        self.assertTrue("already run" in str(ctx.exception))
        pp.cancel()

    def test_pipe_partition_pipe(self):
        pp = Pipe()
        data = pp.data(range(10))
        with self.assertRaises(Exception) as ctx:
            pp.partition({'p': 1, 'n': 2}, src=data)
        self.assertTrue("Requires str or dict object" in str(ctx.exception))
        pp.cancel()
