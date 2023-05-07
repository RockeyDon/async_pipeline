from async_pipeline.api.concat import Concat
from async_pipeline.api.data import Data
from async_pipeline.api.where import Filter, FilterNot, Distinct, Duplicate, Limit
from async_pipeline.api.flatten import Flatten
from async_pipeline.api.select import Map, Each, Peek
from async_pipeline.api.distribute import Partition, Multiply
from async_pipeline.api.result import Result


class APIMixin:

    def data(self, src):
        return Data(self, src)

    def result(self, src=None, output=None):
        return Result(self, src, output)

    def map(self, func, src=None, func_kw=None):
        return Map(self, src, func, func_kw)

    def filter(self, func, src=None, func_kw=None):
        return Filter(self, src, func, func_kw)

    def flatten(self, src=None):
        return Flatten(self, src)

    def concat(self, *src):
        return Concat(self, *src)

    def partition(self, cases, src=None):
        return Partition(self, src, cases)

    def multiply(self, *targets, src=None):
        return Multiply(self, src, targets)

    def each(self, func, src=None, func_kw=None):
        return Each(self, src, func, func_kw)

    def peek(self, func, src=None, func_kw=None):
        return Peek(self, src, func, func_kw)

    def filter_not(self, func, src=None, func_kw=None):
        return FilterNot(self, src, func, func_kw)

    def distinct(self, func=hash, src=None, func_kw=None):
        return Distinct(self, src, func, func_kw)

    def duplicate(self, func=hash, src=None, func_kw=None):
        return Duplicate(self, src, func, func_kw)

    def limit(self, num, src=None):
        return Limit(self, src, num)
