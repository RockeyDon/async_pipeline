import typing as tp

from async_pipeline.queue import Queue
from async_pipeline.signal import Done


class Stage:
    _children = {}
    input: Queue
    outputs: tp.List[Queue]
    total_input: Queue
    f: tp.Callable
    f_kw: tp.Optional[dict]

    def __init__(self, pipe, src, func=None, func_kw=None):
        self.pipe = self.bind_pipe(pipe)
        self.set_func(func, func_kw)
        self.create_queue()
        self.bind_input(src)

    def __init_subclass__(cls, **kwargs):
        cls._children[cls.__name__] = cls

    def bind_pipe(self, pipe):
        pipe.add(self, self.process())
        return pipe

    def set_func(self, func, func_kw):
        if func is not None:
            self.f = func
            self.f_kw = func_kw or {}

    def create_queue(self):
        self.input = self.total_input = Queue(self)
        self.outputs = []

    def bind_input(self, src):
        if src is not None:
            src.bind_output(self.input)

    async def process(self):
        async for elem in self.input.aiter():
            result = await self.run_func(elem)
            await self.send_output(result)
        await self.send_done()

    async def run_func(self, data):
        result = self.f(data, **self.f_kw)
        return await self.may_await(result)

    def bind_output(self, queue: 'Queue'):
        self.outputs.append(queue)

    @staticmethod
    async def may_await(result):
        if isinstance(result, tp.Awaitable):
            return await result
        return result

    async def send_output(self, result):
        for out in self.outputs:
            await out.put(result)

    async def send_done(self):
        return await self.send_output(self.done_signal())

    def done_signal(self):
        return Done(self)

    def __ror__(self, other):
        if isinstance(other, Stage):
            previous = other
        elif isinstance(other, (tp.AsyncIterable, tp.Iterable)):
            previous = self._iter_to_data(other)
        else:
            raise Exception(f"Requires Stage or Iterable object, get {other}")
        self.bind_input(previous)
        self.total_input = previous.input
        return self

    def _iter_to_data(self, data):
        return self._children['Data'](pipe=self.pipe, src=data)
