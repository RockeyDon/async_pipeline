import typing as tp

from async_pipeline.stage import Stage


class Data(Stage):
    input: tp.Union[tp.AsyncIterable, tp.Iterable]

    def __init__(self, pipe, src):
        super().__init__(pipe, src)

    def bind_input(self, src):
        self.input = src

    async def process(self):
        if isinstance(self.input, tp.AsyncIterable):
            async for elem in self.input:
                await self.send_output(elem)
        else:
            for elem in self.input:
                await self.send_output(elem)
        await self.send_done()
