import typing as tp

from async_pipeline.stage import Stage


class Flatten(Stage):

    def __init__(self, pipe, src):
        super().__init__(pipe, src)

    async def process(self):
        async for data in self.input.aiter():
            if isinstance(data, tp.AsyncIterable):
                async for elem in data:
                    await self.send_output(elem)
            else:
                for elem in data:
                    await self.send_output(elem)
        await self.send_done()
