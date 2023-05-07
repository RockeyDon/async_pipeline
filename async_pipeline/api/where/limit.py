from async_pipeline.stage import Stage


class Limit(Stage):

    def __init__(self, pipe, src, num):
        super().__init__(pipe, src)
        self._count = 0
        self._max_count = num

    async def process(self):
        async for elem in self.input.aiter():
            if self._count < self._max_count:
                await self.send_output(elem)
                self._count += 1
            else:
                break
        await self.send_done()
