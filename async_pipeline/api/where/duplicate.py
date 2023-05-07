from async_pipeline.stage import Stage


class Duplicate(Stage):

    def __init__(self, pipe, src, func=hash, func_kw=None):
        super().__init__(pipe, src, func, func_kw)
        self._cache = set()

    async def process(self):
        async for elem in self.input.aiter():
            sign = await self.run_func(elem)
            if sign in self._cache:
                await self.send_output(elem)
            else:
                self._cache.add(sign)
        await self.send_done()
