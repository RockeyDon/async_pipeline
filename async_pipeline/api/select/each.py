from async_pipeline.stage import Stage


class Each(Stage):

    async def process(self):
        async for elem in self.input.aiter():
            await self.run_func(elem)
        await self.send_done()
