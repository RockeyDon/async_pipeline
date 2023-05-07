from async_pipeline.stage import Stage


class FilterNot(Stage):

    async def process(self):
        async for elem in self.input.aiter():
            flag = await self.run_func(elem)
            if not flag:
                await self.send_output(elem)
        await self.send_done()
