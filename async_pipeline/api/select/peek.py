from async_pipeline.stage import Stage


class Peek(Stage):

    async def run_func(self, data):
        result = self.f(data, **self.f_kw)
        _ = await self.may_await(result)
        return data
