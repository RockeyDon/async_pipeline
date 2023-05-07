from async_pipeline.stage import Stage


class Result(Stage):

    def __init__(self, pipe, src, output=None):
        super().__init__(pipe, src)
        self.result = output if output is not None else []

    async def process(self):
        async for elem in self.input.aiter():
            self.result.append(elem)
