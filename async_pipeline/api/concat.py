from async_pipeline.stage import Stage


class Concat(Stage):

    def __init__(self, pipe, *sources):
        super().__init__(pipe, sources)
        self.src_num = len(sources)

    def bind_input(self, sources):
        assert getattr(self, 'pipe') is not None
        for stage in sources:
            if not isinstance(stage, Stage):
                stage = self._iter_to_data(stage)
            stage.outputs.append(self.input)

    async def process(self):
        async for elem in self.input.aiter(self.src_num):
            await self.send_output(elem)
        await self.send_done()
