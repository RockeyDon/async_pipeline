from async_pipeline.api.distribute.partition import PartSrc
from async_pipeline.stage import Stage


class MultiplyItem(Stage):

    def __init__(self, pipe, multiply, src):
        super().__init__(pipe, src)
        self._multiply = multiply

    async def process(self):
        async for elem in self.input.aiter():
            await self.send_output(elem)
        await self.send_done()


class Multiply(Stage):

    def __init__(self, pipe, src, targets):
        super().__init__(pipe, src)
        self._items = []
        self._outputs = []
        for tar in targets:
            self._create_item_from_list(tar)

    def _create_item_from_list(self, target):
        part = PartSrc()
        self._outputs.append(part)
        item = MultiplyItem(self.pipe, self, part)
        item.bind_output(target.total_input)
        self._items.append(target)

    async def process(self):
        async for elem in self.input.aiter():
            await self.send_output(elem)
        await self.send_done()

    async def send_output(self, result):
        for part in self._outputs:
            for out in part.outputs:
                await out.put(result)

    def __getitem__(self, index):
        return self._items[index]
