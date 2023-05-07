import typing as tp
from async_pipeline.stage import Stage


class PartitionItem(Stage):

    def __init__(self, pipe, partition, src):
        super().__init__(pipe, src)
        self._partition = partition

    async def process(self):
        async for elem in self.input.aiter():
            await self.send_output(elem)
        await self.send_done()


class PartSrc:
    def __init__(self):
        self.outputs = []

    def bind_output(self, queue):
        self.outputs.append(queue)


class Partition(Stage):

    def __init__(self, pipe, src, cases):
        super().__init__(pipe, src)
        self._items = {}
        self._outputs = {}
        self._matcher = {}
        for k, v in cases.items():
            if isinstance(v, str):
                self._create_item_from_str(k, v)
            elif isinstance(v, tp.Dict):
                self._create_item_from_dict(k, v)
            else:
                raise Exception(f"Requires str or dict object, get {v}")

    def _create_item_from_str(self, key, val):
        self._outputs[key] = PartSrc()
        self._items[key] = PartitionItem(self.pipe, self, self._outputs[key])
        self._matcher[key] = eval(f'lambda elem: {val}')

    def _create_item_from_dict(self, key, val):
        self._create_item_from_str(key, val['match'])
        self._items[key].bind_output(val['handle'].total_input)
        self._items[key] = val['handle']

    async def process(self):
        async for elem in self.input.aiter():
            for key, match in self._matcher.items():
                if match(elem):
                    await self._send_item_output(elem, key)
                    break
        await self.send_done()

    async def _send_item_output(self, result, key):
        for out in self._outputs[key].outputs:
            await out.put(result)

    async def send_done(self):
        signal = self.done_signal()
        for key in self._outputs:
            await self._send_item_output(signal, key)

    def __getitem__(self, item):
        return self._items[item]
