import asyncio
import typing as tp

from async_pipeline.signal import Done

T = tp.TypeVar("T")


class Queue(asyncio.Queue, tp.Generic[T]):

    def __init__(self, stage):
        super().__init__(maxsize=100)
        self._stage = stage

    async def aiter(self, src_num=1) -> tp.AsyncIterator[T]:
        catch_signal_cnt = 0
        while True:
            elem = await self.get()
            if isinstance(elem, Done):
                catch_signal_cnt += 1
            else:
                yield elem
            if catch_signal_cnt >= src_num:
                break

    def __str__(self):
        return repr(self)
