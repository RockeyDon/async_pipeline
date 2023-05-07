import asyncio
import typing as tp

from async_pipeline.api import APIMixin


class Pipe(APIMixin):

    def __init__(self):
        self._stages = []
        self._coroutines = []
        self._state = 'Init'

    def add(self, stage, task: tp.Coroutine):
        self._stages.append(stage)
        self._coroutines.append(task)

    async def async_run(self):
        self._run_check_state()
        await asyncio.gather(*[task for task in self._coroutines])
        self._state = 'Done'

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_run())

    def cancel(self):
        for coro in self._coroutines:
            coro.close()

    def _run_check_state(self):
        if self._state == 'Done':
            raise Exception(f"pipe {self} already run")
