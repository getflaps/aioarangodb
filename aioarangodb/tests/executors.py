import time

from stashify_graph.arango.executor import (
    AsyncExecutor,
    BatchExecutor,
    TransactionExecutor,
)
from aioarangodb.job import BatchJob


class TestAsyncExecutor(AsyncExecutor):

    def __init__(self, connection):
        super(TestAsyncExecutor, self).__init__(
            connection=connection,
            return_result=True
        )

    async def execute(self, request, response_handler):
        job = await AsyncExecutor.execute(self, request, response_handler)
        while job.status() != 'done':
            time.sleep(.01)
        return job.result()


class TestBatchExecutor(BatchExecutor):

    def __init__(self, connection):
        super(TestBatchExecutor, self).__init__(
            connection=connection,
            return_result=True
        )

    async def execute(self, request, response_handler):
        self._committed = False
        self._queue.clear()

        job = BatchJob(response_handler)
        self._queue[job.id] = (request, job)
        await self.commit()
        return await job.result()


class TestTransactionExecutor(TransactionExecutor):

    # noinspection PyMissingConstructor
    def __init__(self, connection):
        self._conn = connection

    async def execute(self, request, response_handler):
        if request.read is request.write is request.exclusive is None:
            resp = self._conn.send_request(request)
            return response_handler(resp)

        super(TestTransactionExecutor, self).__init__(
            connection=self._conn,
            sync=True,
            allow_implicit=False,
            lock_timeout=0,
            read=request.read,
            write=request.write,
            exclusive=request.exclusive
        )
        result = await TransactionExecutor.execute(self, request, response_handler)
        await self.commit()
        return result
