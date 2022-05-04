import asyncio
import pytest
import logging

from redis import asyncio as aioredis  # type: ignore[attr-defined]

# logging
_log = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    """
    This special event loop fixture overriding the default one in pytest-asyncio
    allows canceled tasks a chance to process the CancelledError that is thrown
    into the task, otherwise the tests will pass but it generates "Task was
    destroyed but it is pending!" messages when the loop is stopped.

    https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancel
    """
    _log.info("event_loop")
    loop = asyncio.get_event_loop()
    _log.info("    - loop %r: %r", id(loop), loop)

    yield loop
    _log.info("    - back from yield")

    loop.call_soon(loop.stop)
    loop.run_forever()
    _log.info("    - loop done running forever %r: %r", id(loop), loop)


@pytest.fixture(scope="session", autouse=True)
def r(event_loop):
    _log.info("r -- redis_connection")

    r = aioredis.from_url("redis://localhost/")
    logging.debug("    - connection: %r", r)

    yield r
    logging.debug("    - finished with the connection")

    event_loop.create_task(r.close())
