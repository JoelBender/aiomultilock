"""
Test
----
"""

import logging
import pytest

from typing import Any

from aiomultilock import Multilock, CannotObtainLock

# logging
_log = logging.getLogger(__name__)


async def debug_lock(r, name: str, step: int) -> None:
    """Handy function for debugging tests."""
    _log.debug("    dump %d:", step)
    _log.debug("        - exclusive: %r", await r.get(name + ":exclusive"))
    _log.debug(
        "        - exclusive waiting: %r",
        await r.zrange(name + ":exclusive_waiting", 0, -1),
    )
    _log.debug("        - shared: %r", await r.zrange(name + ":shared", 0, -1))
    _log.debug(
        "        - shared waiting: %r", await r.zrange(name + ":shared_waiting", 0, -1)
    )


@pytest.mark.asyncio  # type: ignore[misc]
async def test_blocking_004(r, *args: Any, **kwargs: Any) -> None:
    """Exclusive lock blocking shared lock."""
    _log.info("test_shared_002 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_blocking_004", retry_count=0)
    lock_2 = Multilock(r, "test_blocking_004", retry_count=0)

    # lock 1 is exclusive
    await lock_1.acquire_exclusive(0.5)
    lock_owner = await r.get("test_blocking_004:exclusive")
    assert lock_owner.decode() == lock_1._id

    # lock 2 is blocked
    with pytest.raises(CannotObtainLock):
        await lock_2.acquire_shared(0.5)

    # release lock 1
    await lock_1.release()

    # retry lock 2 successful
    await lock_2.acquire_shared(0.5)
    rank = await r.zrank("test_blocking_004:shared", lock_2._id)
    _log.debug("    - lock 2 rank: %r", rank)
    assert rank is not None

    # release lock 2
    await lock_2.release()

    # make sure it's gone
    assert await r.get("test_blocking_004:exclusive") is None
    assert await r.zcount("test_blocking_004:exclusive_waiting", "-inf", "+inf") == 0
    assert await r.zcount("test_blocking_004:shared", "-inf", "+inf") == 0


@pytest.mark.asyncio  # type: ignore[misc]
async def test_blocking_005(r, *args: Any, **kwargs: Any) -> None:
    """Shared lock blocking exclusive lock."""
    _log.info("test_blocking_005 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_blocking_005", retry_count=0)
    lock_2 = Multilock(r, "test_blocking_005", retry_count=0)

    # lock 1 is shared
    await lock_1.acquire_shared(0.5)
    rank = await r.zrank("test_blocking_005:shared", lock_1._id)
    _log.debug("    - lock 1 rank: %r", rank)

    # lock 2 is exclusive, blocked
    with pytest.raises(CannotObtainLock):
        await lock_2.acquire_exclusive(0.5)
    rank = await r.zrank("test_blocking_005:exclusive_waiting", lock_2._id)
    _log.debug("    - lock 2 rank: %r", rank)

    # release lock 1
    await lock_1.release()

    # retry lock 2 is now successful
    await lock_2.acquire_exclusive(0.5)
    lock_owner = await r.get("test_blocking_005:exclusive")
    assert lock_owner.decode() == lock_2._id

    # release lock 2
    await lock_2.release()

    # make sure it's gone
    assert await r.get("test_blocking_005:exclusive") is None
    assert await r.zcount("test_blocking_005:exclusive_waiting", "-inf", "+inf") == 0
    assert await r.zcount("test_blocking_005:shared", "-inf", "+inf") == 0


@pytest.mark.asyncio  # type: ignore[misc]
async def test_blocking_006(r, *args: Any, **kwargs: Any) -> None:
    """Shared locks blocking exclusive lock."""
    _log.info("test_blocking_006 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_blocking_006", retry_count=0)
    lock_2 = Multilock(r, "test_blocking_006", retry_count=0)
    lock_3 = Multilock(r, "test_blocking_006", retry_count=0)

    # lock 1 is shared
    await lock_1.acquire_shared(0.5)
    rank = await r.zrank("test_blocking_006:shared", lock_1._id)
    _log.debug("    - lock 1 shared rank: %r", rank)

    # lock 2 is shared with lock 1
    await lock_2.acquire_shared(0.5)
    rank = await r.zrank("test_blocking_006:shared", lock_2._id)
    _log.debug("    - lock 2 shared rank: %r", rank)

    # lock 3 is exclusive, blocked
    with pytest.raises(CannotObtainLock):
        await lock_3.acquire_exclusive(0.5)
    rank = await r.zrank("test_blocking_006:exclusive_waiting", lock_3._id)
    _log.debug("    - lock 3 exclusive waiting rank: %r", rank)

    # release lock 1
    await lock_1.release()

    # lock 2 still locked
    rank = await r.zrank("test_blocking_006:shared", lock_2._id)
    _log.debug("    - lock 2 shared rank: %r", rank)

    # lock 3 is still blocked
    with pytest.raises(CannotObtainLock):
        await lock_3.acquire_exclusive(0.5)

    # release lock 2
    await lock_2.release()

    # retry lock 3 is now successful
    await lock_3.acquire_exclusive(0.5)
    lock_owner = await r.get("test_blocking_006:exclusive")
    assert lock_owner.decode() == lock_3._id

    # release lock 3
    await lock_3.release()

    # make sure it's gone
    assert await r.get("test_blocking_006:exclusive") is None
    assert await r.zcount("test_blocking_006:exclusive_waiting", "-inf", "+inf") == 0
    assert await r.zcount("test_blocking_006:shared", "-inf", "+inf") == 0


@pytest.mark.asyncio  # type: ignore[misc]
async def test_blocking_007(r, *args: Any, **kwargs: Any) -> None:
    """Shared lock blocking exclusive lock, which is blocking another request
    for a shared lock."""
    _log.info("test_blocking_007 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_blocking_007", retry_count=0)
    lock_2 = Multilock(r, "test_blocking_007", retry_count=0)
    lock_3 = Multilock(r, "test_blocking_007", retry_count=0)

    # lock 1 is shared
    await lock_1.acquire_shared(0.5)
    rank = await r.zrank("test_blocking_007:shared", lock_1._id)
    _log.debug("    - lock 1 shared rank: %r", rank)

    # lock 2 is exclusive, blocked
    with pytest.raises(CannotObtainLock):
        await lock_2.acquire_exclusive(0.5)
    rank = await r.zrank("test_blocking_007:exclusive_waiting", lock_2._id)
    _log.debug("    - lock 2 exclusive waiting rank: %r", rank)

    # lock 3 is shared, blocked
    with pytest.raises(CannotObtainLock):
        await lock_3.acquire_shared(0.5)
    rank = await r.zrank("test_blocking_007:shared_waiting", lock_3._id)
    _log.debug("    - lock 3 shared waiting rank: %r", rank)

    # release lock 1
    await lock_1.release()

    # retry lock 2 is successful
    await lock_2.acquire_exclusive(0.5)
    lock_owner = await r.get("test_blocking_007:exclusive")
    assert lock_owner.decode() == lock_2._id

    # lock 3 is still blocked
    with pytest.raises(CannotObtainLock):
        await lock_3.acquire_shared(0.5)
    rank = await r.zrank("test_blocking_007:shared_waiting", lock_3._id)
    _log.debug("    - lock 3 shared waiting rank: %r", rank)

    # release lock 2
    await lock_2.release()

    # retry lock 3 successful
    await lock_3.acquire_shared(0.5)
    rank = await r.zrank("test_blocking_007:shared", lock_3._id)
    _log.debug("    - lock 3 shared rank: %r", rank)

    # release lock 3
    await lock_3.release()

    # make sure it's gone
    assert await r.get("test_blocking_007:exclusive") is None
    assert await r.zcount("test_blocking_007:exclusive_waiting", "-inf", "+inf") == 0
    assert await r.zcount("test_blocking_007:shared", "-inf", "+inf") == 0
