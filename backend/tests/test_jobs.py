"""Job engine (SPEC 7): success, failure, cancel, locks."""

from __future__ import annotations

import asyncio

import pytest

from diagcore.jobs import JobContext, JobEngine
from diagcore.tree import StateTree

from util import make_base_tree, wait_until


@pytest.fixture
def tree() -> StateTree:
    return make_base_tree()


@pytest.fixture
def engine(tree: StateTree) -> JobEngine:
    return JobEngine(tree)


def status(tree: StateTree, node_id: str) -> str | None:
    node = tree.get(node_id)
    return node.status if node else None


# -- success ----------------------------------------------------------------


async def test_success_with_steps(tree: StateTree, engine: JobEngine) -> None:
    async def body(ctx: JobContext) -> None:
        async with ctx.step("one"):
            ctx.set_progress(0.5)
        async with ctx.step("two"):
            pass

    job = engine.create("demo", body)
    node = tree.get(job.id)
    assert node.status == "init"
    assert node.parentId == "jobs"
    assert [a.name for a in node.actions] == ["cancel"]

    await wait_until(lambda: status(tree, job.id) == "done", message="job done")
    node = tree.get(job.id)
    assert node.progress == 1.0
    assert node.actions == []  # cancel offered only while init/running
    assert node.children == [f"{job.id}:step:1", f"{job.id}:step:2"]
    assert status(tree, f"{job.id}:step:1") == "done"
    assert status(tree, f"{job.id}:step:2") == "done"
    step = tree.get(f"{job.id}:step:1")
    assert step.type == "job.step"
    assert step.parentId == job.id


async def test_jobs_inserted_newest_first(tree: StateTree, engine: JobEngine) -> None:
    async def body(ctx: JobContext) -> None:
        pass

    j1 = engine.create("first", body)
    j2 = engine.create("second", body)
    assert tree.get("jobs").children == [j2.id, j1.id]
    await wait_until(lambda: status(tree, j1.id) == "done" and status(tree, j2.id) == "done")


async def test_set_progress_clamps(tree: StateTree, engine: JobEngine) -> None:
    async def body(ctx: JobContext) -> None:
        ctx.set_progress(2.5)
        assert tree.get(ctx.job.id).progress == 1.0
        ctx.set_progress(-1)
        assert tree.get(ctx.job.id).progress == 0.0

    job = engine.create("clamp", body)
    await wait_until(lambda: status(tree, job.id) == "done")


# -- failure ----------------------------------------------------------------


async def test_failure_marks_job_and_failing_step(tree: StateTree, engine: JobEngine) -> None:
    async def body(ctx: JobContext) -> None:
        async with ctx.step("ok step"):
            pass
        async with ctx.step("bad step"):
            raise RuntimeError("exploded")

    job = engine.create("failing", body)
    await wait_until(lambda: status(tree, job.id) == "error", message="job error")
    node = tree.get(job.id)
    assert node.error.message == "exploded"
    assert "RuntimeError" in node.error.detail
    assert status(tree, f"{job.id}:step:1") == "done"
    step2 = tree.get(f"{job.id}:step:2")
    assert step2.status == "error"
    assert step2.error.message == "exploded"


async def test_nested_step_failure_propagates_to_outer_step(tree: StateTree, engine: JobEngine) -> None:
    async def body(ctx: JobContext) -> None:
        async with ctx.step("outer"):
            async with ctx.step("inner"):
                raise ValueError("deep failure")

    job = engine.create("nested", body)
    await wait_until(lambda: status(tree, job.id) == "error")
    outer = tree.get(f"{job.id}:step:1")
    inner = tree.get(f"{job.id}:step:2")
    assert inner.parentId == outer.id  # nesting => parent/child
    assert inner.status == "error"
    assert outer.status == "error"


# -- cancel -----------------------------------------------------------------


async def test_cancel_running_job_and_step(tree: StateTree, engine: JobEngine) -> None:
    started = asyncio.Event()

    async def body(ctx: JobContext) -> None:
        async with ctx.step("long"):
            started.set()
            await asyncio.sleep(30)

    job = engine.create("cancellable", body)
    await started.wait()
    result = await engine.handle_command(tree.get(job.id), "cancel", {})
    assert result.ok
    await wait_until(lambda: status(tree, job.id) == "cancelled", message="job cancelled")
    assert status(tree, f"{job.id}:step:1") == "cancelled"
    assert tree.get(job.id).actions == []


async def test_cancel_while_waiting_for_lock(tree: StateTree, engine: JobEngine) -> None:
    release = asyncio.Event()

    async def holder(ctx: JobContext) -> None:
        await release.wait()

    async def waiter(ctx: JobContext) -> None:
        pass

    j1 = engine.create("holder", holder, locks=["res"])
    await wait_until(lambda: status(tree, j1.id) == "running")
    j2 = engine.create("waiter", waiter, locks=["res"])
    await wait_until(lambda: tree.get(j2.id).props.get("waitingFor") == ["res"])
    assert status(tree, j2.id) == "init"

    result = await engine.handle_command(tree.get(j2.id), "cancel", {})
    assert result.ok
    await wait_until(lambda: status(tree, j2.id) == "cancelled")

    release.set()
    await wait_until(lambda: status(tree, j1.id) == "done")


async def test_cancel_terminal_job_fails(tree: StateTree, engine: JobEngine) -> None:
    async def body(ctx: JobContext) -> None:
        pass

    job = engine.create("quick", body)
    await wait_until(lambda: status(tree, job.id) == "done")
    result = await engine.handle_command(tree.get(job.id), "cancel", {})
    assert not result.ok
    assert "not cancellable" in result.error.message


async def test_unknown_action_fails(tree: StateTree, engine: JobEngine) -> None:
    async def body(ctx: JobContext) -> None:
        await asyncio.sleep(0.05)

    job = engine.create("j", body)
    result = await engine.handle_command(tree.get(job.id), "explode", {})
    assert not result.ok
    assert "unknown action" in result.error.message
    await wait_until(lambda: status(tree, job.id) == "done")


# -- locks ------------------------------------------------------------------


async def test_lock_exclusivity_and_waiting_for(tree: StateTree, engine: JobEngine) -> None:
    release = asyncio.Event()
    running: list[str] = []

    async def first(ctx: JobContext) -> None:
        running.append("first")
        await release.wait()

    async def second(ctx: JobContext) -> None:
        # first must be completely done before we run
        assert status(tree, j1.id) == "done"
        running.append("second")

    j1 = engine.create("first", first, locks=["bus:x"])
    await wait_until(lambda: status(tree, j1.id) == "running")
    j2 = engine.create("second", second, locks=["bus:x"])
    await wait_until(lambda: tree.get(j2.id).props.get("waitingFor") == ["bus:x"])
    assert status(tree, j2.id) == "init"
    assert running == ["first"]

    release.set()
    await wait_until(lambda: status(tree, j2.id) == "done", message="second job done")
    assert running == ["first", "second"]
    assert tree.get(j2.id).props.get("waitingFor") == []


async def test_locks_acquired_in_sorted_order(tree: StateTree, engine: JobEngine) -> None:
    release = asyncio.Event()

    async def hold_a(ctx: JobContext) -> None:
        await release.wait()

    async def wants_both(ctx: JobContext) -> None:
        pass

    async def wants_b(ctx: JobContext) -> None:
        pass

    # j1 holds "a"; j2 asks for {"b", "a"} (passed unsorted) and must block on
    # "a" FIRST (sorted order), leaving "b" free for j3.
    j1 = engine.create("hold-a", hold_a, locks=["a"])
    await wait_until(lambda: status(tree, j1.id) == "running")
    j2 = engine.create("wants-both", wants_both, locks=["b", "a"])
    await wait_until(lambda: tree.get(j2.id).props.get("waitingFor") == ["a", "b"])  # sorted
    j3 = engine.create("wants-b", wants_b, locks=["b"])
    await wait_until(lambda: status(tree, j3.id) == "done", message="j3 done while j2 blocked")
    assert status(tree, j2.id) == "init"

    release.set()
    await wait_until(lambda: status(tree, j2.id) == "done")


async def test_terminal_job_retention_cap(tree: StateTree) -> None:
    engine = JobEngine(tree, retention=2)

    async def body(ctx: JobContext) -> None:
        pass

    jobs = [engine.create(f"j{i}", body) for i in range(4)]
    await wait_until(
        lambda: tree.get("jobs").children == [jobs[3].id, jobs[2].id],
        message="oldest terminal jobs trimmed",
    )
    for old in jobs[:2]:
        assert tree.get(old.id) is None
        assert engine.get(old.id) is None
    for kept in jobs[2:]:
        assert tree.get(kept.id).status == "done"


async def test_shutdown_cancels_running_jobs(tree: StateTree, engine: JobEngine) -> None:
    async def body(ctx: JobContext) -> None:
        await asyncio.sleep(30)

    job = engine.create("stuck", body)
    await wait_until(lambda: status(tree, job.id) == "running")
    await engine.shutdown()
    assert status(tree, job.id) == "cancelled"
