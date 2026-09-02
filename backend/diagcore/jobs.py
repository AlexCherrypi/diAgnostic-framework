"""Core job engine (SPEC 7).

Jobs are ``job`` nodes under the well-known ``jobs`` node (newest first) with
``job.step`` child nodes. Named resource locks are acquired in sorted order
before the job starts; while waiting the job node stays ``init`` with
``props.waitingFor``. Cancellation is plain ``asyncio`` task cancellation and
surfaces as status ``cancelled``. A job failure marks the failing step AND the
job node ``error``; the engine itself never lets a job exception escape.

M0.5 (SPEC 14.1): the engine is the job seam — the full lifecycle (creation,
lock waits, start, steps, progress milestones at 25 % granularity,
cancellation, errors, completion) is logged automatically, category ``job``.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Sequence

from .loghub import LogHub, LogLevel
from .model import Action, CmdResult, ErrorInfo, Node
from .tree import StateTree

logger = logging.getLogger(__name__)

#: A job body: an async callable receiving the JobContext.
JobFactory = Callable[["JobContext"], Awaitable[Any]]

_ACTIVE_STATUSES = ("init", "running")
_TERMINAL_STATUSES = ("done", "error", "cancelled")

#: Terminal jobs kept under the jobs node; older ones are removed (SPEC 7).
DEFAULT_JOB_RETENTION = 50


@dataclass
class Job:
    id: str
    name: str
    lock_names: tuple[str, ...] = ()
    task: asyncio.Task[None] | None = field(default=None, repr=False)


class _StepContext:
    """Async context manager creating a running ``job.step`` node."""

    def __init__(self, ctx: JobContext, name: str) -> None:
        self._ctx = ctx
        self._name = name
        self.id: str | None = None

    async def __aenter__(self) -> _StepContext:
        ctx = self._ctx
        ctx._step_counter += 1
        step_id = f"{ctx.job.id}:step:{ctx._step_counter}"
        parent_id = ctx._stack[-1]
        ctx._tree.add(
            Node(
                id=step_id,
                type="job.step",
                name=self._name,
                status="running",
                owner="core",
                parentId=parent_id,
            )
        )
        ctx._stack.append(step_id)
        self.id = step_id
        ctx._log("info", f"job step {step_id} {self._name!r} started (job {ctx.job.id})", node_id=step_id)
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> bool:
        ctx = self._ctx
        ctx._stack.pop()
        assert self.id is not None
        if exc_type is None:
            ctx._tree.update(self.id, {"status": "done"})
            ctx._log("info", f"job step {self.id} {self._name!r} done", node_id=self.id)
        elif issubclass(exc_type, asyncio.CancelledError):
            ctx._tree.update(self.id, {"status": "cancelled"})
            ctx._log("info", f"job step {self.id} {self._name!r} cancelled", node_id=self.id)
        else:
            message = str(exc) or exc_type.__name__
            ctx._tree.update(self.id, {"status": "error", "error": {"message": message}})
            ctx._log(
                "error",
                f"job step {self.id} {self._name!r} failed: {message}",
                node_id=self.id,
                data={"error": {"message": message}},
            )
        return False  # always propagate


class JobContext:
    """Handed to the job coroutine; creates steps and reports progress."""

    def __init__(self, tree: StateTree, job: Job, loghub: LogHub | None = None) -> None:
        self._tree = tree
        self.job = job
        self._stack: list[str] = [job.id]
        self._step_counter = 0
        self._loghub = loghub
        self._milestone = 0  # last logged progress quarter (0..4)

    def step(self, name: str) -> _StepContext:
        """``async with ctx.step("..."):`` — creates a running step node,
        marks it done/error/cancelled on exit. Steps may nest."""
        return _StepContext(self, name)

    def set_progress(self, value: float) -> None:
        clamped = min(1.0, max(0.0, float(value)))
        self._tree.update(self.job.id, {"progress": clamped})
        quarter = int(clamped * 4)
        if quarter > self._milestone:
            self._milestone = quarter
            if quarter >= 1:
                self._log(
                    "info",
                    f"job {self.job.id} {self.job.name!r} progress {quarter * 25}% ({clamped:.2f})",
                    node_id=self.job.id,
                    data={"progress": clamped},
                )

    def _log(
        self,
        level: LogLevel,
        message: str,
        *,
        node_id: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        if self._loghub is not None:
            self._loghub.emit(level, "job", message, node_id=node_id, data=data)


class JobEngine:
    def __init__(
        self,
        tree: StateTree,
        jobs_node_id: str = "jobs",
        *,
        retention: int = DEFAULT_JOB_RETENTION,
        loghub: LogHub | None = None,
    ) -> None:
        self._tree = tree
        self._jobs_node_id = jobs_node_id
        self._retention = retention
        self._loghub = loghub
        self._locks: dict[str, asyncio.Lock] = {}
        self._jobs: dict[str, Job] = {}
        self._counter = 0

    def _log(self, level: LogLevel, message: str, *, node_id: str, data: dict[str, Any] | None = None) -> None:
        if self._loghub is not None:
            self._loghub.emit(level, "job", message, node_id=node_id, data=data)

    # -- public API --------------------------------------------------------

    def create(self, name: str, coro_factory: JobFactory, locks: Sequence[str] = ()) -> Job:
        """Create a job node (newest first under ``jobs``) and start it."""
        self._counter += 1
        job_id = f"job:{self._counter}"
        self._tree.add(
            Node(
                id=job_id,
                type="job",
                name=name,
                status="init",
                owner="core",
                parentId=self._jobs_node_id,
                actions=[Action(name="cancel", label="Cancel")],
            ),
            index=0,
        )
        job = Job(id=job_id, name=name, lock_names=tuple(sorted(set(locks))))
        self._log(
            "info",
            f"job {job_id} {name!r} created (locks={list(job.lock_names)})",
            node_id=job_id,
            data={"jobId": job_id, "name": name, "locks": list(job.lock_names)},
        )
        job.task = asyncio.get_running_loop().create_task(
            self._run(job, coro_factory), name=f"diagcore-job-{job_id}"
        )
        self._jobs[job_id] = job
        return job

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def cancel(self, job_id: str) -> bool:
        """Request cancellation; True if the job was still active."""
        job = self._jobs.get(job_id)
        node = self._tree.get(job_id)
        if job is None or job.task is None or node is None:
            return False
        if node.status not in _ACTIVE_STATUSES:
            return False
        self._log("info", f"job {job_id} {job.name!r} cancellation requested", node_id=job_id)
        job.task.cancel()
        return True

    async def handle_command(self, node: Node, action: str, params: dict[str, Any]) -> CmdResult:
        """Command handler for core-owned job nodes (action ``cancel``)."""
        if action != "cancel":
            return CmdResult(
                ok=False,
                error=ErrorInfo(message=f"unknown action {action!r} for job {node.id!r}"),
            )
        current = self._tree.get(node.id)
        if current is None or node.id not in self._jobs:
            return CmdResult(ok=False, error=ErrorInfo(message=f"unknown job: {node.id!r}"))
        if not self.cancel(node.id):
            return CmdResult(
                ok=False,
                error=ErrorInfo(message=f"job {node.id!r} is not cancellable (status {current.status!r})"),
            )
        return CmdResult(ok=True, data={})

    async def shutdown(self) -> None:
        """Cancel all still-running jobs and wait for them to finish."""
        tasks = [j.task for j in self._jobs.values() if j.task is not None and not j.task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # -- internals ---------------------------------------------------------

    async def _run(self, job: Job, coro_factory: JobFactory) -> None:
        held: list[asyncio.Lock] = []
        ctx = JobContext(self._tree, job, self._loghub)
        try:
            if job.lock_names:
                self._log(
                    "debug",
                    f"job {job.id} {job.name!r} waiting for locks {list(job.lock_names)}",
                    node_id=job.id,
                    data={"locks": list(job.lock_names)},
                )
                self._tree.merge_props(job.id, {"waitingFor": list(job.lock_names)})
                for lock_name in job.lock_names:
                    lock = self._locks.setdefault(lock_name, asyncio.Lock())
                    await lock.acquire()
                    held.append(lock)
                self._tree.merge_props(job.id, {"waitingFor": []})
            self._tree.update(job.id, {"status": "running", "progress": 0.0})
            self._log("info", f"job {job.id} {job.name!r} started", node_id=job.id)
            await coro_factory(ctx)
            self._safe_update(job.id, {"status": "done", "progress": 1.0, "actions": []})
            self._log("info", f"job {job.id} {job.name!r} done", node_id=job.id)
        except asyncio.CancelledError:
            self._safe_update(job.id, {"status": "cancelled", "actions": []})
            self._log("info", f"job {job.id} {job.name!r} cancelled", node_id=job.id)
        except Exception as exc:
            message = str(exc) or type(exc).__name__
            self._safe_update(
                job.id,
                {
                    "status": "error",
                    "error": {"message": message, "detail": traceback.format_exc()},
                    "actions": [],
                },
            )
            self._log(
                "error",
                f"job {job.id} {job.name!r} failed: {message}",
                node_id=job.id,
                data={"error": {"message": message, "detail": traceback.format_exc()}},
            )
        finally:
            for lock in reversed(held):
                lock.release()
            job.task = None  # release the finished Task object
            self._trim_terminal_jobs()

    def _trim_terminal_jobs(self) -> None:
        """Retention cap: keep the newest ``retention`` terminal jobs.

        Without a cap a long-running core accumulates job (and job.step)
        nodes plus Job bookkeeping forever, growing every hello/resync
        snapshot without bound. Active jobs are never touched.
        """
        jobs_node = self._tree.get(self._jobs_node_id)
        if jobs_node is None:
            return
        terminal: list[str] = []
        for child_id in jobs_node.children:  # newest first (insert at index 0)
            child = self._tree.get(child_id)
            if child is not None and child.status in _TERMINAL_STATUSES:
                terminal.append(child_id)
        for job_id in terminal[self._retention :]:
            try:
                self._tree.remove(job_id)
            except ValueError:
                pass
            self._jobs.pop(job_id, None)

    def _safe_update(self, node_id: str, changes: dict[str, Any]) -> None:
        try:
            self._tree.update(node_id, changes)
        except ValueError:
            logger.warning("job node %s vanished before final update", node_id)
