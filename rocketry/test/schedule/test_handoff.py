"""Regression tests for run-scoped handoff between starts and returns.

The scenario mirrors regional extraction/cleaning jobs: two regions
(north/south) run extraction and cleaning daily.  A cleaning run must
read the return of the *upstream run it accepted*, not whichever run of
the same task happens to have succeeded most recently.
"""

import asyncio
import logging
import time

import pytest
from redbird.logging import RepoHandler
from redbird.repos import MemoryRepo

from rocketry import Session
from rocketry.args import Return
from rocketry.conditions import DependSuccess
from rocketry.core.handoff import Handoff, RunReference
from rocketry.exc import RunNotAvailable, TaskSetupError
from rocketry.log import MinimalRecord, RunRecord
from rocketry.tasks import FuncTask


def get_label(task, params=None):
    data = params.to_dict() if hasattr(params, "to_dict") else dict(params or {})
    return str(data.get("label"))


def extract(label):
    return f"raw-{label}"


def make_session(execution="thread", model=MinimalRecord, run_id=True):
    session = Session(
        config={"execution": execution, "cycle_sleep": 0.001},
        delete_existing_loggers=True,
    )
    logger = logging.getLogger("rocketry.task")
    logger.handlers = [RepoHandler(repo=MemoryRepo(model=model))]
    logger.setLevel(logging.INFO)
    if run_id:
        session.config.func_run_id = get_label
    return session


def wait_until(predicate, timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.002)
    return predicate()


def run_thread(task):
    asyncio.run(task.start_async())
    for run in list(task._run_stack):
        if run.task is not None:
            run.task.join(timeout=2)
    time.sleep(0.02)


def test_ticket_is_bound_to_accepted_run_without_run_ids():
    """A1 is accepted, then B1 and A2 finish before cleaning starts.

    The cleaning run must still read A1 even though A2 is newer and
    B1 is another region's run.
    """
    session = make_session(execution="thread", model=MinimalRecord, run_id=False)
    seen = {}

    north = FuncTask(extract, name="north", execution="main", session=session)
    south = FuncTask(extract, name="south", execution="main", session=session)
    cleaning = FuncTask(
        lambda data: seen.setdefault("value", data),
        name="north cleaning",
        start_cond=DependSuccess(depend_task="north"),
        parameters={"data": Return("north")},
        execution="thread",
        session=session,
    )

    north.start({"label": "A1"})
    cleaning.run()
    ticket = cleaning.batches[0].to_dict()["_handoff_"]
    assert isinstance(ticket, Handoff)

    south.start({"label": "B1"})
    north.start({"label": "A2"})

    run_thread(cleaning)

    assert cleaning.status == "success"
    assert seen["value"] == "raw-A1"


def test_two_tickets_read_different_runs():
    """A queued A1 ticket and a supplemental A2 ticket stay separate."""
    session = make_session(execution="thread", model=RunRecord, run_id=True)
    seen = []

    north = FuncTask(extract, name="north", execution="main", session=session)
    south = FuncTask(extract, name="south", execution="main", session=session)
    cleaning = FuncTask(
        lambda data: seen.append(data),
        name="north cleaning",
        start_cond=DependSuccess(depend_task="north"),
        parameters={"data": Return("north")},
        execution="thread",
        session=session,
    )

    north.start({"label": "A1"})
    cleaning.run()
    assert cleaning.batches[0].to_dict()["_handoff_"].get_run_id("north") == "A1"

    south.start({"label": "B1"})
    north.start({"label": "A2"})

    # The cleaning was requested again, explicitly accepting A2.
    cleaning.run(_handoff_=Handoff({"north": "A2"}))
    assert cleaning.batches[1].to_dict()["_handoff_"].get_run_id("north") == "A2"

    run_thread(cleaning)
    run_thread(cleaning)

    assert cleaning.status == "success"
    assert seen == ["raw-A1", "raw-A2"]


def test_withdrawn_run_is_unavailable_and_does_not_fall_back():
    """A1 withdrawn before materializing: fail as unavailable."""
    session = make_session(execution="main", model=RunRecord, run_id=True)

    north = FuncTask(extract, name="north", execution="main", session=session)
    south = FuncTask(extract, name="south", execution="main", session=session)
    cleaning = FuncTask(
        lambda data: None,
        name="north cleaning",
        start_cond=DependSuccess(depend_task="north"),
        parameters={"data": Return("north")},
        execution="main",
        session=session,
    )

    north.start({"label": "A1"})
    cleaning.run()
    assert cleaning.batches[0].to_dict()["_handoff_"].get_run_id("north") == "A1"

    # A1 is withdrawn (file validation failed); B1 and A2 appear later.
    north.withdraw_run("A1", reason="file rejected")
    south.start({"label": "B1"})
    north.start({"label": "A2"})

    with pytest.raises(TaskSetupError) as exc_info:
        asyncio.run(cleaning.start_async())

    assert isinstance(exc_info.value.__cause__, RunNotAvailable)
    assert "A1" in str(exc_info.value.__cause__)
    assert cleaning.status == "fail"


def test_withdrawn_after_materialization_raises_run_not_available():
    session = make_session(execution="main", model=RunRecord, run_id=True)
    north = FuncTask(extract, name="north", execution="main", session=session)

    north.start({"label": "A1"})
    session.returns.withdraw("north", "A1", reason="file rejected")

    with pytest.raises(RunNotAvailable):
        Return("north").get_value(
            session=session,
            _handoff_=Handoff(RunReference("north", "A1")),
        )


def test_ticket_withdrawal_is_visible_on_queued_batch():
    session = make_session(execution="main", model=RunRecord, run_id=True)
    north = FuncTask(extract, name="north", execution="main", session=session)
    cleaning = FuncTask(
        lambda data: None,
        name="north cleaning",
        start_cond=DependSuccess(depend_task="north"),
        parameters={"data": Return("north")},
        execution="main",
        session=session,
    )

    north.start({"label": "A1"})
    cleaning.run()
    ticket = cleaning.batches[0].to_dict()["_handoff_"]

    cleaning.withdraw_handoff(reason="file rejected")
    ticket = cleaning.batches[0].to_dict()["_handoff_"]

    assert ticket.get_reference("north") is None
    assert ticket.get_withdrawn("north") is not None
    assert session.returns.is_available("north", "A1") is False


def test_legacy_return_still_reads_latest_success():
    """Without a ticket, Return keeps its pre-existing behavior."""
    session = make_session(execution="main", model=MinimalRecord, run_id=False)
    north = FuncTask(extract, name="north", execution="main", session=session)

    north.start({"label": "A1"})
    north.start({"label": "A2"})

    assert Return("north").get_value(session=session) == "raw-A2"
    assert session.returns[north] == "raw-A2"


def test_returns_are_stored_per_run():
    session = make_session(execution="main", model=RunRecord, run_id=True)
    north = FuncTask(extract, name="north", execution="main", session=session)

    north.start({"label": "A1"})
    north.start({"label": "A2"})

    assert session.returns.get_value("north", run_id="A1") == "raw-A1"
    assert session.returns.get_value("north", run_id="A2") == "raw-A2"
    # The legacy task-keyed view remains the latest value.
    assert session.returns[north] == "raw-A2"
