import datetime

import pytest

from rocketry.conds import time_of_day, false, scheduler_cycles
from rocketry.conditions import TaskSucceeded, TaskFailed, FuncCond
from rocketry.tasks import FuncTask
from rocketry.testing.log import create_task_record
from rocketry.time import TimeOfDay


def to_timestamp(dt_string):
    return datetime.datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S.%f").timestamp()


class Clock:
    def __init__(self, now):
        self.now = now

    def get_time(self):
        return self.now

    def set(self, dt_string):
        self.now = to_timestamp(dt_string)


@pytest.mark.parametrize("halt_midcycle", [False, True])
@pytest.mark.parametrize("force_status_from_logs", [False, True])
def test_cycle_shared_business_time(session, force_status_from_logs, halt_midcycle):
    """All tasks in a scheduler cycle must use the business time read
    at the beginning of the cycle. Log records are judged by what is
    written to the log repo; a halt switch is still read live."""
    session.config.execution = "main"
    session.config.force_status_from_logs = force_status_from_logs

    day = "2026-11-28"
    clock = Clock(to_timestamp(f"{day} 17:59:59.0"))
    session.config.time_func = clock.get_time

    # The receipt task (only logged to, never run by the scheduler)
    inbound = FuncTask(lambda: None, name="inbound", start_cond=false, session=session)

    runs = []
    halt = {"on": False}

    today = TimeOfDay(None, None)
    inbound_success = TaskSucceeded(task="inbound", period=today)
    inbound_failure = TaskFailed(task="inbound", period=today)

    def make_runner(name):
        def run():
            runs.append((name, session.scheduler.n_cycles))
        return run

    # Release: only before 18:00, exactly one inbound success and no failure today
    release = FuncTask(
        make_runner("release"), name="release", priority=30,
        start_cond=time_of_day.before("18:00") & (inbound_success == 1) & (inbound_failure == 0),
        session=session,
    )

    halt_switch = FuncCond(lambda: halt["on"])

    # Recollect: from 18:00 with an inbound failure, or immediately when halted
    recollect = FuncTask(
        make_runner("recollect"), name="recollect", priority=10,
        start_cond=(time_of_day.after("18:00") & inbound_failure) | halt_switch,
        session=session,
    )

    # Real-time alert using the very same failure condition object
    alert = FuncTask(
        make_runner("alert"), name="alert", priority=0,
        start_cond=inbound_failure,
        session=session,
    )

    # Simulates the clock and the log repo between the release check
    # and the recollect check of the first cycle
    advanced = {"done": False}

    def advance():
        if advanced["done"]:
            return
        advanced["done"] = True
        clock.set(f"{day} 18:00:00.5")
        inbound.log_record(create_task_record(task_name="inbound", action="fail", created=f"{day} 17:58:00"))
        if halt_midcycle:
            halt["on"] = True

    FuncTask(
        advance, name="advancer", priority=20,
        start_cond=FuncCond(lambda: not advanced["done"]),
        session=session,
    )

    # Before the first cycle the log repo only has the 17:55 success
    inbound.log_record(create_task_record(task_name="inbound", action="success", created=f"{day} 17:55:00"))

    session.config.shut_cond = scheduler_cycles(more_than=1)
    session.start()

    # First cycle (business time 17:59:59): release starts, recollect does
    # not (unless halted) and the real-time alert sees the fresh failure
    assert ("release", 0) in runs
    assert ("alert", 0) in runs
    if halt_midcycle:
        assert ("recollect", 0) in runs
    else:
        assert ("recollect", 0) not in runs

    # Second cycle (business time past 18:00): release does not start,
    # recollect starts due to the failure arrived in the previous cycle
    assert ("release", 1) not in runs
    assert ("recollect", 1) in runs

    # The success count of the inbound task is untouched
    assert inbound.logger.filter_by(action="success").count() == 1
