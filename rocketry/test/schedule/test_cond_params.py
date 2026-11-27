import asyncio
import logging
import threading
import time

from redbird.logging import RepoHandler
from redbird.repos import MemoryRepo

from rocketry.args import Arg, FuncArg
from rocketry.core import BaseCondition
from rocketry.log.log_record import MinimalRunRecord
from rocketry.tasks import FuncTask


class QuoteProvider:
    "Provides quotes per market and records every fetch"

    def __init__(self):
        self.quotes = {}
        self.calls = []

    def set_quote(self, market, value):
        self.quotes[market] = value

    def get_quote(self, market):
        self.calls.append((market, threading.current_thread().name))
        return self.quotes[market]


class SourceFiles:
    ready = False


def quote_files_cond(param, files):
    "Condition that reads a quote and requires the source files"
    class QuoteFilesCond(BaseCondition):
        def get_state(self, quote=Arg(param)):
            return files.ready and quote is not None
    return QuoteFilesCond()


def wait_finish(task, timeout=5):
    deadline = time.time() + timeout
    while task.is_alive() and time.time() < deadline:
        time.sleep(0.01)
    assert not task.is_alive(), f"Task '{task.name}' did not finish"


def test_start_cond_params_reused_in_async_run(session):
    # Spot collection: conversion price and reconciliation baseline
    # are two Arg aliases to the same session level FuncArg
    provider = QuoteProvider()
    files = SourceFiles()
    files.ready = False
    session.parameters['prod_quote'] = FuncArg(provider.get_quote, 'PROD')

    received = {}
    def spot_collect(conv_price, recon_baseline):
        received['conv_price'] = conv_price
        received['recon_baseline'] = recon_baseline

    task = FuncTask(
        spot_collect,
        name='spot_collect',
        execution='async',
        parameters={
            'conv_price': Arg('prod_quote'),
            'recon_baseline': Arg('prod_quote'),
        },
        start_cond=quote_files_cond('prod_quote', files),
        session=session,
    )

    # First check reads P1 but the source files are missing: no start
    provider.set_quote('PROD', 'P1')
    assert not task.is_runnable()

    # Next check reads P2 and lets the task start
    provider.set_quote('PROD', 'P2')
    files.ready = True
    assert task.is_runnable()

    # The production quote moves on before the task picks its parameters
    provider.set_quote('PROD', 'P3')

    async def run_task():
        await task.start_async()
        for run in list(task._run_stack):
            if run.task is not None:
                await run.task
    asyncio.run(run_task())

    # The run gets the quote the passing condition check was made with
    assert received == {'conv_price': 'P2', 'recon_baseline': 'P2'}
    # The provider was asked only in the two condition checks
    assert provider.calls == [
        ('PROD', threading.main_thread().name),
        ('PROD', threading.main_thread().name),
    ]


def test_start_cond_params_reused_in_thread_run(session):
    # Threaded backfill: conversion uses the trial quote, a reference
    # market quote is fetched independently and the reconciliation
    # baseline is explicitly set to None in the task parameters
    provider = QuoteProvider()
    files = SourceFiles()
    files.ready = True

    def get_batch_no():
        if threading.current_thread() is threading.main_thread():
            raise RuntimeError("Batch number only exists in the receiver thread")
        provider.calls.append(('BATCH', threading.current_thread().name))
        return 'BATCH-7'

    def old_baseline():
        raise RuntimeError("Old baseline must never be called")

    def unaccepted_pre():
        raise RuntimeError("Param not accepted by the task must never be called")

    session.parameters['trial_quote'] = FuncArg(provider.get_quote, 'TRIAL')
    session.parameters['ref_quote'] = FuncArg(provider.get_quote, 'REF')
    session.parameters['recon_baseline'] = FuncArg(old_baseline)
    session.parameters['unaccepted_pre'] = FuncArg(unaccepted_pre, materialize='pre')

    received = {}
    def backfill(conv_price, ref_price, recon_baseline, batch_no):
        received['conv_price'] = conv_price
        received['ref_price'] = ref_price
        received['recon_baseline'] = recon_baseline
        received['batch_no'] = batch_no

    task = FuncTask(
        backfill,
        name='backfill',
        execution='thread',
        parameters={
            'conv_price': Arg('trial_quote'),
            'ref_price': Arg('ref_quote'),
            'recon_baseline': None,
            'batch_no': FuncArg(get_batch_no, materialize='post'),
        },
        start_cond=quote_files_cond('trial_quote', files),
        session=session,
    )

    # The condition check reads S7 and lets the task start
    provider.set_quote('TRIAL', 'S7')
    provider.set_quote('REF', 'R4')
    assert task.is_runnable()

    # The trial quote moves on before the thread picks its parameters
    provider.set_quote('TRIAL', 'S8')

    task.start()
    wait_finish(task)

    assert task.status == 'success'
    assert received == {
        'conv_price': 'S7',
        'ref_price': 'R4',
        'recon_baseline': None,
        'batch_no': 'BATCH-7',
    }
    # The trial quote was fetched only in the condition check, the
    # reference quote was fetched once on its own and the batch
    # number was fetched in the receiver thread
    assert [market for market, _ in provider.calls] == ['TRIAL', 'REF', 'BATCH']
    assert provider.calls[1][1] == threading.main_thread().name
    assert provider.calls[2][1] != threading.main_thread().name


def test_manual_run_skips_cond_and_failed_params_are_not_reused(session):
    provider = QuoteProvider()
    files = SourceFiles()
    files.ready = True
    batch_received = threading.Event()
    batch_received.set()

    def get_batch_no():
        provider.calls.append(('BATCH', threading.current_thread().name))
        if not batch_received.is_set():
            raise RuntimeError("No batch received by the worker")
        return 'BATCH-9'

    session.parameters['trial_quote'] = FuncArg(provider.get_quote, 'TRIAL')

    received = {}
    def backfill(conv_price, batch_no):
        received['conv_price'] = conv_price
        received['batch_no'] = batch_no

    task = FuncTask(
        backfill,
        name='backfill',
        execution='thread',
        parameters={
            'conv_price': Arg('trial_quote'),
            'batch_no': FuncArg(get_batch_no, materialize='post'),
        },
        start_cond=quote_files_cond('trial_quote', files),
        session=session,
    )

    repo = MemoryRepo(model=MinimalRunRecord)
    handler = RepoHandler(repo=repo)
    logger = logging.getLogger(session.config.task_logger_basename)
    logger.addHandler(handler)
    try:
        # A condition triggered run with S7
        provider.set_quote('TRIAL', 'S7')
        assert task.is_runnable()
        task.start()
        wait_finish(task)
        assert received == {'conv_price': 'S7', 'batch_no': 'BATCH-9'}
        received.clear()

        # Manual backfill for which the batch number cannot be fetched
        provider.set_quote('TRIAL', 'S8')
        batch_received.clear()
        task.run()
        task.start()
        wait_finish(task)

        # The file processing function was not executed but the
        # failure is logged for this specific run
        assert received == {}
        assert task.status == 'fail'
        records = repo.filter_by(task_name='backfill').all()
        run_record = [rec for rec in records if rec.action == 'run'][-1]
        fail_record = [rec for rec in records if rec.action == 'fail'][-1]
        assert run_record.run_id is not None
        assert fail_record.run_id == run_record.run_id

        # Next manual backfill gets the batch number and the trial
        # quote of that moment; nothing is reused from the condition
        # check or from the failed run
        provider.set_quote('TRIAL', 'S9')
        batch_received.set()
        task.run()
        task.start()
        wait_finish(task)

        assert task.status == 'success'
        assert received == {'conv_price': 'S9', 'batch_no': 'BATCH-9'}
        assert [market for market, _ in provider.calls] == [
            'TRIAL', # condition check (S7)
            'BATCH', # condition triggered run
            'TRIAL', # failed manual run (S8)
            'BATCH', # failed manual run (unavailable)
            'TRIAL', # successful manual run (S9)
            'BATCH', # successful manual run
        ]
    finally:
        logger.removeHandler(handler)
