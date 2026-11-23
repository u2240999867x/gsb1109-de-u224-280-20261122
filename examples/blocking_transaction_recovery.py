"""Recoverable hand-off of one original as two messages in one transaction.

An archiver splits each original into an index message and a cold-storage
request, and confirms the original only after both are handed off. Both
publishes go out in a single broker transaction on one channel, so they land
together or not at all, and both carry the original's stable business id as
`message_id`, which each downstream can answer a durable lookup by.

The transaction result is recoverable rather than fire-and-forget:

- `tx_commit` returns CommitOk: the transaction committed. CommitOk is the
  transaction result, settling both messages at once, not a per-message
  publish confirm.
- The connection drops before the commit is sent: the broker discards the
  open transaction, nothing is persisted, and the original is redelivered.
  Resubmitting under the same business id is safe.
- The connection drops after the commit is sent but before CommitOk arrives:
  the outcome is unknown. Recover by asking both downstreams for evidence,
  and report the batch complete only when both have it. While evidence is
  incomplete the batch keeps a queryable `pending` status keyed by the
  business id, for a later recheck or an idempotent resubmit.

Run against a local broker (see examples/README.md):

    python examples/blocking_transaction_recovery.py
"""

import pika

INDEX_QUEUE = 'archive.index'
COLD_QUEUE = 'archive.coldstore'

# Ledger statuses, keyed by the original's business id. A real archiver keeps
# the ledger in a database so it survives a restart; a dict shows the same
# transitions.
PENDING = 'pending'  # commit outcome unknown, evidence incomplete
RESUBMIT = 'resubmit'  # known not committed; send the same business id again
COMMITTED = 'committed'  # CommitOk received
COMPLETE = 'complete'  # both downstreams show durable evidence


def publish_pair(channel, business_id):
    """Publish the index message and cold-storage request of one original."""
    properties = pika.BasicProperties(
        message_id=business_id,
        delivery_mode=pika.DeliveryMode.Persistent)
    channel.basic_publish(exchange='',
                          routing_key=INDEX_QUEUE,
                          body=f'index of {business_id}'.encode(),
                          properties=properties)
    channel.basic_publish(exchange='',
                          routing_key=COLD_QUEUE,
                          body=f'cold-store request {business_id}'.encode(),
                          properties=properties)


def transfer(channel, business_id, ledger):
    """Hand one original to both downstreams as a single transaction.

    Records and returns the batch status: COMMITTED when the broker answers
    CommitOk, RESUBMIT when the hand-off fails before the commit is sent
    (the broker discards the open transaction, so the original is redelivered
    and the same business id can be sent again), or PENDING when the commit
    was sent but its result never arrived.
    """
    channel.tx_select()
    try:
        publish_pair(channel, business_id)
    except pika.exceptions.AMQPError:
        ledger[business_id] = RESUBMIT
        return RESUBMIT
    try:
        channel.tx_commit()
    except pika.exceptions.AMQPError:
        # The commit may or may not have been processed; only downstream
        # evidence settles it.
        ledger[business_id] = PENDING
        return PENDING
    ledger[business_id] = COMMITTED
    return COMMITTED


def downstream_has(channel, queue, business_id):
    """Whether *queue* holds evidence of *business_id*, consuming nothing."""
    seen = set()
    while True:
        method, properties, _body = channel.basic_get(queue=queue,
                                                      auto_ack=False)
        if method is None:
            return False
        channel.basic_nack(method.delivery_tag, requeue=True)
        if properties.message_id == business_id:
            return True
        # A requeued message comes around again; stop once the scan repeats.
        if properties.message_id in seen:
            return False
        seen.add(properties.message_id)


def reconcile(channel, business_id, ledger):
    """Settle a batch from downstream evidence instead of the broker's reply.

    Returns COMPLETE once both downstreams show durable evidence of the
    business id. Otherwise the batch keeps its status (PENDING, or RESUBMIT
    when it is already known to be uncommitted), so it stays queryable by
    business id for a later recheck or an idempotent resubmit.
    """
    if (downstream_has(channel, INDEX_QUEUE, business_id) and
            downstream_has(channel, COLD_QUEUE, business_id)):
        ledger[business_id] = COMPLETE
    else:
        ledger.setdefault(business_id, PENDING)
    return ledger[business_id]


def main():
    connection = pika.BlockingConnection()
    channel = connection.channel()
    # Durable queues and persistent messages: evidence has to survive a
    # broker restart to be worth reconciling against.
    channel.queue_declare(queue=INDEX_QUEUE, durable=True)
    channel.queue_declare(queue=COLD_QUEUE, durable=True)
    # Evidence lookups run on their own channel: on a channel in transaction
    # mode the requeueing nacks below would only take effect on commit.
    evidence = connection.channel()

    ledger = {}

    # CommitOk arrives: both messages are handed off, the original can be
    # confirmed, and the evidence check agrees.
    print('ORD-0001 transfer:', transfer(channel, 'ORD-0001', ledger))
    print('ORD-0001 reconcile:', reconcile(evidence, 'ORD-0001', ledger))

    # Lost before the commit: the open transaction is discarded. Driven here
    # as an explicit rollback so the example is deterministic; a dropped
    # connection makes the broker discard it the same way. No evidence shows
    # up, and the same business id is resubmitted.
    channel.tx_select()
    publish_pair(channel, 'ORD-0002')
    channel.tx_rollback()
    ledger['ORD-0002'] = RESUBMIT
    print('ORD-0002 reconcile:', reconcile(evidence, 'ORD-0002', ledger))
    print('ORD-0002 transfer:', transfer(channel, 'ORD-0002', ledger))
    print('ORD-0002 reconcile:', reconcile(evidence, 'ORD-0002', ledger))

    # Lost after the commit was sent: the worker's CommitOk never reached the
    # ledger, which is the state a crash after sending the commit leaves
    # behind. The evidence shows the commit did land.
    worker = pika.BlockingConnection()
    worker_channel = worker.channel()
    worker_channel.tx_select()
    publish_pair(worker_channel, 'ORD-0003')
    worker_channel.tx_commit()
    worker.close()
    ledger['ORD-0003'] = PENDING
    print('ORD-0003 reconcile:', reconcile(evidence, 'ORD-0003', ledger))

    # Evidence that has not shown up is not proof of loss: the batch stays
    # pending and queryable by its business id until a later recheck or an
    # idempotent resubmit settles it.
    ledger['ORD-0004'] = PENDING
    print('ORD-0004 reconcile:', reconcile(evidence, 'ORD-0004', ledger))

    print('ledger:', ledger)
    connection.close()


if __name__ == '__main__':
    main()
