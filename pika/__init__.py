__version__ = '1.5.0'

import logging

# Add NullHandler before importing Pika modules to prevent logging warnings
logging.getLogger(__name__).addHandler(logging.NullHandler())

# ruff: noqa: E402

from pika import adapters, frame, spec
from pika.adapters import BaseConnection, BlockingConnection, Connection, SelectConnection
from pika.adapters.utils.connection_workflow import AMQPConnectionWorkflow
from pika.connection import ConnectionParameters, SSLOptions, URLParameters
from pika.credentials import PlainCredentials
from pika.delivery_mode import DeliveryMode
from pika.spec import BasicProperties
from pika.streaming_publish import (
    MessageBodySource,
    StreamingPublish,
    StreamingPublishState,
)

__all__ = [
    'AMQPConnectionWorkflow',
    'BaseConnection',
    'BasicProperties',
    'BlockingConnection',
    'Connection',
    'ConnectionParameters',
    'DeliveryMode',
    'MessageBodySource',
    'PlainCredentials',
    'SSLOptions',
    'SelectConnection',
    'StreamingPublish',
    'StreamingPublishState',
    'URLParameters',
    'adapters',
    'frame',
    'spec',
]
