"""Assorted core transports.

This is where all transports that are part of core vumi live.

.. note::
   Anything in :mod:`vumi.workers` is deprecated and needs to be migrated.
"""

from vumi.transports.base import Transport
from vumi.transports.failures import FailureWorker

__all__ = ['Transport', 'FailureWorker']
