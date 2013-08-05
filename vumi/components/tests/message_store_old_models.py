"""Previous versions of message store models."""


from vumi.message import TransportUserMessage
from vumi.persist.model import Model
from vumi.persist.fields import (
    VumiMessage, ForeignKey, ListOf, Dynamic, Tag, Unicode)


class BatchVNone(Model):
    # key is batch_id
    tags = ListOf(Tag())
    metadata = Dynamic(Unicode())


class OutboundMessageVNone(Model):
    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batch = ForeignKey(BatchVNone, null=True)


class InboundMessageVNone(Model):
    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batch = ForeignKey(BatchVNone, null=True)
