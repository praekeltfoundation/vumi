"""Previous versions of message store models."""


from vumi.message import TransportUserMessage
from vumi.persist.model import Model
from vumi.persist.fields import (
    VumiMessage, ForeignKey, ListOf, Dynamic, Tag, Unicode, ManyToMany)
from vumi.components.message_store_migrators import (
    InboundMessageMigrator, OutboundMessageMigrator)


class BatchVNone(Model):
    bucket = 'batch'

    # key is batch_id
    tags = ListOf(Tag())
    metadata = Dynamic(Unicode())


class OutboundMessageVNone(Model):
    bucket = 'outboundmessage'

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batch = ForeignKey(BatchVNone, null=True)


class InboundMessageVNone(Model):
    bucket = 'inboundmessage'

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batch = ForeignKey(BatchVNone, null=True)


class OutboundMessageV1(Model):
    bucket = 'outboundmessage'

    VERSION = 1
    MIGRATOR = OutboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(BatchVNone)


class InboundMessageV1(Model):
    bucket = 'inboundmessage'

    VERSION = 1
    MIGRATOR = InboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(BatchVNone)
