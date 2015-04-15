"""Previous versions of message store models."""


from vumi.message import TransportUserMessage, TransportEvent, format_vumi_date
from vumi.persist.model import Model
from vumi.persist.fields import (
    VumiMessage, ForeignKey, ListOf, Dynamic, Tag, Unicode, ManyToMany)
from vumi.components.message_store_migrators import (
    InboundMessageMigrator, OutboundMessageMigrator, EventMigrator)


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


class EventVNone(Model):
    bucket = 'event'

    # key is event_id
    event = VumiMessage(TransportEvent)
    message = ForeignKey(OutboundMessageVNone)


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


class OutboundMessageV2(Model):
    bucket = 'outboundmessage'

    VERSION = 2
    MIGRATOR = OutboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(BatchVNone)

    # Extra fields for compound indexes
    batches_with_timestamps = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        batches_with_timestamps = []
        timestamp = self.msg['timestamp']
        for batch_id in self.batches.keys():
            batches_with_timestamps.append(u"%s$%s" % (batch_id, timestamp))
        self.batches_with_timestamps = batches_with_timestamps
        return super(OutboundMessageV2, self).save()


class InboundMessageV2(Model):
    bucket = 'inboundmessage'

    VERSION = 2
    MIGRATOR = InboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(BatchVNone)

    # Extra fields for compound indexes
    batches_with_timestamps = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        batches_with_timestamps = []
        timestamp = self.msg['timestamp']
        for batch_id in self.batches.keys():
            batches_with_timestamps.append(u"%s$%s" % (batch_id, timestamp))
        self.batches_with_timestamps = batches_with_timestamps
        return super(InboundMessageV2, self).save()


class OutboundMessageV3(Model):
    bucket = 'outboundmessage'

    VERSION = 3
    MIGRATOR = OutboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(BatchVNone)

    # Extra fields for compound indexes
    batches_with_timestamps = ListOf(Unicode(), index=True)
    batches_with_addresses = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        batches_with_timestamps = []
        batches_with_addresses = []
        timestamp = format_vumi_date(self.msg['timestamp'])
        for batch_id in self.batches.keys():
            batches_with_timestamps.append(u"%s$%s" % (batch_id, timestamp))
            batches_with_addresses.append(
                u"%s$%s$%s" % (batch_id, timestamp, self.msg['to_addr']))
        self.batches_with_timestamps = batches_with_timestamps
        self.batches_with_addresses = batches_with_addresses
        return super(OutboundMessageV3, self).save()


class InboundMessageV3(Model):
    bucket = 'inboundmessage'

    VERSION = 3
    MIGRATOR = InboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(BatchVNone)

    # Extra fields for compound indexes
    batches_with_timestamps = ListOf(Unicode(), index=True)
    batches_with_addresses = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        batches_with_timestamps = []
        batches_with_addresses = []
        timestamp = self.msg['timestamp']
        for batch_id in self.batches.keys():
            batches_with_timestamps.append(u"%s$%s" % (batch_id, timestamp))
            batches_with_addresses.append(
                u"%s$%s$%s" % (batch_id, timestamp, self.msg['from_addr']))
        self.batches_with_timestamps = batches_with_timestamps
        self.batches_with_addresses = batches_with_addresses
        return super(InboundMessageV3, self).save()


class EventV1(Model):
    bucket = 'event'

    VERSION = 1
    MIGRATOR = EventMigrator

    # key is event_id
    event = VumiMessage(TransportEvent)
    message = ForeignKey(OutboundMessageV3)

    # Extra fields for compound indexes
    message_with_status = Unicode(index=True, null=True)

    def save(self):
        # We override this method to set our index fields before saving.
        timestamp = self.event['timestamp']
        status = self.event['event_type']
        if status == "delivery_report":
            status = "%s.%s" % (status, self.event['delivery_status'])
        self.message_with_status = u"%s$%s$%s" % (
            self.message.key, timestamp, status)
        return super(EventV1, self).save()
