# -*- test-case-name: vumi.components.tests.test_message_store_migrators -*-
# -*- coding: utf-8 -*-

from vumi.persist.model import ModelMigrator


class MessageMigratorBase(ModelMigrator):
    def _copy_msg_field(self, msg_field, mdata):
        key_prefix = "%s." % (msg_field,)
        msg_fields = [k for k in mdata.old_data if k.startswith(key_prefix)]
        mdata.copy_values(*msg_fields)

    def _foreign_key_to_many_to_many(self, foreign_key, many_to_many, mdata):
        old_keys = mdata.old_index.get('%s_bin' % (foreign_key,), [])
        mdata.set_value(many_to_many, old_keys)
        many_to_many_index = '%s_bin' % (many_to_many,)
        for old_key in old_keys:
            mdata.add_index(many_to_many_index, old_key)


class EventMigrator(MessageMigratorBase):
    def migrate_from_unversioned(self, mdata):
        mdata.set_value('$VERSION', 1)

        if 'message' not in mdata.old_data:
            # We have an old-style index-only field here, so add the data.
            [message_id] = mdata.old_index['message_bin']
            mdata.old_data['message'] = message_id

        self._copy_msg_field('event', mdata)
        mdata.copy_values('message')
        mdata.copy_indexes('message_bin')

        return mdata

    def reverse_from_1(self, mdata):
        # We only copy existing fields and indexes over. The new fields and
        # indexes are computed at save time.
        # We don't set the version because we're writing unversioned models.
        self._copy_msg_field('event', mdata)
        mdata.copy_values('message')
        mdata.copy_indexes('message_bin')

        return mdata

    def migrate_from_1(self, mdata):
        # If the old data contains a value for the `batches` field, it must be
        # back-migrated from a newer version. If not, we have no way to know
        # what batches the event belongs to, so we leave the field empty. Some
        # external data migration tool will have to populate it.
        mdata.set_value('$VERSION', 2)
        self._copy_msg_field('event', mdata)
        mdata.set_value('batches', mdata.old_data.get('batches', []))
        mdata.copy_values('message')
        mdata.copy_indexes('message_bin')
        mdata.copy_indexes('message_with_status_bin')

        return mdata

    def reverse_from_2(self, mdata):
        # We copy the `batches` field and related indexes even though the older
        # model version doesn't know about them. This lets us migrate
        # v2 -> v1 -> v2 without losing data.
        mdata.set_value('$VERSION', 1)
        self._copy_msg_field('event', mdata)
        mdata.copy_values('message', 'batches')
        mdata.copy_indexes('message_bin')
        mdata.copy_indexes('message_with_status_bin')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_statuses_reverse_bin')

        return mdata


class OutboundMessageMigrator(MessageMigratorBase):
    def migrate_from_unversioned(self, mdata):
        mdata.set_value('$VERSION', 1)

        self._copy_msg_field('msg', mdata)
        self._foreign_key_to_many_to_many('batch', 'batches', mdata)

        return mdata

    def migrate_from_1(self, mdata):
        # We only copy existing fields and indexes over. The new fields and
        # indexes are computed at save time.
        mdata.set_value('$VERSION', 2)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')

        return mdata

    def migrate_from_2(self, mdata):
        # We only copy existing fields and indexes over. The new fields and
        # indexes are computed at save time.
        mdata.set_value('$VERSION', 3)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_timestamps_bin')

        return mdata

    def reverse_from_3(self, mdata):
        # The only difference between v2 and v3 is an index that's computed at
        # save time, so the reverse migration is identical to the forward
        # migration except for the version we set.
        mdata.set_value('$VERSION', 2)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_timestamps_bin')

        return mdata

    def migrate_from_3(self, mdata):
        # We only copy existing fields and indexes over. The new fields and
        # indexes are computed at save time.
        mdata.set_value('$VERSION', 4)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_timestamps_bin')
        mdata.copy_indexes('batches_with_addresses_bin')

        return mdata

    def reverse_from_4(self, mdata):
        # The only difference between v3 and v4 is an index that's computed at
        # save time, so the reverse migration is identical to the forward
        # migration except for the version we set.
        mdata.set_value('$VERSION', 3)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_timestamps_bin')
        mdata.copy_indexes('batches_with_addresses_bin')

        return mdata

    def migrate_from_4(self, mdata):
        # We copy existing fields and indexes over except for the indexes we're
        # removing.
        mdata.set_value('$VERSION', 5)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_addresses_bin')
        mdata.copy_indexes('batches_with_addresses_reverse_bin')

        return mdata

    def reverse_from_5(self, mdata):
        # The only difference between v4 and v5 is an index that's computed at
        # save time, so the reverse migration is identical to the forward
        # migration except for the version we set.
        mdata.set_value('$VERSION', 4)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_addresses_bin')
        mdata.copy_indexes('batches_with_addresses_reverse_bin')

        return mdata


class InboundMessageMigrator(MessageMigratorBase):
    def migrate_from_unversioned(self, mdata):
        mdata.set_value('$VERSION', 1)

        self._copy_msg_field('msg', mdata)
        self._foreign_key_to_many_to_many('batch', 'batches', mdata)

        return mdata

    def migrate_from_1(self, mdata):
        # We only copy existing fields and indexes over. The new fields and
        # indexes are computed at save time.
        mdata.set_value('$VERSION', 2)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')

        return mdata

    def migrate_from_2(self, mdata):
        # We only copy existing fields and indexes over. The new fields and
        # indexes are computed at save time.
        mdata.set_value('$VERSION', 3)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_timestamps_bin')

        return mdata

    def reverse_from_3(self, mdata):
        # The only difference between v2 and v3 is an index that's computed at
        # save time, so the reverse migration is identical to the forward
        # migration except for the version we set.
        mdata.set_value('$VERSION', 2)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_timestamps_bin')

        return mdata

    def migrate_from_3(self, mdata):
        # We only copy existing fields and indexes over. The new fields and
        # indexes are computed at save time.
        mdata.set_value('$VERSION', 4)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_timestamps_bin')
        mdata.copy_indexes('batches_with_addresses_bin')

        return mdata

    def reverse_from_4(self, mdata):
        # The only difference between v3 and v4 is an index that's computed at
        # save time, so the reverse migration is identical to the forward
        # migration except for the version we set.
        mdata.set_value('$VERSION', 3)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_timestamps_bin')
        mdata.copy_indexes('batches_with_addresses_bin')

        return mdata

    def migrate_from_4(self, mdata):
        # We copy existing fields and indexes over except for the indexes we're
        # removing.
        mdata.set_value('$VERSION', 5)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_addresses_bin')
        mdata.copy_indexes('batches_with_addresses_reverse_bin')

        return mdata

    def reverse_from_5(self, mdata):
        # The only difference between v4 and v5 is an index that's computed at
        # save time, so the reverse migration is identical to the forward
        # migration except for the version we set.
        mdata.set_value('$VERSION', 4)
        self._copy_msg_field('msg', mdata)
        mdata.copy_values('batches')
        mdata.copy_indexes('batches_bin')
        mdata.copy_indexes('batches_with_addresses_bin')
        mdata.copy_indexes('batches_with_addresses_reverse_bin')

        return mdata
