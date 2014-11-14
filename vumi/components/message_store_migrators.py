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
        mdata.copy_indexes('batches')

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
        mdata.copy_indexes('batches')

        return mdata
