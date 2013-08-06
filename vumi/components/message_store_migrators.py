# -*- test-case-name: vumi.components.tests.test_message_store_migrators -*-
# -*- coding: utf-8 -*-

from vumi.persist.model import ModelMigrator


class OutboundMessageMigrator(ModelMigrator):
    def migrate_from_unversioned(self, mdata):
        # set version
        mdata.set_value('$VERSION', 1)

        # Copy stuff that hasn't changed between versions
        msg_fields = [k for k in mdata.old_data if k.startswith('msg.')]
        mdata.copy_values(*msg_fields)

        # Migrate batch -> batches
        batches = mdata.old_index.get('batch_bin', [])
        mdata.set_value('batches', batches)
        for batch_id in batches:
            mdata.add_index('batches_bin', batch_id)

        return mdata


class InboundMessageMigrator(ModelMigrator):
    def migrate_from_unversioned(self, mdata):
        # set version
        mdata.set_value('$VERSION', 1)

        # Copy stuff that hasn't changed between versions
        msg_fields = [k for k in mdata.old_data if k.startswith('msg.')]
        mdata.copy_values(*msg_fields)

        # Migrate batch -> batches
        batches = mdata.old_index.get('batch_bin', [])
        mdata.set_value('batches', batches)
        for batch_id in batches:
            mdata.add_index('batches_bin', batch_id)

        return mdata
