# -*- test-case-name: vumi.components.tests.test_message_store_migrators -*-
# -*- coding: utf-8 -*-

from vumi.persist.model import ModelMigrator


class OutboundMessageMigrator(ModelMigrator):
    def migrate_from_unversioned(self, mdata):
        # Copy stuff that hasn't changed between versions
        mdata.copy_values('msg')


class InboundMessageMigrator(ModelMigrator):
    def migrate_from_unversioned(self, mdata):
        # Copy stuff that hasn't changed between versions
        mdata.copy_values('msg')
