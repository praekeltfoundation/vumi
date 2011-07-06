Datastore Access
================

Currently all datastore access is via Django's ORM with the database being PostgreSQL. This is going to change.

We will continue to use PostgreSQL for data that isn't going to be very write heavy. These include:

1. User accounts
2. Groups
3. Accounting related data (related to user accounts and groups)

The change we are planning for is to be using HBase for the following data:

1. Conversation
2. Messages that are part of a conversation