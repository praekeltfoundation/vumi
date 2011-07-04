Conversation Datastore
======================

We are currently using PostgreSQL as our main datastore and are using Django's ORM as our means of interacting with it. **This however is going to change.**

What we are going towards:

    1. HBase as our conversation store.
    2. Interface with it via HBase's Stargate REST APIs.
