Setting up a Keyword SMS trigger in Vumi
========================================

1. Make sure you have the Vumi VM running.
2. Run `vagrant provision` to make sure you're running the latest code from the Vumi repository.
3. Go to http://localhost:7000/admin and log in with username `vumi` and password `vumi`.
5. Create a URL callback:
    1. Profile: Select `Profile for vumi`
    2. Name: Select `SMS Received`
    3. URL: Enter the URL you want to HTTP POST the SMS to.
6. Create a keyword for the user `vumi` at http://localhost:7000/admin/api/keyword/. Let's use 'vumi' as the keyword for now. Specifying a keyword automatically forwards any incoming message with that starts with that keyword to all the URL callbacks defined.
7. Go to http://localhost:7011/ which is the SMSC simulator.
    1. Click on "Inject an MO Message" in the black bar.
    2. In the `short_message` field enter 'vumi hello world'
    3. In the `destination_addr` field enter '27761234567'
    4. Click on 'Submit Message'.
8. The SMS is posted to the URL you specified in the callback.


What's happening here?
======================

1. We're manually sending an SMS from the SMSC to Vumi, MO stands for "mobile originated".
2. Vumi is configured to expect keyword based SMS messages on the numbers specified in the `config/smpp.yaml` file, one of which is "27761234567"
4. When Vumi receives a text message on a number configured in `config/smpp.yaml` it:

    1. Looks up the keyword based on the first word found in the incoming message.
    2. Looks up the user the keyword belongs to
    3. Looks up the URL callbacks for that user
    4. HTTP POSTs the following fields to each given URL:
        1. callback_name (sms_receipt or sms_received)
        2. to_msisdn (who the message was addressed to)
        3. from_msisdn (who the message was sent from)
        4. message (the full text of the message received)
