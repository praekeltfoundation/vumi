Custom Application Logic
========================

Javascript is the DSL of the web. Vumi will allow developers used to front-end development technologies to build and host frontend and backend applications using Javascript as the main language. 

Pros:

* Javascript lends itself well to event based programming, ideal for messaging.
* Javascript is well known to the target audience.
* Javascript is currently experiencing major development in terms of performance improvements by Google, Apple, Opera & Mozilla.
* Javascript has AMQP libraries available.


Cons:

* We would need to sandbox it (but we’d need to do that regardless, Node.js has some capabilities for this but I’d want the sandbox to restrict any file system access).
* We’re introducing a different environment next to Python.
* Data access could be more difficult than Python.


How would it work?
------------------

Application developers could bundle (zip) their applications as follows:

* application/index.html is the HTML5 application that we’ll host.
* application/assets/ is the Javascript, CSS and images needed by the frontend application.
* workers/worker.js has the workers that we’d fire up to run the applications workers for specific campaigns. These listen to messages arriving over AMQP as ‘events’ trigger specific pieces of logic for that campaign.

The HTML5 application would have direct access to the Vumi JSON APIs, zero middleware would be needed.

This application could then be uploaded to Vumi and we’d make it available in their account and link their logic to a specific SMS short/long code, twitter handle or USSD code.

Python would still be driving all of the main pieces (SMPP, Twitter, our JSON API’s etc...) only the hosted applications would be javascript based. Nothing is stopping us from allowing Python as a worker language at a later stage as well.
