Vumi
====

Documentation available online at http://vumi.readthedocs.org/ and in the `docs` directory of the repository.

|vumi-ci|_

.. |vumi-ci| image:: https://travis-ci.org/praekelt/vumi.png?branch=develop
.. _vumi-ci: https://travis-ci.org/praekelt/vumi

|vumi-cover|_

.. |vumi-cover| image:: https://coveralls.io/repos/praekelt/vumi/badge.png?branch=develop
.. _vumi-cover: https://coveralls.io/r/praekelt/vumi

To build the docs locally::

    $ virtualenv --no-site-packages ve/
    $ source ve/bin/activate
    (ve)$ pip install Sphinx
    (ve)$ cd docs
    (ve)$ make html

You'll find the docs in `docs/_build/index.html`

You can contact the Vumi development team in the following ways:

* via *email* by joining the the `vumi-dev@googlegroups.com`_ mailing list
* on *irc* in *#vumi* on the `Freenode IRC network`_

.. _vumi-dev@googlegroups.com: https://groups.google.com/forum/?fromgroups#!forum/vumi-dev
.. _Freenode IRC network: https://webchat.freenode.net/?channels=#vumi

Issues can be filed in the GitHub issue tracker. Please don't use the issue
tracker for general support queries.
