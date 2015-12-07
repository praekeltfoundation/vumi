Vumi
====

NOTE: Version 0.6.x is backward-compatible with 0.5.x for the most part, with
some caveats. The first few releases will be removing a bunch of obsolete and
deprecated code and replacing some of the internals of the base worker. While
this will almost certainly not break the majority of things built on vumi, old
code or code that relies too heavily on the details of worker setup may need to
be fixed.

Documentation available online at http://vumi.readthedocs.org/ and in the `docs` directory of the repository.

|vumi-ver| |vumi-ci| |vumi-cover| |python-ver| |vumi-docs| |vumi-downloads| |vumi-license|

.. |vumi-ver| image:: https://pypip.in/v/vumi/badge.png?text=pypi
    :alt: Vumi version
    :scale: 100%
    :target: https://pypi.python.org/pypi/vumi

.. |vumi-ci| image:: https://travis-ci.org/praekelt/vumi.png?branch=develop
    :alt: Vumi Travis CI build status
    :scale: 100%
    :target: https://travis-ci.org/praekelt/vumi

.. |vumi-cover| image:: https://coveralls.io/repos/praekelt/vumi/badge.png?branch=develop
    :alt: Vumi coverage on Coveralls
    :scale: 100%
    :target: https://coveralls.io/r/praekelt/vumi

.. |python-ver| image:: https://pypip.in/py_versions/vumi/badge.svg
    :alt: Python version
    :scale: 100%
    :target: https://pypi.python.org/pypi/vumi

.. |vumi-docs| image:: https://readthedocs.org/projects/vumi/badge/?version=latest
    :alt: Vumi documentation
    :scale: 100%
    :target: http://vumi.readthedocs.org/

.. |vumi-downloads| image:: https://pypip.in/download/vumi/badge.svg
    :alt: Vumi downloads from PyPI
    :scale: 100%
    :target: https://pypi.python.org/pypi/vumi

.. |vumi-license| image:: https://pypip.in/license/vumi/badge.svg
    :target: https://pypi.python.org/pypi/vumi
    :alt: Vumi license


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
