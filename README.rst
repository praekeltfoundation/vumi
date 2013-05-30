Vumi
====

Documentation available online at http://vumi.readthedocs.org/ and in the `docs` directory of the repository.

|vumi-ci|_

.. |vumi-ci| image:: https://travis-ci.org/praekelt/vumi.png?branch=develop
.. _vumi-ci: https://travis-ci.org/praekelt/vumi

To build the docs locally::

    $ virtualenv --no-site-packages ve/
    $ source ve/bin/activate
    (ve)$ pip install Sphinx
    (ve)$ cd docs
    (ve)$ make html

You'll find the docs in `docs/_build/index.html`


