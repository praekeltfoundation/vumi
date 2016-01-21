.. How to do a release

How we do releases
==================

Update the release notes and roadmap
------------------------------------

Update the :doc:`roadmap` and :doc:`release-notes` as necessary.


Create a release branch
-----------------------

Select a release series number and initial version number::

    $ SERIES=0.1.x
    $ VER=0.1.0a

Start by creating the release branch (usually from develop but you can
also specify a commit to start from)::

    $ git flow release start $SERIES [<start point>]

Set the version in the release branch::

    $ ./utils/bump-version.sh $VER
    $ git add setup.py docs/conf.py vumi/__init__.py
    $ git commit -m "Set initial version for series $SERIES"

Set the version number in the develop branch *if necessary*.

Push your changes to Github::

    $ git push origin release/$SERIES


Tag the release
---------------

Select a series to release from and version number::

    $ SERIES=0.1.x
    $ VER=0.1.0
    $ NEXTVER=0.1.1a

Bump version immediately prior to release and tag the commit::

    $ git checkout release/$SERIES
    $ ./utils/bump-version.sh $VER
    $ git add setup.py docs/conf.py vumi/__init__.py
    $ git commit -m "Version $VER"
    $ git tag vumi-$VER

Bump version number on release branch::

    $ ./utils/bump-version.sh $NEXTVER
    $ git add setup.py docs/conf.py vumi/__init__.py
    $ git commit -m "Bump release series version."

Merge to master *if this is a tag off the latest release series*::

    $ git checkout master
    $ git merge vumi-$VER

Push your changes to Github (don't forget to push the new tag)::

    $ git push
    $ git push origin refs/tags/vumi-$VER


Release to PyPI
---------------

Select the version number::

    $ VER=0.1.0
    $ git checkout vumi-$VER

Register the release with PyPI::

    $ python setup.py register

Build the source distribution package::

    $ python setup.py sdist

Upload the release to PyPI::

    $ twine-upload dist/vumi-$VER.tar.gz

Declare victory.
