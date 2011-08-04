.. How to do a release

How we do releases
==================

Create a release branch
-----------------------

Select a release series number and initial version number::

    $ SERIES=0.1.x
    $ VER=0.1.0a

Start by creating the release branch (usually from develop but you can
also specify a commit to start from)::

    $ git flow release start $SERIES [<start point>]

Set the version in the release branch setup.py::

    $ ./utils/bump_version.sh $VER
    $ git commit -m "Set initial version for series $SERIES"

Set the version number in the develop branch *if necessary*.


Tag the release
---------------

Select a series to release from and version number::

    $ SERIES=0.1.x
    $ VER=0.1.0
    $ NEXTVER=0.1.1a

Bump version immediately prior to release and tag the commit::

    $ git checkout release/$SERIES
    $ ./utils/bump_version.sh $VER
    $ git commit -m "Version $VER"
    $ git tag vumi-$VER

Bump version number on release branch::

    $ ./utils/bump_version $NEXTVER
    $ git commit -m "Bump release series version."

Merge to master *if this is a tag off the latest release series*::

    $ git checkout master
    $ git merge vumi-$VER
    $ git commit -m "Merge $VER to master."

Declare victory.
