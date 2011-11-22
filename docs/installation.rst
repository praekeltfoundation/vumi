Installing Vumi
===============

Installing Vumi on Ubuntu
-------------------------

Vumi should run on most recent versions of Ubuntu.

First, install the necessary packages::

  $ sudo apt-get install build-essential python2.6 python2.6-dev \
    python-setuptools python-pip python-virtualenv postgresql-8.4 libpq-dev \
    rabbitmq-server git-core openjdk-6-jre-headless libcurl4-openssl-dev \
    redis-server

Then setup the development environment:

    1. ``$ git clone https://github.com/praekelt/vumi.git`` (clone the git repository)
    2. ``cd vumi`` (change directory to the repository folder)
    3. ``$ virtualenv --no-site-packages ve`` (create a virtualenv)
    4. ``$ . ve/bin/activate`` (activate the virtualenv)
    5. ``$ pip install -r config/requirements.pip`` (install requirements)
    6. ``$ sudo utils/rabbitmq.setup.sh`` (setup RabbitMQ vhosts)
    7. ``$ utils/postgres.setup.sh`` (setup PostGreSQL databases)
    8. ``$ utils/run-test.sh`` (run tests to check setup)

If all the tests pass, proceeding to :doc:`getting-started`.

.. todo::

   Remove the 'vumi' database name from the note below once we
   no longer depend on Django. Remove the 'test' database name from
   the note below once the test database is configurable.

.. note::

   Currently one must use the password 'vumi' for the 'vumi' and
   'test' databses created by `postgres.setup.sh` because the vumi
   tests rely on this.


Installing Vumi with VirtualBox and Vagrant
-------------------------------------------

If you are not using an operating system we have detail installation
instructions for, the easiest way to try out Vumi is by using
VirtualBox_ and Vagrant_:

    1. Install VirtualBox_
    2. Install Vagrant_, make sure you follow the `OS specific instructions`_.
    3. Clone Vumi with ``git clone https://github.com/praekelt/vumi.git``
    4. Execute ``cd vumi``
    5. Execute ``vagrant up``, this will take some time to complete as it:
        1. Downloads a 480MB Ubuntu 10.04 server VM
        2. Downloads ~ 40MB worth of Python packages
        3. Downloads ~ 100MB worth of packages with `apt-get`
    6. Go to http://localhost:7000/admin and log in with username
       `vumi` and password `vumi` for the Django based webapp.
    7. Go to http://localhost:7010 for the Supervisord web based management
    8. Go to http://localhost:7011 for the SMSC simulator console.

.. note::

    Ubuntu 10.04 only provides version 1.3.5 of RubyGems while
    Vagrant requires version 1.3.6. You'll need to get an upstream
    version or install from source as per the `OS specific
    instructions`_.

.. note::

    Ubuntu doesn't put `vagrant` on your $PATH, you'll need to
    manually symlink it with `sudo ln -s /var/lib/gems/1.8/bin/vagrant
    /usr/bin/`

.. _Vagrant: http://www.vagrantup.com
.. _VirtualBox: http://www.virtualbox.org
.. _OS specific instructions: http://vagrantup.com/docs/getting-started/index.html
