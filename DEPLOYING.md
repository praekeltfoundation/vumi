Deploying Vumi
------------------

The deploment of Vumi ties directly into how it is being developed with Git.

  1.  All development of Vumi occurs in topical branches. Topical branches 
      are all named `topics/...`. For example `topics/smn-fabric-deploys`.
  2.  When the new development is finished, `master` is rebased into the
      `topics/...` branch. If all tests pass then the `topics/...` branch is 
      merged into `master`.
  3.  When `master` is ready for testing then it is promoted to the `staging`
      branch with `git push origin master:staging`.
  4.  If the `staging` code is ready for a release then push the `staging` to 
      `production` with `git push origin master:production`

The `staging` and `production` branches do not to be tracked locally for you to push changes to it, `git push origin master:staging` (or `... master:production`) pushes the changes remotely. To avoid conflicts and accidental changes in these release branches it's probably a good idea *not* to pull these in locally.

Fabric!
-------

For deploying, updating, starting & stopping of services we use [Fabric][fabric].

Fabric provides a command line tool using SSH for application deployment and/or administration tasks.

The `-l` and the `-d <command name>` provide some insights into the available commands, their arguments and example usage.

    $ fab -l
    $ fab -d <command> 

Start the virtualenv & make sure all the requirements are installed:

    $ source ve/bin/activate
    (ve) $ pip install -r config/requirements.pip

There isn't a limit on the number of environments. Every branch in git can be a deployable environment. All environments will be installed in `/var/praekelt/vumi/`. For this example we'll walk through installing a `staging` environment.

Before we start, update the `fabfile.py` and change `env.hosts` to point to the machine you're deploying to. You can also manually specify the hosts variable with the `-H` command line option or create a `~/.fabricrc` as specified in the [documentation](http://docs.fabfile.org/0.9.0/usage/fab.html#settings-files).

To start the deploy execute the following command:

    (ve) $ fab deploy:staging
    ...

Fabric has the convention of "command:args,keyword=args". Here we're telling fabric to start the `deploy` command with `staging` as it's first argument.

Internally this first calls `fab layout:staging` to create the following  layout directory:

    .
    └── vumi
        └── staging
            ├── releases            # all timestamped releases go here
            └── shared              # all shared stuff that needs to be kept
                ├── logs            # across releases is stored here.
                ├── repositories
                └── tmp
                    ├── cache
                    │   └── pip
                    └── pids

Then it continues to checkout the repository into  `staging/shared/repositories/vumi` and switches to the branch `staging`. If the repository already exists it instead does a `git pull` to pull in the latest updates.

Then it creates a new timestamped release folder in `releases` and copies the  cloned GIT repository into it.

Next it'll call `fab create_virtualenv:staging` to setup the virtualenv in the latest release folder. Setting up the `virtualenv` will download all the project dependencies and install them. Remember that `libcurl-dev` is needed for the `pycurl` library to compile.

Next the latest release will be symlinked to `staging/current`. Nginx will point to whatever app is running inside `current`.

After this you'll probably want to setup the database. In my case I'm using `psyopg2` and it's not in the requirements file, I'll have to install that manually too.

    (ve) $ fab execute:staging,"pip -E ve install pyscopg2"
    (ve) $ fab execute:staging,"./manage.py syncdb"

If you'd want to deploy some minor code change then use `fab update:staging`, that'll pull in the latest changes. Remember to restart the webapp for the changes in the code to be picked up.

Starting, restarting & stopping the webapp
------------------------------------------

By default the webapp will start on port 8000. You can specify which port you want. The port is used as the unique identifier for the process, it's used for naming the PID. You can start multiple instances by specifying different ports.

    (ve) $ fab start_webapp:staging,port=8001
    (ve) $ fab restart_webapp:staging,port=8001
    (ve) $ fab stop_webapp:staging,port=8001


Deploying other environments
----------------------------

Other environments work exactly the same:

    (ve) $ fab deploy:production

Will deploy to `/var/praekelt/vumi/production` with the same directory layout. It'll switch to the `production` branch in the git repository.

[fabric]: http://www.fabfile.org
