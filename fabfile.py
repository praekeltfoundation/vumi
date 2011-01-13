from __future__ import with_statement
from fabric.api import *
from fabric.contrib.console import confirm
from fabric.contrib.files import exists
from datetime import datetime
from os.path import join as _join

from deploy import git, system, base, twistd

RELEASE_NAME_FORMAT = '%Y%m%d_%H%M%S' # timestamped
# default for now
# env.hosts = ['ubuntu-server.local']

def _setup_env(fn):
    def wrapper(branch, *args, **kwargs):
        layout(branch)
        return fn(branch, *args, **kwargs)
    wrapper.func_name = fn.func_name
    wrapper.func_doc = (fn.func_doc or '') + \
                                        "Requires the branch from which you " \
                                        "want to deploy as an argument."
    return wrapper


def _setup_env_for(branch):
    env.branch = branch
    env.github_user = 'praekelt'
    env.github_repo_name = 'vumi'
    env.github_repo = 'http://github.com/%(github_user)s/%(github_repo_name)s.git' % env
    
    env.deploy_to = '/var/praekelt/%(github_repo_name)s/%(branch)s' % env
    env.releases_path = "%(deploy_to)s/releases" % env
    env.current = "%(deploy_to)s/current" % env
    env.shared_path = "%(deploy_to)s/shared" % env
    env.tmp_path = "%(shared_path)s/tmp" % env
    env.pip_cache_path = "%(tmp_path)s/cache/pip" % env
    env.pids_path = "%(tmp_path)s/pids" % env
    env.logs_path = "%(shared_path)s/logs" % env
    env.repo_path = "%(shared_path)s/repositories" % env
    env.django_settings_file = "environments.%(branch)s" % env
    env.layout = [
        env.releases_path,
        env.tmp_path,
        env.pip_cache_path,
        env.pids_path,
        env.logs_path,
        env.repo_path,
    ]

def _repo_path(repo_name):
    return '%(repo_path)s/%(github_repo_name)s' % env

def _repo(repo_name):
    """helper to quickly switch to a repository"""
    return cd(_repo_path(repo_name))

def layout(branch):
    """
    Create a file system directory layout for deploying to.
    """
    require('hosts')
    _setup_env_for(branch)
    require('layout', provided_by=['_setup_env_for'])
    system.create_dirs(env.layout)

@_setup_env
def deploy(branch):
    """
    Deploy the application in a timestamped release folder.
    
        $ fab deploy:staging
    
    Internally this does the following:
    
        * `git pull` if a cached repository already exists
        * `git clone` if it's the first deploy ever
        * Checkout the current selected branch
        * Create a new timestamped release directory
        * Copy the cached repository to the new release directory
        * Setup the virtualenv
        * Install PIP's requirements, downloading new ones if not already cached
        * Symlink `<branch>/current` to `<branch>/releases/<timestamped release directory>`
    
    """
    if not git.is_repository(_repo_path(env.github_repo_name)):
        # repository doesn't exist, do a fresh clone
        with cd(env.repo_path):
            git.clone(env.github_repo, env.github_repo_name)
        with _repo(env.github_repo_name):
            git.checkout(branch)
    else:
        # repository exists
        with _repo(env.github_repo_name):
            if not (branch == git.current_branch()):
                # switch to our branch if not already
                git.checkout(branch)
            # pull in the latest code
            git.pull(branch)
    # 20100603_125848
    new_release_name = datetime.utcnow().strftime(RELEASE_NAME_FORMAT)
    # /var/praekelt/vumi/staging/releases/20100603_125848
    new_release_path = _join(env.releases_path, new_release_name)
    # /var/praekelt/vumi/staging/releases/20100603_125848/vumi
    # Django needs the project name as it's parent dir since that is 
    # automagically appended to the loadpath
    new_release_repo = _join(new_release_path, env.github_repo_name)
    
    system.create_dir(new_release_path)
    system.copy_dirs(_repo_path(env.github_repo_name), new_release_path)
    
    copy_settings_file(branch, release=new_release_name)
    
    symlink_shared_dirs = ['logs', 'tmp']
    for dirname in symlink_shared_dirs:
        with cd(new_release_repo):
            system.remove(dirname, recursive_force=True)
            system.symlink(_join(env.shared_path, dirname), dirname)
    
    # create the virtualenv
    create_virtualenv(branch)
    # ensure we're deploying the exact revision as we locally have
    base.set_current(new_release_name)


@_setup_env
def copy_settings_file(branch, release=None):
    """
    Copy the settings file for this branch to the server
    
        $ fab copy_settings_file:staging
        
    If no release is specified it defaults to the latest release.
    
    
    """
    release = release or base.current_release()
    directory = _join(env.releases_path, release, env.github_repo_name)
    put(
        "environments/%(branch)s.py" % env, 
        _join(directory, "environments/%(branch)s.py" % env)
    )

@_setup_env
def managepy(branch, command, release=None):
    """
    Execute a ./manage.py command in the virtualenv with the current
    settings file
    
        $ fab managepy:staging,"syncdb"
    
    This will do a `./manage.py syncdb --settings=environments.staging`
    within the virtualenv.
    
    If no release is specified it defaults to the latest release.
    
    """
    return execute(branch, "./manage.py %s --settings=%s" % (
        command, 
        env.django_settings_file
    ), release)

@_setup_env
def execute(branch, command, release=None):
    """
    Execute a shell command in the virtualenv
    
        $ fab execute:staging,"tail logs/*.log"
    
    If no release is specified it defaults to the latest release.
    
    """
    release = release or base.current_release()
    directory = _join(env.releases_path, release, env.github_repo_name)
    return _virtualenv(directory, command)

@_setup_env
def create_virtualenv(branch, release=None):
    """
    Create the virtualenv and install the PIP requirements
    
        $ fab create_virtualenv:staging
    
    If no release is specified it defaults to the latest release
    
    """
    release = release or base.current_release()
    directory = _join(env.releases_path, release, env.github_repo_name)
    with cd(directory):
        return run(" && ".join([
            "virtualenv --no-site-packages ve",
            "source ve/bin/activate",
            "pip -E ve install --download-cache=%(pip_cache_path)s -r config/requirements.pip" % env,
            "python setup.py develop",
        ]))


def _virtualenv(directory, command, env_name='ve'):
    activate = 'source %s/bin/activate' % env_name
    deactivate = 'deactivate'
    with cd(directory):
        run(' && '.join([activate, command, deactivate]))


@_setup_env
def update(branch):
    """
    Pull in the latest code for the latest release.
    
        $ fab update:staging
        
    Only to be used for small fixed, typos etc..
    
    """
    current_release = base.releases(env.releases_path)[-1]
    with cd(_join(env.current, env.github_repo_name)):
        git.pull(branch)


@_setup_env
def start_webapp(branch, **kwargs):
    """
    Start the webapp as a daemonized twistd plugin
    
        $ fab start_webapp:staging,port=8000
    
    The port is optional, it defaults to 8000. The port is also used to create
    the pid and log files, it functions as the unique id for this webapp
    instance.
    
    By default `environments.<branch>` is used but this can be overridden by 
    specifying django-settings=environments.somethingelse as a 
    keyword argument.
    
    """
    _virtualenv(
        _join(env.current, env.github_repo_name),
        twistd.start_command('vumi_webapp', **kwargs)
    )

@_setup_env
def start_webapp_cluster(branch, *ports, **kwargs):
    """
    Start the webapp cluster
    
        $ fab start_webapp_cluster:staging,8000,8001,8002,8003
    
    """
    for port in ports:
        start_webapp(branch,port=port, **kwargs)
    

@_setup_env
def restart_webapp(branch, **kwargs):
    """
    Restart the webapp
    
        $ fab restart_webapp:staging,port=8000
    
    """
    _virtualenv(
        _join(env.current, env.github_repo_name),
        twistd.restart_command('vumi_webapp', **kwargs)
    )

@_setup_env
def restart_webapp_cluster(branch, *ports, **kwargs):
    """
    Restart a cluster of webapp instances
    
        $ fab restart_webapp_cluster:staging,8000,8001,8002,8003
    """
    for port in ports:
        restart_webapp(branch, port=port, **kwargs)

@_setup_env
def stop_webapp(branch, **kwargs):
    """
    Stop the webapp
    
        $ fab stop_webapp:staging,port=8000
    
    """
    _virtualenv(
        _join(env.current, env.github_repo_name),
        twistd.stop_command('vumi_webapp', **kwargs)
    )

@_setup_env
def stop_webapp_cluster(branch, *ports, **kwargs):
    """
    Stop the webapp cluster
    
        $ fab stop_webapp_cluster:staging,8000,8001,8002,8003
    
    """
    for port in ports:
        stop_webapp(branch, port=port, **kwargs)


@_setup_env
def cleanup(branch,limit=5):
    """
    Cleanup old releases
    
        $ fab cleanup:staging,limit=10
    
    Remove old releases, the limit argument is optional (defaults to 5).
    """
    run("cd %(releases_path)s && ls -1 . | head --line=-%(limit)s | " \
        "xargs rm -rf " % {
            'releases_path': env.releases_path,
            'limit': limit
        }
    )

def _get_screens(name_prefix):
    original_warn_only = env.warn_only
    env.warn_only = True
    screens = [line.strip() for line in \
                run("screen -ls | grep %s" % name_prefix).split('\n')]
    env.warn_only = original_warn_only
    if not screens:
        return []
    screen_names = [screen.split()[0] for screen in screens]
    pid_names = [screen_name.split(".") for screen_name in screen_names]
    return [(pid_name[0], pid_name[1].split("_")[-1]) for pid_name in pid_names]

@_setup_env
def start_celery_worker(branch, uuid):
    """
    Start a celery worker
    
        $ fab start_celery:staging,1
        
    """
    with cd(_join(env.current, env.github_repo_name)):
        run("screen -dmS celery_%(uuid)s ./start-celery.sh %(settings)s %(uuid)s" % {
            'uuid': uuid,
            'settings': env.django_settings_file
        })
        
@_setup_env
def list_celery_workers(branch):
    """
    List all running celery workers
    
        $ fab list_celery_workers:staging
    
    """
    with cd(_join(env.current, env.github_repo_name)):
        sessions = _get_screens("celery_")
        for pid,uuid in sessions:
            print "Celery Worker => pid:%s, uuid:%s" % (pid, uuid)
        

@_setup_env
def stop_celery_worker(branch, uuid):
    """
    Stop a celery worker
    
        $ fab stop_celery:staging,1
    
    """
    with cd(_join(env.current, env.github_repo_name)):
        sessions = _get_screens("celery_")
        for pid,uuid_ in sessions:
            if uuid_ == uuid:
                run("kill %s" % pid)
