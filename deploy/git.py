from __future__ import with_statement
from fabric.api import *
from fabric.contrib.files import exists

def clone(url, name=''):
    return run('git clone %(url)s %(name)s' % {
        'url': url,
        'name': name,
    })

def remove_branch(branch):
    return run('git branch -d %s' % branch)

def checkout(branch, revision=None):
    if not revision:
        revision = 'origin/%s' % branch
    return run('git checkout -b %(branch)s %(revision)s' % {
        'branch': branch,
        'revision': revision
    })

def checkout_revision(revision):
    return run('git checkout %s' % revision)

def pull(branch='HEAD'):
    return run('git pull origin %s' % branch)
    

def current_branch():
    return run('git branch | grep \'^*\'').split()[-1]

def is_repository(directory):
    return exists('%s/.git' % directory)

def revision(branch='HEAD'):
    return run("git rev-list --max-count=1 %s" % branch)

def local_revision(branch='HEAD'):
    return local("git rev-list --max-count=1 %s" % branch)

def fetch():
    return run('git fetch')
