from fabric.api import *

def create_dirs(dirs):
    cmd = "mkdir -p %s" % " ".join(dirs)
    return run(cmd)

def create_dir(dirname):
    return create_dirs([dirname])

def copy_dirs(dest1, dest2):
    return run("cp -RPp %(from)s %(to)s/" % {
        'from': dest1,
        'to': dest2
    })

def symlink(dest1, dest2, force=True):
    return run("ln %(force)s -s %(from)s %(to)s" % {
        'from': dest1,
        'to': dest2,
        'force': '--force --no-dereference' if force else ''
    })

def list_dirs(path):
    return run("ls -x %s" % path).split()

def remove(filename, recursive_force=False):
    return run("rm %(force)s %(filename)s" % {
        'filename': filename,
        'force': '-rf' if recursive_force else ''
    })

