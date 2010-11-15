from fabric.api import *
from os.path import join

def start_command(plugin, *args, **kwargs):
    port = kwargs.setdefault('port', 8000) # default to 8000
    args = ["-%s" % arg for arg in args]
    kwargs.setdefault('django-settings', env.django_settings_file)
    kwargs = ["--%s=%s" % (k,v) for k,v in kwargs.items()]
    return "twistd " \
        "--pidfile=%(pid_file)s " \
        "--logfile=%(log_file)s " \
        "%(plugin)s " \
        "%(args)s " \
        "%(kwargs)s " % {
            'pid_file': join(env.pids_path, 'twistd.%s.%s.pid' % (plugin, port)),
            'log_file': join(env.logs_path, 'twistd.%s.%s.log' % (plugin, port)),
            'plugin': plugin,
            'args': ' '.join(args),
            'kwargs': ' '.join(kwargs)
    }

def stop_command(plugin, port=8000):
    pid_file = '/'.join([env.pids_path, 'twistd.%s.%s.pid' % (plugin, port)])
    return 'kill `cat %s`' % pid_file


def restart_command(plugin, *args, **kwargs):
    port = kwargs.setdefault('port', 8000)
    return ' && '.join([
        stop_command(plugin, port),
        start_command(plugin, *args, **kwargs),
    ])
