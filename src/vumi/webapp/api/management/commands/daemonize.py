"""

Start the celery daemon from the Django management command.

"""
from django.core.management.base import BaseCommand
from django.core.management import ManagementUtility, handle_default_options, CommandError
from optparse import make_option, OptionParser
from django.utils import daemonize
import os, errno, sys, signal
from contextlib import closing


class Command(BaseCommand):
    help = """Run commands in the background, separate the arguments for daemonize from the arguments of the command with `--`:

'./manage.py daemonize --logfile=daemonize.pid --pidfile=daemonize.pid -- celeryd --concurrency=4',
    """

    option_list = BaseCommand.option_list + (
        make_option('--logfile', action='store', type='string', default='daemonize.log',
                        dest='logfile', help='Where to log the output of the process'),
        make_option('--pidfile', action='store', type='string', default='daemonize.pid',
                        dest='pidfile', help='Where to store the PID file'),
    )
    
    def create_parser(self, prog_name, subcommand):
        """
        Create and return the ``OptionParser`` which will be used to
        parse the arguments to this command.

        """
        command_instance = self.get_command(subcommand)
        return OptionParser(prog=prog_name,
                            usage=command_instance.usage(subcommand),
                            version=command_instance.get_version(),
                            option_list=command_instance.option_list)

    def create_daemonize_parser(self, prog_name, subcommand):
        """
        Create and return the ``OptionParser`` which will be used to
        parse the arguments to this command.

        """
        return OptionParser(prog=prog_name,
                            usage=self.usage(subcommand),
                            version=self.get_version(),
                            option_list=self.option_list)

    def get_command(self, command):
        return ManagementUtility().fetch_command(command)
    
    def run_from_argv(self, argv):
        """
        Set up any environment changes requested (e.g., Python path
        and Django settings), then run this command.

        """
        if '--' in argv:
            split_at = argv.index('--')
            command_argv = [argv[0]] + argv[split_at + 1:]
            if len(command_argv) == 1:
                print self.usage('daemonize')
            else:
                daemon_argv = argv[:split_at]
                daemon_parser = self.create_daemonize_parser(daemon_argv[0], daemon_argv[1])
                daemon_options, daemon_args = daemon_parser.parse_args(daemon_argv[2:])
                handle_default_options(daemon_options)
                
                parser = self.create_parser(command_argv[0], command_argv[1])
                options, args = parser.parse_args(command_argv[2:])
                
                # prepend the execute args with the original command again
                self.execute(daemon_args=daemon_args,
                                daemon_options=daemon_options.__dict__,
                                command=command_argv[1], 
                                command_args=args, 
                                command_options=options.__dict__)
        else:
            super(Command, self).run_from_argv(argv)
    
    def check_pid(self, pid_file):
        """
        Adapted from 
        
        http://twistedmatrix.com/trac/browser/trunk/twisted/scripts/_twistd_unix.py
        """
        if os.path.exists(pid_file):
            try:
                with closing(open(pid_file,'r')) as fp:
                    pid = int(fp.read())
            except ValueError:
                sys.exit('Pidfile %s contains non-numeric value' % pid_file)
            try:
                os.kill(pid, 0)
            except OSError, why:
                if why[0] == errno.ESRCH:
                    # The pid doesnt exists.
                    print 'Removing stale pidfile %s' % pid_file
                    os.remove(pid_file)
                else:
                    sys.exit("Can't check status of PID %s from pidfile %s: %s" %
                             (pid, pid_file, why[1]))
            else:
                sys.exit("""\
    Another daemonized process is running, PID %s\n
    This could either be a previously started instance of your application or a
    different application entirely. To start a new one, either run it in some other
    directory, or use the --pidfile and --logfile parameters to avoid clashes.
    """ %  pid)
    
    def write_pid_to_pid_file(self, pid_file):
        with closing(open(pid_file,'wb')) as fp:
            fp.write(str(os.getpid()))
    
    def remove_pid_file(self, pid_file):
        print "REMOVING PID FILE!!", pid_file
        os.remove(pid_file)
    
    def execute(self, daemon_args, daemon_options, command, command_args, command_options):
        out_log = daemon_options.pop('logfile')
        pid_file = daemon_options.pop('pidfile')
        
        daemonize.become_daemon(out_log=out_log, err_log=out_log)
        self.check_pid(pid_file)
        self.write_pid_to_pid_file(pid_file)
        signal.signal(signal.SIGTERM, lambda: self.remove_pid_file(pid_file))
        command_instance = self.get_command(command)
        command_instance.execute(*command_args, **command_options)
        self.remove_pid_file(pid_file)
