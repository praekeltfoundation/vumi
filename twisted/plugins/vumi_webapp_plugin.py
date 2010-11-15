import os
from zope.interface import implements

from twisted.python import usage, util
from twisted.internet import reactor
from twisted.web import wsgi, server
from twisted.application import service, internet
from twisted import plugin

class Options(usage.Options):
    optParameters = [
        ["config", "c", None, "Read options from config file", str],
        ['port', 'p', 8000, 'The port to run on', int],
        ['django-settings', 's', 'vumi.webapp.settings', 'Value to be set as DJANGO_SETTINGS_MODULE', str],
    ]
    
    def opt_config(self, path):
        """
        Read the options from a config file rather than command line, uses
        the ConfigParser from stdlib. Section headers are prepended to the
        options and together make up the command line parameter:
        
            [webapp]
            host: localhost
            port: 8000
        
        Equals to
        
            twistd ... --webapp-host=localhost --webapp-port=8000
        
        """
        import ConfigParser
        config = ConfigParser.ConfigParser()
        config.readfp(open(path))
        for section in config.sections():
            for option in config.options(section):
                parameter_name = '%s-%s' % (section, option)
                # don't need to do getint / getfloat etc... here, Twisted's
                # usage.Options does the validation / coerce stuff for us
                parameter_value = config.get(section, option)
                dispatcher = self._dispatch[parameter_name]
                dispatcher.dispatch(parameter_name, parameter_value)
        
    opt_c = opt_config


class VumiWebappServiceMaker(object):
    """
    Run Django in as a Twisted service
    """
    implements(service.IServiceMaker, plugin.IPlugin)
    tapname = "vumi_webapp"
    description = "The vumi webapp, providing webhooks / callbacks and a REST api"
    options = Options
    
    def makeService(self, options):
        os.environ['DJANGO_SETTINGS_MODULE'] = options['django-settings']
        from django.core.handlers.wsgi import WSGIHandler
        from django.core.servers.basehttp import AdminMediaHandler
        app = AdminMediaHandler(WSGIHandler())
        resource = wsgi.WSGIResource(reactor, reactor.getThreadPool(), app)
        site = server.Site(resource)
        return internet.TCPServer(options['port'], site)

serviceMaker = VumiWebappServiceMaker()
