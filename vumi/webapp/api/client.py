import json
import re
import urllib
import urllib2
import base64
from functools import wraps
from collections import namedtuple
from datetime import datetime

__doc__ = """

The HTTP API Client
===================

A client for Vumi's HTTP API.

"""

CLASS_CACHE = {}
"""
Classes automatically generated with namedtuple are cached
here to they're not automatically generated more than once
"""

TYPE_CAST_MAP = [
    (r'.*_at$', lambda i: datetime.strptime(i, '%Y-%m-%d %H:%M:%S')),
    (r'.*_id$', lambda i: int(i)),
]
"""
List of (pattern, callback) tuples, if the pattern matches a response's
dictionary key then the callback is used to convert the string value
to a python value.
"""


class ClientException(Exception):
    pass


def camelcase(name):
    """
    Converts lower_case to LowerCase

    >>> camelcase('camel_case_this')
    u'CamelCaseThis'
    >>>

    """
    return u''.join(map(lambda s: s.capitalize(), name.split('_')))


def make_response(name, dictionary):
    """
    Turns a dictionary into a list of python objects

    >>> resp = make_response('SentSMSResponse', {'created_at':'2011-3-28 14:04:00', 'message':u'hello world'})
    >>> resp
    SentSMSResponse(created_at=datetime.datetime(2011, 3, 28, 14, 4), message=u'hello world')
    >>> resp.created_at
    datetime.datetime(2011, 3, 28, 14, 4)
    >>> resp.message
    u'hello world'
    >>>

    """
    if name not in CLASS_CACHE:
        cls = namedtuple(name, dictionary.keys())
        CLASS_CACHE[name] = cls
    typed_dictionary = process_types(dictionary)
    return CLASS_CACHE[name](**typed_dictionary)


def process_types(dictionary):
    """
    Process a dictionary's values according to types as defined in
    TYPE_CAST_MAP

    >>> process_types({'created_at':'2011-3-28 14:04:00', 'message':u'hello world'})
    {'created_at': datetime.datetime(2011, 3, 28, 14, 4), 'message': u'hello world'}
    >>>

    """
    for pattern, func in TYPE_CAST_MAP:
        keys = filter(lambda key: re.search(pattern, key), dictionary.keys())
        for key in keys:
            value = dictionary.get(key)
            if value:
                dictionary[key] = func(value)
    return dictionary


def is_successful(response):
    return 200 <= response.status_code < 300


def auto_response(function):
    """
    A decorator, it'll automatically convert a dictionary into
    proper python objects and it'll create namedtuple instances
    for each of the objects using the camelcased name of the
    decorated function
    """
    @wraps(function)
    def decorator(*args, **kwargs):
        response = function(*args, **kwargs)
        if is_successful(response):
            obj = json.loads(response.content)
            name = "%sResponse" % camelcase(function.__name__)
            if isinstance(obj, list):
                return map(lambda o: make_response(name, o), obj)
            else:
                return make_response(name, obj)
        else:
            print response.content
            raise ClientException('Bad Response: %s' % response.status_code)
    return decorator


def dict_to_tuple(dictionary):
    """

    >>> dict_to_tuple({'id':[1,2,3,4],'testing':1,'bla':'three'})
    [('testing', 1), ('bla', 'three'), ('id', 1), ('id', 2), ('id', 3), ('id', 4)]
    >>>

    """
    return_list = []
    for key, value in dictionary.items():
        if isinstance(value, list):
            for entry in value:
                return_list.append((key, entry))
        else:
            return_list.append((key, value))
    return return_list


class RequestWithMethod(urllib2.Request):
    def __init__(self, url, method, data=None, headers={},\
                    origin_req_host=None, unverifiable=False):
        self._method = method
        urllib2.Request.__init__(self, url, data, headers,\
                                    origin_req_host, unverifiable)

    def get_method(self):
        if self._method:
            return self._method.upper()
        else:
            return urllib2.Request.get_method(self)


class ApiResponse(object):

    def __init__(self, url_response):
        self.status_code = url_response.code
        self.content = url_response.read()
        self.url_response = url_response


class Connection(object):

    def __init__(self, username, password, base_url):
        self.username = username
        self.password = password
        self.credentials = base64.encodestring("%s:%s" %
                               (self.username, self.password)).strip()
        self.base_url = base_url
        self.debug = False

    def request(self, method, path, data={}):
        url = self.base_url + path
        url_data = urllib.urlencode(dict_to_tuple(data))
        request = RequestWithMethod(url, method, data=url_data)
        request.add_header('User-Agent', 'Vumi API Client/0.1')
        request.add_header('Authorization', 'Basic %s' % self.credentials)

        if self.debug:
            print 'REQUEST'
            print '\theaders:', request.headers
            print '\tmethod:', method
            print '\tpath:', path
            print '\tdata:', data
            print '\turl_data', url_data
            print

        response = ApiResponse(urllib2.urlopen(request))

        if self.debug:
            print 'RESPONSE'
            print '\tstatus_code:', response.status_code
            print '\theaders:', response.url_response.info()
            print '\tcontent:', response.content
            print

        return response

    def get(self, *args, **kwargs):
        return self.request('get', *args, **kwargs)

    def post(self, *args, **kwargs):
        return self.request('post', *args, **kwargs)

    def put(self, *args, **kwargs):
        return self.request('put', *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.request('delete', *args, **kwargs)


class Client(object):
    """
    A client for Vumi's HTTP API.
    """

    def __init__(self, username, password, connection=None):
        self.connection = connection or Connection(username, password,
                base_url='http://vumi.praekeltfoundation.org')

    @auto_response
    def send_sms(self, **kwargs):
        """
        Send one or more SMS messages::

            >>> client = Client('username', 'password')
            >>> client.send_sms(to_msisdn='27123456789', from_msisdn='27123456789', message='hello world') # doctest: +SKIP
            [SendSmsResponse(...), ...]
            >>>

        :keyword to_msisdn: the msisdn of the recipient
        :keyword from_msisdn: the msisdn of the sender
        :keyword message: the message being sent

        """
        return self.connection.post('/api/v1/sms/send.json', kwargs)

    @auto_response
    def send_template_sms(self, **kwargs):
        """
        Send one or more personalized SMS messages::

            >>> client = Client('username', 'password')
            >>> client.send_template_sms(template = 'Hello {{ name }} {{ surname }}',
            ...        from_msisdn = '27123456789',
            ...        to_msisdns = {
            ...            '27123456788': {
            ...                'name': 'Foo',
            ...                'surname': 'Bar'
            ...            },
            ...            '27123456789': {
            ...                'name': 'Boo',
            ...                'surname': 'Far'
            ...            }
            ...        }) # doctest: +SKIP
            [SendTemplateSmsResponse(...), ...]
            >>>

        :keyword template: a Mustache_ template
        :keyword from_msisdn: the MSISDN of the sender
        :keyword to_msisdns: a dictionary where the key is the MSISDN and the
                             values is the template context for that MSISDN

        .. _Mustache: https://github.com/defunkt/pystache

        """
        # make sure we order the entries consistently
        to_msisdns = kwargs.get('to_msisdns', {})
        entries = [(msisdn, context) for msisdn, context in to_msisdns.items()]
        kwargs = {
            'template': kwargs['template'],
            'from_msisdn': kwargs['from_msisdn'],
            'to_msisdn': [msisdn for msisdn, context in entries],
        }

        for msisdn, context in entries:
            for key, value in context.items():
                kwargs.setdefault('template_%s' % key, []).append(value)
        return self.connection.post('/api/v1/sms/template_send.json', kwargs)

    @auto_response
    def get_status(self, batch_id):
        """
        Get the status of a sent sms batch::

            >>> client = Client('username', 'password')
            >>> client.get_status(1) # doctest: +SKIP
            GetStatusResponse(...)
            >>>

        :param batch_id: the id of the batch that was sent

        """
        return self.connection.get('/api/v1/sms/status/%s.json' % batch_id)

    @auto_response
    def get_status_since(self, **kwargs):
        """
        Get the status of SMSs sent since a specific date::

            >>> client = Client('username', 'password')
            >>> client.get_status_since(since=datetime.now() - timedelta(days=1)) # doctest: +SKIP
            [GetStatusSinceResponse(...), ...]
            >>>

        :keyword since: timestamp for since when to query for messages sent
        """
        url_qs = urllib.urlencode(dict_to_tuple(kwargs))
        return self.connection.get('/api/v1/sms/status.json?%s' % url_qs)

    @auto_response
    def get_status_by_id(self, **kwargs):
        """
        Get the status of SMSs sent by their ids::

            >>> client = Client('username', 'password')
            >>> client.get_status_by_id(id=[1,2]) # doctest: +SKIP
            [GetStatusByIdResponse(...), ...]
            >>>

        :keyword ids: list of IDs to query
        """
        url_qs = urllib.urlencode(dict_to_tuple(kwargs))
        return self.connection.get('/api/v1/sms/status.json?%s' % url_qs)

    @auto_response
    def set_callback(self, event, url):
        """
        Specify a URL callback for an event type::

            >>> client = Client('username', 'password')
            >>> client.set_callback('sms_received', 'http://localhost/...') # doctest: +SKIP
            SetCallbackResponse(...)
            >>>

        :param event: the type of event, `sms_received` or `sms_delivered`
        :param url: the URL to HTTP POST to.

        """
        return self.connection.post('/api/v1/account/callbacks.json', {
            'name': event,
            'url': url,
        })

    @auto_response
    def update_callback(self, id, url):
        """
        Update a URL callback::

            >>> client = Client('username', 'password')
            >>> client.update_callback(1, 'http://localhost/update/...') # doctest: +SKIP
            UpdateCallbackResponse(...)
            >>>

        :param id: id of the callback being updated
        :param url: the URL to HTTP POST to.

        """
        return self.connection.put('/api/v1/account/callbacks/%s.json' % id, {
            'url': url,
        })

    def delete_callback(self, id):
        """
        Delete a URL callback::

            >>> client = Client('username', 'password')
            >>> client.delete_callback(1) # doctest: +SKIP
            DeleteCallBackResponse(success=True)
            >>>

        :param id: the id of the callback to delete
        """
        resp = self.connection.delete('/api/v1/account/callbacks/%s.json' % id)
        if is_successful(resp):
            return make_response('DeleteCallBackResponse', {'success': True})
        raise ClientException('Bad Response: %s' % resp.status_code)
