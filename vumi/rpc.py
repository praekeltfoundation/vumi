# -*- coding: utf-8 -*-

"""Utilties for marking up RPC methods."""

import inspect
import textwrap
import functools
import itertools

from twisted.internet.defer import Deferred


class RpcCheckError(Exception):
    """Raised when a value fails a type check."""


class Signature(object):

    NO_DEFAULT = object()
    NO_ARG = object()

    def __init__(self, f, returns=None, requires_self=True, **kw):
        self.returns = returns if returns is not None else Null()
        self.requires_self = requires_self
        self.params = kw
        self.argspec = inspect.getargspec(f)
        self.defaults = [self.NO_DEFAULT] * (
            len(self.argspec.args) - len(self.argspec.defaults or ()))
        self.defaults += list(self.argspec.defaults or ())

    def check_params(self, args, kw):
        if kw:
            raise RpcCheckError("Keyword parameters not yet supported.")
        if len(args) > len(self.argspec.args):
            raise RpcCheckError("Too many positional arguments.")

        missing_arg_count = len(self.argspec.args) - len(args)
        args = list(args) + [self.NO_ARG] * missing_arg_count
        arg_tuples = itertools.izip(self.argspec.args, self.defaults, args)
        if self.requires_self:
            next(arg_tuples)

        for arg_name, default, arg_value in arg_tuples:
            if arg_value is self.NO_ARG:
                arg_value = default
            if arg_value is self.NO_DEFAULT:
                raise RpcCheckError("Positional argument %r missing"
                                    " but no default is available." % arg_name)
            arg_type = self.params[arg_name]
            arg_type.check(arg_name, arg_value)

    def check_result(self, result):
        self.returns.check('return value', result)
        return result

    def _wrap_help(self, help_text):
        indent = '    '
        return textwrap.wrap(help_text, initial_indent=indent,
                             subsequent_indent=indent)

    def _format_param(self, param_name, param_type, default):
        lines = [":param %s %s:" % (param_type.name, param_name)]
        help_text = param_type.help()
        if param_type.nullable():
            help_text += " May be null."
        if default is not self.NO_DEFAULT:
            help_text += " Default: %r." % (default,)
        lines.extend(self._wrap_help(help_text))
        return lines

    def _format_return(self, param_type):
        lines = [":rtype %s:" % (param_type.name,)]
        lines.extend(self._wrap_help(param_type.help()))
        return lines

    def _args_with_defaults(self):
        args_defaults = itertools.izip(self.argspec.args, self.defaults)
        if self.requires_self:
            next(args_defaults)

        for arg, default in args_defaults:
            yield arg, self.params[arg], default

    def param_doc(self):
        lines = []
        for arg, arg_type, default in self._args_with_defaults():
            lines.extend(self._format_param(arg, self.params[arg], default))
        lines.extend(self._format_return(self.returns))
        return lines

    def jsonrpc_signature(self):
        sig = [self.returns.jsonrpc_type]
        sig.extend(arg_type.jsonrpc_type for _, arg_type, _
                   in self._args_with_defaults())
        return [sig]


def signature(**kw):
    def decorator(f):
        sig = Signature(f, **kw)

        def wrapper(*args, **kw):
            sig.check_params(args, kw)
            result = f(*args, **kw)
            if isinstance(result, Deferred):
                result.addCallback(sig.check_result)
            else:
                sig.check_result(result)
            return result

        functools.update_wrapper(wrapper, f)
        doc = textwrap.wrap(wrapper.__doc__ or '')
        doc.append("")
        doc.extend(sig.param_doc())
        wrapper.__doc__ = "\n".join(doc)
        wrapper.signature = sig.jsonrpc_signature()
        wrapper.signature_object = sig
        return wrapper

    return decorator


class RpcType(object):

    # See: http://xmlrpc.scripting.com/spec.html
    # valid simple types are:
    #    int, boolean, string, double, base64 and dateTime.iso8601
    # valid compound types are:
    #    array, struct
    jsonrpc_type = None

    def __init__(self, help=None, null=False):
        self._help = help
        self._null = null

    @property
    def name(self):
        return self.__class__.__name__

    def help(self):
        return self._help or ''

    def nullable(self):
        return self._null

    def check(self, name, value):
        if value is None:
            if not self._null:
                raise RpcCheckError("%s may not be None (got None)" % (name,))
            return
        self.nonnull_check(name, value)

    def nonnull_check(self, name, value):
        raise RpcCheckError("The base class RpcType accepts no values.")


class Null(RpcType):
    jsonrpc_type = 'null'

    def __init__(self, *args, **kw):
        kw.setdefault('null', True)
        super(Null, self).__init__(*args, **kw)

    def nonnull_check(self, name, value):
        if value is not None:
            raise RpcCheckError("Null value expected for %s (got %r)"
                                % (name, value))


class Unicode(RpcType):
    jsonrpc_type = 'string'

    def nonnull_check(self, name, value):
        if not isinstance(value, unicode):
            raise RpcCheckError("Unicode value expected for %s (got %r)"
                                % (name, value))


class Int(RpcType):
    jsonrpc_type = 'int'

    def nonnull_check(self, name, value):
        if not isinstance(value, (int, long)):
            raise RpcCheckError("Int value expected for %s (got %r)"
                                % (name, value))


class List(RpcType):
    jsonrpc_type = 'array'

    def __init__(self, *args, **kw):
        self._item_type = kw.pop('item_type', None)
        self._length = kw.pop('length', None)
        super(List, self).__init__(*args, **kw)

    def nonnull_check(self, name, value):
        if not isinstance(value, list):
            raise RpcCheckError("List value expected for %s (got %r)"
                                % (name, value))
        if self._length is not None and len(value) != self._length:
            raise RpcCheckError("List value for %s expected to have"
                                " length %d (got %r)"
                                % (name, self._length, value))
        if self._item_type is not None:
            item_name = 'items of %s' % (name,)
            for item in value:
                self._item_type.check(item_name, item)


class Dict(RpcType):
    jsonrpc_type = 'struct'

    def __init__(self, *args, **kw):
        self._item_type = kw.pop('item_type', None)
        self._required_fields = kw.pop('required_fields', {})
        self._optional_fields = kw.pop('optional_fields', {})
        self._closed = kw.pop('closed', False)
        self._no_checks = all(not x for x in (
            self._item_type, self._required_fields, self._optional_fields,
            self._closed))
        super(Dict, self).__init__(*args, **kw)

    def nonnull_check(self, name, value):
        if not isinstance(value, dict):
            raise RpcCheckError("Dict value expected for %s (got %r)"
                                % (name, value))
        if self._no_checks:
            return
        for key in value:
            field_type = self._required_fields.get(key)
            field_type = (self._optional_fields.get(key)
                          if field_type is None else field_type)
            if field_type is None:
                if self._closed:
                    raise RpcCheckError("Dict received unexpected key %s"
                                        " (got %r)" % (key, value))
                field_type = self._item_type
            if field_type is not None:
                field_type.check('item %s of %s' % (key, name), value[key])
        for key in self._required_fields:
            if key not in value:
                raise RpcCheckError("Dict requires key %s (got %r)"
                                    % (key, value))


class Tag(RpcType):
    jsonrpc_type = 'array'

    def nonnull_check(self, name, value):
        if not isinstance(value, (list, tuple)):
            raise RpcCheckError("Tag %s must be a list or tuple (got %r)"
                                % (name, value))
        if len(value) != 2:
            raise RpcCheckError("Tag %s must contain two elements, a pool name"
                                " and a tag name (got %r)"
                                % (name, value))
        for item in value:
            if not isinstance(item, unicode):
                raise RpcCheckError("Tag %s must have unicode pool and tag"
                                    " name (got %r)" % (name, value))
