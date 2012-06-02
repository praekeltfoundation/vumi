import _ast
from functools import partial


# There are two versions of make_function(). One builds an AST and the other
# builds a string. They should be equivalent, and can be swapped at the bottom
# of the module. I (jerith) prefer the AST version, but am happy to use
# whichever one people thing is clearer and/or saner.


def make_function_s(name, func, args, vararg=None, kwarg=None, defaults=()):
    "Create a function that has a nice signature and calls out to ``func``."

    # Give our default arguments names so we can shove them in globals.
    dflts = [("default_%s" % i, d) for i, d in enumerate(defaults)]

    # Build up the argument list for our function def.
    argspec = []
    argspec.extend(args[:len(args) - len(defaults)])
    argspec.extend("%s=%s" % (a, d[0])
                   for a, d in zip(args[len(args) - len(defaults):], dflts))
    if vararg is not None:
        argspec.append('*' + vararg)
    if kwarg is not None:
        argspec.append('**' + kwarg)
    argspec = ', '.join(argspec)

    # Build up the argument list for our function call.
    callargs = []
    callargs.extend(args[:len(args) - len(defaults)])
    callargs.extend("%s=%s" % (a, a) for a in args[len(args) - len(defaults):])
    if vararg is not None:
        callargs.append('*' + vararg)
    if kwarg is not None:
        callargs.append('**' + kwarg)
    callargs = ', '.join(callargs)

    # Construct a string containing our function.
    funcstr = '\n'.join([
            "def %(name)s(%(argspec)s):",
            "    return func(%(callargs)s)"])
    funcstr = funcstr % {
        'name': name, 'argspec': argspec, 'callargs': callargs}

    # Build up locals and globals, then compile and extract our function.
    locs = {}
    globs = dict(globals(), func=func, **dict(dflts))
    eval(compile(funcstr, '<string>', 'exec'), globs, locs)
    return locs[name]


def _mknode(cls, **kw):
    "Make an AST node with the relevant bits attached."
    node = cls()
    node.lineno = 0
    node.col_offset = 0
    for k, v in kw.items():
        setattr(node, k, v)
    return node


# Some conveniences for building the AST.
arguments = partial(_mknode, _ast.arguments)
Call = partial(_mknode, _ast.Call)
Attribute = partial(_mknode, _ast.Attribute)
FunctionDef = partial(_mknode, _ast.FunctionDef)
Return = partial(_mknode, _ast.Return)
Module = partial(_mknode, _ast.Module)

_param = lambda name: _mknode(_ast.Name, id=name, ctx=_ast.Param())
_load = lambda name: _mknode(_ast.Name, id=name, ctx=_ast.Load())
_kw = lambda name: _mknode(_ast.keyword, arg=name, value=_load(name))


def make_function_a(name, func, args, vararg=None, kwarg=None, defaults=()):
    "Create a function that has a nice signature and calls out to ``func``."

    # Give our default arguments names so we can shove them in globals.
    dflts = [("default_%s" % i, d) for i, d in enumerate(defaults)]

    # Build args and default lists for our function def.
    a_args = [_param(a) for a in args]
    a_defaults = [_load(k) for k, v in dflts]

    # Build args and keywords lists for our function call.
    c_args = [_load(a) for a in args[:len(args) - len(defaults)]]
    c_keywords = [_kw(a) for a in args[len(args) - len(defaults):]]

    # Construct the call to our external function.
    call = Call(func=_load('func'), args=c_args, keywords=c_keywords,
                starargs=(vararg and _load(vararg)),
                kwargs=(kwarg and _load(kwarg)))

    # Construct the function definition we're actually making.
    func_def = FunctionDef(
        name=name, args=arguments(
            args=a_args, vararg=vararg, kwarg=kwarg, defaults=a_defaults),
        body=[Return(value=call)], decorator_list=[])

    # Build up locals and globals, then compile and extract our function.
    locs = {}
    globs = dict(globals(), func=func, **dict(dflts))
    eval(compile(Module(body=[func_def]), '<ast>', 'exec'), globs, locs)
    return locs[name]


make_function = make_function_a
