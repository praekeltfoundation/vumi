import ast
from functools import partial


def _mknode(cls, **kw):
    "Make an AST node with the relevant bits attached."
    node = cls()
    node.lineno = 0
    node.col_offset = 0
    for k, v in kw.items():
        setattr(node, k, v)
    return node


# Some conveniences for building the AST.
arguments = partial(_mknode, ast.arguments)
Call = partial(_mknode, ast.Call)
Attribute = partial(_mknode, ast.Attribute)
FunctionDef = partial(_mknode, ast.FunctionDef)
Return = partial(_mknode, ast.Return)
Module = partial(_mknode, ast.Module)

_param = lambda name: _mknode(ast.Name, id=name, ctx=ast.Param())
_load = lambda name: _mknode(ast.Name, id=name, ctx=ast.Load())
_kw = lambda name: _mknode(ast.keyword, arg=name, value=_load(name))


def make_function(name, func, args, vararg=None, kwarg=None, defaults=()):
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
