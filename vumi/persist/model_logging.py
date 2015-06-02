from eliot import ActionType, Field
from eliot import fields as _fields


def tolist(things):
    try:
        return list(things)
    except TypeError:
        return [things]


def fields(*args, **kw):
    args = list(args)
    for k, v in kw.items():
        args.append(Field.forTypes(k, tolist(v), ""))
    return _fields(*args)


def opt(types):
    return tolist(types) + [None]


unistr = (str, unicode)


def riak_fields(*args, **kw):
    return fields(*args, manager=unistr, **kw)


def bucket_fields(*args, **kw):
    return riak_fields(*args, riak_bucket=unistr, **kw)


def key_fields(*args, **kw):
    return bucket_fields(*args, riak_key=unistr, **kw)


def model_fields(*args, **kw):
    return riak_fields(*args, modelcls=str, model_key=unistr, **kw)


# Low level operations

LOG_RIAK_INDEX = ActionType(
    u"riak:INDEX",
    # Start message fields:
    bucket_fields(
        index_name=unistr, start_value=unistr, end_value=opt(unistr),
        max_results=opt(int), continuation=opt(unistr), from_page=bool),
    # Success message fields:
    fields(has_next_page=bool),
    u"A Riak INDEX operation.")


LOG_RIAK_PUT = ActionType(
    u"riak:PUT",
    # Start message fields:
    key_fields(),
    # Success message fields:
    fields(),
    u"A Riak PUT operation.")


LOG_RIAK_GET = ActionType(
    u"riak:GET",
    # Start message fields:
    key_fields(),
    # Success message fields:
    fields(),
    u"A Riak GET operation.")


LOG_RIAK_DELETE = ActionType(
    u"riak:DELETE",
    # Start message fields:
    key_fields(),
    # Success message fields:
    fields(),
    u"A Riak DELETE operation.")


# Model level operations


LOG_MODEL_STORE = ActionType(
    u"riak_model:store",
    # Start message fields:
    model_fields(),
    # Success message fields:
    fields(was_migrated=bool),
    u"A Riak PUT operation.")


LOG_MODEL_DELETE = ActionType(
    u"riak_model:delete",
    # Start message fields:
    model_fields(),
    # Success message fields:
    fields(),
    u"A Riak PUT operation.")


LOG_MODEL_LOAD = ActionType(
    u"riak_model:load",
    # Start message fields:
    model_fields(),
    # Success message fields:
    fields(was_migrated=bool, object_found=bool),
    u"A Riak PUT operation.")
