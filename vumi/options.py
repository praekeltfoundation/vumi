
from copy import deepcopy


__dictionary = None

def set(dictionary):
    global __dictionary
    if __dictionary == None:
        __dictionary = {}
        __dictionary.update(deepcopy(dictionary))
    else:
        raise Exception(
                "Warning, vumi.options have been set and may not be re-set!"
                )

def get():
    global __dictionary
    return deepcopy(__dictionary) or {}


def is_set():
    global __dictionary
    return __dictionary != None

