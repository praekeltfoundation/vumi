
from copy import deepcopy


__dictionary = {}


def set(dictionary):
    global __dictionary
    __dictionary = deepcopy(dictionary)


def reset():
    global __dictionary
    __dictionary = {}


def update(dictionary):
    global __dictionary
    __dictionary.update(deepcopy(dictionary))


def delete(key):
    global __dictionary
    del __dictionary[key]


def get():
    global __dictionary
    return deepcopy(__dictionary)
