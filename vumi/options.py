
from copy import deepcopy


__dictionary = None


def set(dictionary):
    global __dictionary
    if __dictionary == None:
        __dictionary = {}
        __dictionary.update(deepcopy(dictionary))
    else:
        raise Exception("Warning, vumi.options have been set and"
                        " may not be re-set!")


def get_all():
    global __dictionary
    return deepcopy(__dictionary) or {}


def is_set():
    global __dictionary
    return __dictionary != None


#def get_deploy_int(deployment):
    #lookup = {
        #"develop": 7,
        #"/develop": 7,
        #"development": 7,
        #"/development": 7,
        #"production": 8,
        #"/production": 8,
        #"staging": 9,
        #"/staging": 9,
        #"qa": 9,
        #"/qa": 9,
        #}
    #return lookup.get(deployment.lower(), 7)
