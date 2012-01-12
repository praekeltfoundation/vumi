import yaml
import json
import time
import datetime

from vumi.errors import VumiError


def getVumiSession(r_server, key):
    sess = r_server.get(key)
    if sess:
        session = yaml.safe_load(sess)
        session.set_r_server(r_server)
        return session
    else:
        session = VumiSession()
        session.set_r_server(r_server)
        session.set_key(key)
        session.save()
        return session


def delVumiSession(r_server, key):
    return r_server.delete(key)


class VumiSession(yaml.YAMLObject):
    # Allow use with safe_load() / safe_dump()
    yaml_loader = yaml.SafeLoader
    yaml_dumper = yaml.SafeDumper

    yaml_tag = u'VumiSession'

    key = None
    decision_tree = None
    r_server = None

    def __init__(self, **kwargs):
        pass

    def set_decision_tree(self, decision_tree):
        self.decision_tree = decision_tree

    def get_decision_tree(self):
        return self.decision_tree

    def set_key(self, key):
        self.key = key

    def get_key(self):
        return self.key

    def set_r_server(self, r_server):
        self.r_server = r_server

    def save(self):
        if self.r_server:
            r_server = self.r_server
            self.r_server = None
            r_server.set(self.get_key(), yaml.safe_dump(self))
            self.r_server = r_server

    def delete(self):
        self.r_server.delete(self.get_key())
        self.r_server = None


class DecisionTree(yaml.YAMLObject):
    # Allow use with safe_load() / safe_dump()
    yaml_loader = yaml.SafeLoader
    yaml_dumper = yaml.SafeDumper

    yaml_tag = u'DecisionTree'

    def __init__(self):
        pass

    def serialize_to_json(self):
        return json.dumps(self.__dict__)

    def deserialize_from_json(self, string):
        self.__dict__ = json.loads(string)


class TemplatedDecisionTree(DecisionTree):

    yaml_tag = u'TemplatedDecisionTree'

    def __init__(self):
        DecisionTree.__init__(self)
        self.template = None
        self.template_current = None
        self.template_history = []

    def load_yaml_template(self, yaml_string):
        self.template = yaml.safe_load(yaml_string)
        self.template_current = self.template.get('__start__')

    def get_template(self):
        return self.template

    def get_data_source(self):
        if self.template:
            if self.template.get('__data__'):
                return {
                    "url": self.template['__data__'].get('url'),
                    "username": self.template['__data__'].get('username'),
                    "password": self.template['__data__'].get('password'),
                    "params": self.template['__data__'].get('params', []),
                    }
        return {"url": None, "username": None, "password": None, "params": []}

    def get_post_source(self):
        if self.template:
            if self.template.get('__post__'):
                return {
                    "url": self.template['__post__'].get('url'),
                    "username": self.template['__post__'].get('username'),
                    "password": self.template['__post__'].get('password'),
                    "params": self.template['__post__'].get('params', []),
                    }
        return {"url": None, "username": None, "password": None, "params": []}

    def get_dummy_data(self):
        if self.template:
            if self.template.get('__data__'):
                return self.template['__data__'].get('json')
        return None


class PopulatedDecisionTree(TemplatedDecisionTree):

    yaml_tag = u'PopulatedDecisionTree'

    def __init__(self):
        TemplatedDecisionTree.__init__(self)
        self.data = None
        # So that I can modify the original data, data_current must
        # be stored by reference as a list/dict, index/key pair
        self.data_current = ([None], 0)
        self.data_history = []

    def load_json_data(self, json_string):
        self.data = json.loads(json_string)

    def load_dummy_data(self):
        self.load_json_data(self.get_dummy_data())

    def dump_json_data(self):
        return json.dumps(self.data)

    def get_data(self):
        return self.data

    def resolve_dc(self):
        return self.data_current[0][self.data_current[1]]

    def update_dc(self, new_value):
        self.data_current[0][self.data_current[1]] = new_value


class TraversedDecisionTree(PopulatedDecisionTree):

    yaml_tag = u'TraversedDecisionTree'

    def __init__(self):
        PopulatedDecisionTree.__init__(self)
        self.max_chars = 140
        self.list_pos = {'offset': 0, 'length': 0, 'remainder': 0}
        self.echo = False
        self.started = False
        self.completed = False
        self.language = "english"

    # The data will be a nested data-structure of a sort that can
    # be deserialized from a JSON string.

    # This means that when traversing it the current object will be one of:
    #     dict    -> in which case the template should auto-select the
    #                correct key
    #     list    -> in which case the user should be asked from displayed
    #                options where there is only one list option it should be
    #                auto-selected
    #     boolean -> as for list, just yes/no questions only
    #     number  -> in which case the user should be prompted for a value
    #     string  -> in which case the user should be prompted for text

    # NB. User entered strings (which include things like dates),
    #     should be avoided where possible.

    def is_completed(self):
        return self.completed

    def is_started(self):
        return self.started

    def echo_on(self):
        self.echo = True

    def set_language(self, language):
        self.language = language

    def dumps(self, level=1, serialize=repr):
        def wrap(title, serialize, obj):
            return "\n****" + str(title) + "****\n" \
                    + serialize(obj)
        s = ""
        if level >= 1:
            s += wrap("TEMPLATE", serialize, self.template)
        if level >= 0:
            s += wrap("TEMPLATE_CURRENT", serialize, self.template_current)
        if level >= 2:
            s += wrap("TEMPLATE_HISTORY", serialize, self.template_history)
        if level >= 1:
            s += wrap("DATA", serialize, self.data)
        if level >= 0:
            s += wrap("DATA_CURRENT", serialize, self.data_current)
        if level >= 2:
            s += wrap("DATA_HISTORY", serialize, self.data_history)
        return s

    def select(self, template, data):
        self.list_pos = {'offset': 0, 'length': 0, 'remainder': 0}
        self.template_history.append(self.template_current)
        self.data_history.append(self.data_current)
        self.template_current = template
        self.data_current = data

    def try_auto_select(self):
        if type(self.resolve_dc()) == list and len(self.resolve_dc()) == 1:
            __next = self.template_current.get('next')
            d = (self.resolve_dc()[0], __next)
            t = self.template.get(__next)
            self.select(t, d)
            return True
        return False

    def go_back(self):
        try:
            self.list_pos = {'offset': 0, 'length': 0, 'remainder': 0}
            self.template_current = self.template_history.pop()
            self.data_current = self.data_history.pop()
            self.completed = False
        except:
            pass

    def go_up(self):
        old_data_current_0 = self.data_current[0]
        self.go_back()
        while old_data_current_0 == self.data_current[0] \
                and len(self.data_history) > 0:
            self.go_back()

    def start(self):
        self.started = True
        greeting = ''
        if self.template_current.get('display'):
            greeting += self.template_current['display'].get(
                    self.language, '')
        if self.echo:
            print "\n", greeting
        if not self.template:
            raise VumiError("template must be loaded")
        if not self.data:
            raise VumiError("data must be loaded")
        __next = self.template_current.get('next')
        t = self.template.get(__next)
        d = (self.data, __next)
        self.select(t, d)
        return greeting

    def __finish(self):
        self.completed = True

    def finish(self):
        if self.completed:
            goodbye = ''
            if self.template_current.get('display'):
                goodbye += self.template_current['display'].get(
                        self.language, '')
            if self.echo:
                print goodbye
            return goodbye
        else:
            return None

    def question(self):
        self.try_auto_select()
        offset = self.list_pos['offset']
        count = 0
        index = 0
        que = ""
        try:
            more_option = "\n0. " + \
                          self.template_current['more'][self.language]
        except:
            more_option = "\n0. ..."
        que += self.template_current['question'][self.language]
        if type(self.resolve_dc()) == list:
            list_length = len(self.resolve_dc())
            last_index = 0
            for opt in self.resolve_dc():
                index += 1
                more_length = len(more_option)
                if index == list_length:
                    more_length = 0
                if index > offset and count < 9:
                    option_str = "\n" + str(count + 1) + ". "
                    option_str += str(opt.get(
                                      self.template_current['options']))
                    if len(que + option_str) + more_length < self.max_chars:
                        count += 1
                        last_index = index
                        que += option_str
                    else:
                        index = -1
            remainder = list_length - last_index
            self.list_pos = {'offset': offset, 'length': count,
                             'remainder': remainder}
            if remainder:
                que += more_option
        elif type(self.template_current.get('options')) == list:
            for opt in self.template_current.get('options'):
                count += 1
                que += "\n" + str(count) + ". "
                que += str(opt.get('display').get(self.language))
        if self.echo:
            print que
        return que

    def answer(self, ans):
        if self.echo:
            print ">", ans, "\n"
        ans = str(ans)  # in reality we'll only get text
        try:
            if type(self.resolve_dc()) == list:
                if int(ans) == 0 and self.list_pos['remainder'] > 0:
                    self.list_pos['offset'] = self.list_pos['offset'] + \
                                              self.list_pos['length']
                    return None
        except:
            pass
        try:
            ans = self.validate(ans, self.template_current.get('validate'))
            __next = self.template_current.get('next')
            if type(self.resolve_dc()) == list:
                d = (self.resolve_dc()[int(ans) - 1 + self.list_pos['offset']],
                     __next)
                t = self.template.get(__next)
            elif type(self.template_current.get('options')) == list:
                opt = self.template_current.get('options')[int(ans) - 1]
                __next = opt.get('next')
                if opt.get('default'):
                    self.update_dc(self.resolve_default(opt['default']))
                    t = self.template.get(__next)
                else:
                    t = __next
                d = self.data_current
            else:
                self.update_dc(ans)
                d = (self.data_current[0], __next)
                t = self.template.get(__next)
            self.select(t, d)
            if __next == "__finish__":
                self.__finish()
        except:
            pass

    def validate(self, ans, validate):
        if validate == 'date':
            return str(int(time.mktime(
                datetime.datetime.strptime(ans, '%d/%m/%Y').timetuple())))
        if validate == 'integer':
            return str(int(ans))
        return ans

    def resolve_default(self, default):
        if default == 'today':
            return str(int(time.mktime(
                datetime.date.today().timetuple())))
        if default == 'yesterday':
            return str(int(time.mktime(
                (datetime.date.today() - datetime.timedelta(days=1))
                .timetuple())))
        return default
