# TODO definitly need a session object
# with hist & expiry & to_str & to_json etc
import yaml
import json
from vumi.errors import VumiError


class VumiSession():
    key = None
    decision_tree = None

    def __init__(self, **kwargs):
        pass

    def set_decision_tree(self, decision_tree):
        self.decision_tree = decision_tree

    def get_decision_tree(self):
        return self.decision_tree




class DecisionTree():
    pass


class TemplatedDecisionTree(DecisionTree):
    template = None

    def load_yaml_template(self, yaml_string):
        self.template = yaml.load(yaml_string)

    def get_template(self):
        return self.template


class PopulatedDesicionTree(TemplatedDecisionTree):
    data = None

    def load_json_data(self, json_string):
        self.data = json.loads(json_string)

    def get_data(self):
        return self.data


class TraversedDecisionTree(PopulatedDesicionTree):
    completed = False
    language = "english"
    template_current = None
    template_history = []
    data_current = None
    data_history = []

    def is_completed(self):
        return self.completed


    def dumps(self):
        s = ""
        s += "\nTEMPLATE:  "
        s += repr(self.template)
        s += "\nTEMPLATE_CURRENT:  "
        s += repr(self.template_current)
        s += "\nTEMPLATE_HISTORY:  "
        s += repr(self.template_history)
        s += "\nDATA:  "
        s += repr(self.data)
        s += "\nDATA_CURRENT:  "
        s += repr(self.data_current)
        s += "\nDATA_HISTORY:  "
        s += repr(self.data_history)
        return s


    def start(self):
        if not self.template:
            raise VumiError("template must be loaded")
        if not self.data:
            raise VumiError("data must be loaded")
        t = self.template.get(self.template.get("__start__"))
        d = self.data.get(self.template.get("__start__"))
        self.select(t, d)


    def select(self, template, data):
        self.template_history.append(self.template_current)
        self.data_history.append(self.data_current)
        self.template_current = template
        self.data_current = data


    def previous(self):
        try:
            self.template_current = self.template_history.pop()
            self.data_current = self.data_history.pop()
        except:
            pass


    def question(self):
        que = ""
        que += self.template_current['question'][self.language]
        count = 0
        for opt in self.data_current:
            count += 1
            que += "\n" + str(count) + ". " + opt.get(self.template_current['options'])
        return que


    def answer(self, ans):
        pass






