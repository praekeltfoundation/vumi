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
    current_template = None
    history_template = []
    current_data = None
    history_data = []

    def is_completed(self):
        return self.completed

    def start(self):
        if not self.template:
            raise VumiError("template must be loaded")
        if not self.data:
            raise VumiError("data must be loaded")


    def __next(self, _template, _data):
        pass


    def __back(self):
        return (None, None)




    def question(self):
        pass

    def answer(self, ans):
        pass






