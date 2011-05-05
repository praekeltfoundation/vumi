# TODO definitly need a session object
# with hist & expiry & to_str & to_json etc
import yaml
import json

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

    def is_completed(self):
        return self.completed






