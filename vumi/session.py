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

    def load_yaml(self, yaml_string):
        self.template = yaml.load(yaml_string)

    def get_template(self):
        return self.template


class PopulatedDesicionTree(TemplatedDecisionTree):

    def load_json(self, json):
        pass


class TraversedDecisionTree(PopulatedDesicionTree):
    completed = False

    def is_completed(self):
        return self.completed






