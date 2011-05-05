# TODO definitly need a session object
# with hist & expiry & to_str & to_json etc

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

    def load_yaml(self, yaml):
        pass


class PopulatedDesicionTree(TemplatedDecisionTree):

    def load_json(self, json):
        pass


class TraversedDecisionTree(PopulatedDesicionTree):
    completed = False




