# -*- test-case-name: vumi.demos.tests.test_calculator -*-
import operator

from vumi.application import ApplicationWorker


def mk_menu(preamble, options):
    return '\n'.join(
        [preamble] +
        ['%s. %s' % (idx, action)
         for (idx, (action, op)) in enumerate(options, 1)])


class CalculatorApp(ApplicationWorker):

    def setup_application(self):
        self._sessions = {}
        self.actions = [
            ('Add', operator.add),
            ('Subtract', operator.sub),
            ('Multiply', operator.mul),
        ]

    def teardown_application(self):
        pass

    def save_session(self, user_id, data):
        self._sessions[user_id] = data
        return data

    def get_session(self, user_id):
        return self._sessions.get(user_id, {})

    def clear_session(self, user_id):
        return self.save_session(user_id, {})

    def new_session(self, message):
        self.clear_session(message.user())
        return self.reply_to(message, mk_menu(
            'What would you like to do?', self.actions))

    def close_session(self, message):
        self.clear_session(message.user())

    def consume_user_message(self, message):
        user_id = message.user()
        session = self.get_session(user_id)

        try:
            numeric_input = int(message['content'])
        except (ValueError, TypeError):
            self.clear_session(message.user())
            return self.reply_to(
                message, 'Sorry invalid input!', continue_session=False)

        if 'action' not in session:
            action_index = numeric_input - 1
            try:
                action, op = self.actions[action_index]
            except IndexError:
                return self.new_session(message)

            session['action'] = action_index
            d = self.reply_to(message, 'What is the first number?')
            d.addCallback(lambda *a: self.save_session(user_id, session))
            return d

        if 'first_number' not in session:
            session['first_number'] = numeric_input
            d = self.reply_to(message, 'What is the second number?')
            d.addCallback(lambda *a: self.save_session(user_id, session))
            return d

        if 'second_number' not in session:
            session['second_number'] = numeric_input
            result = self.calculate(session)
            d = self.reply_to(message, result, continue_session=False)
            d.addCallback(lambda *a: self.save_session(user_id, session))
            return d

    def calculate(self, session):
        action, op = self.actions[session['action']]
        result = op(session['first_number'], session['second_number'])
        return 'The result is: %s.' % (result,)
