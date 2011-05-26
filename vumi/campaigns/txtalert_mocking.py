from datetime import datetime

"""Mocking classes for getting txtAlert USSD to work"""
class Visit(object):
    def __init__(self):
        self.date = datetime.now()
        self.clinic = 'Clinic A'
    
    def reschedule_earlier_date(self):
        return True
    def reschedule_earlier_time(self):
        return True
    def reschedule_later_time(self):
        return True
    def reschedule_later_date(self):
        return True
    

class Patient(object):
    def __init__(self):
        self.attendance = '85%'
        self.attended = 60
        self.total = 85
        self.rescheduled = 5
        self.missed = 15
    
    def next_visit(self):
        return Visit()
