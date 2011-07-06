import logging
from django.http import HttpResponse


def example_sms_callback(request):
    logging.debug("Received callback:" % request.POST)
    return HttpResponse("OK!")
