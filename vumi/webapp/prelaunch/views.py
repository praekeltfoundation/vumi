# Create your views here.
from django.shortcuts import render_to_response
from vumi.webapp.prelaunch.models import Registrant
from django.template import RequestContext


def thanks(request):
    if request.POST:
        fn = request.POST.get('name')
        ea = request.POST.get('email')
        registrant, _ = Registrant.objects.get_or_create(full_name=fn,
                                                         email_address=ea)
    return render_to_response("thanks.html", locals(),
                              context_instance=RequestContext(request))


def process(request):
    if request.POST:
        ea = request.POST.get('email')
        registrant, _ = Registrant.objects.get_or_create(email_address=ea)
    return render_to_response("process.html", locals(),
                              context_instance=RequestContext(request))
