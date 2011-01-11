from django.views.decorators.http import require_POST
from couchforms.util import post_xform_to_couch
from django.http import HttpResponse, HttpResponseServerError
import logging

@require_POST
def post(request, callback=None):
    """
    XForms can get posted here.  They will be forwarded to couch.
    
    Just like play, if you specify a callback you get called, 
    otherwise you get a generic response.  Callbacks follow
    a different signature as play, only passing in the document
    (since we don't know what xform was being posted to)
    """
    # just forward the post request to couch
    # this won't currently work with ODK
    # post to couch
    instance = request.raw_post_data
    try:
        doc = post_xform_to_couch(instance)
        if callback:
            return callback(doc)
        return HttpResponse("Thanks! Your new xform id is: %s" % doc["_id"])
    except Exception, e:
        logging.exception(e)
        return HttpResponseServerError("FAIL")
