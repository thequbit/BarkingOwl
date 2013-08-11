from pyramid.view import view_config

from db.urls import urls
from db.phrases import phrases
from db.docs import docs
from db.urlphrases import urlphrases
from db.finds import finds
from db.scraps import scraps
from db.runs import runs

import simplejson

@view_config(renderer="urls.pt", name="urls")
def add_url(request):
    if request.method == 'POST':
        print request.body
        print request.json_body
        params = simplejson.loads(request.json_body)
        print params['a']
    return {}

@view_config(renderer="index.pt")
def index_view(request):
    return {}

