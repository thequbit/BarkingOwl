import datetime
from dispatcher import Dispatcher

def StartDispatcher():

    dispatcher = Dispatcher(
        address='localhost',
        exchange='barkingowl',
        self_dispatch=False,
        DEBUG=True
    )

    url = {
        'target_uerl': "http://timduffy.me/",
        'doc_types': [
            'application/pdf',
        ],
        'title': "TimDuffy.Me",
        'description': "Tim Duffy's Personal Website",
        'max_link_level': 1,
        'creation_datetime': str(datetime.datetime.now()),
        'allowed_domains': [

        ],
    }

    urls = []
    urls.append(url)

    dispatcher.set_urls(urls)

    # note: blocking
    dispatcher.start()

if __name__ == '__main__':

    print "BarkingOwl Dispatcher Starting."

#    try:
    if True:
        StartDispatcher()
#    except:
#        pass

#    print "BarkingOwl Dispatcher Exiting."
