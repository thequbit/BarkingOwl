import daemon

import datetime
from dispatcher import Dispatcher

def StartDispatcher():

    with daemon.DaemonContext():
        try:
            dispatcher = Dispatcher(
                address='localhost',
                exchange='barkingowl',
                self_dispatch=False,
                DEBUG=True
            )

            url = {
                'target_url': "http://timduffy.me/",
                'doc_types': [
                'application/pdf',
                    #'*',
                ],
                'title': "TimDuffy.Me",
                'description': "Tim Duffy's Personal Website",
                'max_link_level': 1,
                'creation_datetime': str(datetime.datetime.now()),
                'allowed_domains': [
                ],
                'sleep_time': 0, # 1 secon
            }

            urls = []
            urls.append(url)

            dispatcher.set_urls(urls)

            # note: blocking
            dispatcher.start()

        except Exception, e:
            print "Error: {0}".format(e)

if __name__ == '__main__':

    print ("Launching BarkingOwl Dispatcher as daemon process.\n"
           "To shutdown dispatcher, issue (global) shutdown command.\n")

    StartDispatcher()
