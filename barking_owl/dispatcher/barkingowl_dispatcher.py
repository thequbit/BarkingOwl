import pika
import json
import uuid
from time import strftime
import time
import threading

import datetime

from barking_owl.busaccess import BusAccess

class Dispatcher():

    def __init__(self,address='localhost',exchange='barkingowl',
            self_dispatch=True,uid=str(uuid.uuid4()),url_parameters=None,
            DEBUG=False):
        """

        __init__() constructor will setup message bus as well as status variables.

        address
            default
                'localhost'
            description
                This is the address that the dispatcher will try and connect to the RabbitMQ 
                bus on.

        exchange
            default
                'barkingowl'
            description
                This is the RabbitMQ exchange that the dispatcher will be listening on and
                dispatching to.  All other elements in this BarkingOwl universe should be 
                on the same exchange name.

        self_dispatch
            default
                True
            description
                BarkingOwl has two main modes: self dispatch, and queue dispatch.  In self 
                dispatch mode the dispatched is loaded with URLs to dispatch once, and it
                handles when they are dispatched based on frequency values.  In queue mode
                a list of URLs are loaded into the dispatched and they are dispatched, in 
                order, as soon as a scraper anouncement is seen.  Frequency information is
                ignored in queue mode.  Once all of the urls have been dispatched, the 
                dispatcher waits in idle until set_urls() is called again.

        uid
            default
                str(uuid.uuid4()) [ creates a UUID for you that is statisticly unused ]
            description
                All elements within the BarkingOwl universe must have unique id's.  The 
                default value here creates a uuid for you to use.  If there is no reason
                to have the uuid a specfic string, then use the default.

        url_parameters
            default
                None
            description
                pika.URLParameters object holding the URL of the AMQ server to connec to.
                Defaults to None, and will not be looked at if None.  If not None, then
                the address field is ignored, and url_parameters is used.

        DEBUG
            default
                False
            description
                When set to True, lots of debug information will be printed to the screen.
                Not recommended for production.
               
        """ 


        self.address = address
        self.exchange = exchange
        self.self_dispatch = self_dispatch
        self.uid = uid
        self.url_parameters = url_parameters
        self.DEBUG = DEBUG

        self.interval = 5 # 5 seconds
        self.exiting = False

        self.current_url_index = 0

        self.urls = []
        self.scrapers = []
        #self.runs = []

        self.bus_access = BusAccess(
            uid = self.uid,
            address = self.address,
            exchange = self.exchange,
            url_parameters = self.url_parameters,
            DEBUG = self.DEBUG,
        )

        self.bus_access.set_callback(
            callback = self._reqcallback,
        )


        #setup message bus
#        self.reqcon = pika.BlockingConnection(pika.ConnectionParameters(host=address))
#        self.reqchan = self.reqcon.channel()
#        self.reqchan.exchange_declare(exchange=exchange,type='fanout')
#        result = self.reqchan.queue_declare(exclusive=True)
#        queue_name = result.method.queue
#        self.reqchan.queue_bind(exchange=exchange,queue=queue_name)
#        self.reqchan.basic_consume(self._reqcallback,queue=queue_name,no_ack=True)
#       
#        self.respcon = pika.BlockingConnection(pika.ConnectionParameters(host=self.address))
#        self.respchan = self.respcon.channel()
#        self.respchan.exchange_declare(exchange=self.exchange,type='fanout')
#
        self.broadcast_status()

    def set_urls(self,urls):
        """
        set_urls() expects an array of dictionaries that hold url data.  The format should
        be the following:
        
            url = {'target_url': target_url, # the root url to be scraped
                   'title': title, # a title for the URL
                   'description': descritpion, # a description of the url
                   'max_link_level': max_link_level, # the max link level for the scraper to follow to
                   'creation_datetime': creation_datetime, # ISO creation date and time
                   'doc_type': doc_type, # the text for the magic lib to look for (ex. 'application/pdf')
                   'frequency': frequency, # the frequency in minutes the URL should be scraped
                   'allowed_domains': [], # a list of allowable domains for the scraper to follow
                  }
        
        note: every time this function is called, the url list is reset, including the last
        time any scraper ran agaist a url.
        """

        # setup urls for future data
        for i in range(0,len(urls)):
            urls[i]['start_datetime'] = ""
            urls[i]['finish_datetime'] = ""
            urls[i]['runs'] = []

        self.current_url_index = 0

        if self.DEBUG:
            print "Updated URL list with {0} URLs".format(len(self.urls))

        self.urls = urls

    def get_next_url_index(self):
        """
        get_next_url_index() returns the index withint he self.urls list of the url to dispatch to 
        the scrapers.  This function has the logic within it to return the index of the url that
        has not run within the last defined frequency window.
        """

        url_index = -1
        for i in range(0,len(self.urls)):
            now = datetime.datetime.now() #strptime(strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S") # gross ...
            # check to see if it ever ran
            if self.urls[i]['start_datetime'] == "":
                if self.DEBUG:
                    print "URL has not run yet: '{0}'".format(self.urls[i]['target_url'])
                url_index = i
                break
            else:
                start_datetime = strdatetime.datetime.now()
                if self.DEBUG:
                    print "Start DateTime: {0}".format(start_datetime)
                    print "24 Hours: {0}".format(datetime.timedelta(hours=24))
                    print "Start DateTime + 24 Hours: {0}".format(startdatetime + datetime.timedelta(hours=24))
                    print "Now: {0}".format(now)
                if self.urls[i]['finish_datetime'] == "":
                    if self.DEBUG:
                        print "The URL has not finished yet, waiting ..."
                    if now >= start_datetime + datetime.timedelta(hours=24):
                        if self.DEBUG:
                            print "WARNING! The scraper has been out scraping for over 24 hours.  Returning URL to pool."
                        # the scraper has been running for over a day, rerun it
                        url_index = i
                        break
                else:
                    freq = self.urls[i]['frequency']
                    finish_datetime = datetime.datetime.strptime(self.urls[i]['finishdatetime'],"%Y-%m-%d %H:%M:%S")
                    #finishdatetime = self.urls[i]['finishdatetime']
                    if now >= finish_datetime + datetime.timedelta(minutes=int(freq)):
                        # it's been over the frequcy, we need to run again
                        url_index = i
                        break
        return url_index
             
    def start(self):
        """
        start() starts the Dispatcher message bus consumer.  This allows the Dispatcher to being listening for 
        scrapers that come online.
        """

        #self.urls = self._geturls()
        #self.broadcasturls()
        #self.urlindex = len(self.urls)-1
        
        if self.DEBUG:
            print "Listening for messages on Message Bus ..."
        
        #self.reqchan.start_consuming()

        self.bus_access.listen()

    def broadcast_status(self):

        packet = {
            'urls': self.urls,
            'current_url_index': self.current_url_index,
            'scrapers': self.scrapers,
            'status_datetime': str(datetime.datetime.now()), 
        }
        #payload = {
        #    'command': 'dispatcher_status',
        #    'source_id': self.uid,
        #    'destination_id': 'broadcast',
        #    'message': packet,
        #}
        #jbody = json.dumps(payload)
        #self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

        self.bus_access.send_message(
            command = 'dispatcher_status',
            destination_id = 'broadcast',
            message = packet,
        )

        if self.exiting == False:
            threading.Timer(self.interval, self.broadcast_status).start()

    def send_url(self,url_index,destination_id):
        """
        sendurl() dispatches a URL to a waiting scraper.  It takes in the urlindex which points to a
        url within the self.urls list, as well as a destination ID of the scraper to dispatch the
        url to.
        """

        packet = self.urls[url_index]

        #payload = {
        #    'command': 'url_dispatch',
        #    'source_id': self.uid,
        #    'destination_id': destination_id,
        #    'message': packet
        #}
        #jbody = json.dumps(payload)
        #self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

        self.bus_access.send_message(
            command = 'url_dispatch',
            destination_id = destination_id,
            message = packet,
        )

    def get_remaining_url_count(self):
        """
        getremainingurlcount() - returns the remaining number of urls to be sent.  this
                                 is only used when the dispatched is in queue mode.
        """

        # calc remaining urls to be sent
        # len - 1 to represent index correctly (zero based)
        # index - 1 because we are always one ahead of the last dispatched
        remaining = (len(self.urls)-1) - (self.current_url_index-1) 

        return remaining

    def stop(self):
        self.exiting = True
        #self.reqchan.stop_consuming()
        self.bus_access.stop_listening()

    # message handler
    def _reqcallback(self,payload): #ch,method,properties,body):
        
        response = payload #json.loads(body)
        
        #print "Processing Message:\n\t{0}".format(response)

        if response['command'] == 'set_dispatcher_urls':
            if self.DEBUG:
                print "Seen Set URLs command"
            self.set_urls(response['message']['urls'])

        if response['command'] == 'scraper_finished':
            if self.DEBUG:
                print "Seen Scraper Finished Command."
            for i in range(0,len(self.urls)):
                target_url = response['message']['url_data']['target_url']
                source_id = response['source_id']
                if self.DEBUG:
                    print self.urls[i]
                    print "Comparing targeturl: {0} to {1}, sourceid: {2} to {3}".format(target_url,
                                                                                         self.urls[i]['target_url'],
                                                                                         source_id,
                                                                                         self.urls[i]['scraper_id']
                    )
                now = str(datetime.datetime.now())
                if self.urls[i]['target_url'] == target_url and self.urls[i]['scraper_id'] == source_id:
                    self.urls[i]['finish_datetime'] = now
                    if self.DEBUG:
                        print "Scraper Announced URL Finish."

        if response['command'] == 'scraper_available':
            
            #
            # TODO: update this for new method of loading URLs
            #

            if self.DEBUG:
                print "Processing URL Request ..."

            if self.self_dispatch == True:
                url_index = self.get_next_url_index()
            else:
                url_index = -1
                if self.DEBUG:
                    print "Number of URLS: {0}".format(len(self.urls))
                    print "Current URL Index: {0}".format(self.current_url_index)
                    print "getremainingurlcount(): {0}".format(self.get_remaining_url_count())
                if len(self.urls) != 0 and self.current_url_index <= len(self.urls)-1:
                    # there are still urls to be sent, get the index of the next one
                    url_index = self.current_url_index
                    self.current_url_index+=1
                    if self.DEBUG:
                        print "URL Found for Dispatch, urlindex: {0}".format(url_index)

            if not url_index == -1:
                self.urls[url_index]['start_datetime'] = str(datetime.datetime.now())
                self.urls[url_index]['scraper_id'] = response['source_id']
                self.urls[url_index]['status'] = 'running'

                if self.DEBUG:
                    print "URL request seen, sending next URL."

                self.send_url(url_index,response['source_id'])
            else:
                if self.DEBUG:
                    print "URL request seen, no URLs to send."

            #if self.urlindex < 0:
            #    self.urls = self._geturls()
            #    self.broadcasturls()
            #    self.urlindex = len(self.urls)-1
            #if self.urlindex >= 0:
            #    self.sendurl(self.urls[self.urlindex],response['sourceid'])
            #    self.urlindex -= 1
            #    print "URL dispatched to '{0}'".format(response['sourceid'])
            #else:
            #    print "No URLs available for dispatch, ignoring request."
        if response['command'] == 'global_shutdown':
            if self.DEBUG:
                print "global_shutdown command seen, exiting."
            #raise Exception("Dispatcher Exiting.")
            self.stop()

if __name__ == '__main__':

    print "BarkingOwl Dispatcher Starting."

    dispatcher = Dispatcher(
        address='localhost',
        exchange='barkingowl',
        self_dispatch=False,
        DEBUG=True
    )
    
    url = {
           'target_url': "http://timduffy.me/",
           'title': "TimDuffy.Me",
           'description': "Tim Duffy's Personal Website",
           'max_link_level': -1,
           'creation_datetime': str(strftime("%Y-%m-%d %H:%M:%S")),
           'doc_type': 'application/pdf',
           'frequency': 2,
           'allowed_domains': [],
          }
    
    #url = {
    #    'targeturl': "http://www.scottsvilleny.org/",
    #    'title': "Village of Scottsville",
    #    'description': "Village of Scottsville, NY Website",
    #    'maxlinklevel': 3,
    #    'creationdatetime': str(strftime("%Y-%m-%d %H:%M:%S")),
    #    'doctype': 'application/pdf',
    #    'frequency': 1,
    #    'allowdomains': [],
    #}

    urls = []
    urls.append(url)

    dispatcher.set_urls(urls)

    if True:
    #try:
        dispatcher.start()
    #except:
    #    pass

    print "BarkingOwl Dispatcher Exiting."
