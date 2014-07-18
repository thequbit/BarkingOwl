import pika
import json
import uuid
from time import strftime
import time

import datetime

#from models import *

class Dispatcher():

    def __init__(self,address='localhost',exchange='barkingowl',selfdispatch=True,uuid=str(uuid.uuid4()),DEBUG=False):
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

        selfdispatch
            default
                True
            description
                BarkingOwl has two main modes: self dispatch, and queue dispatch.  In self 
                dispatch mode the dispatched is loaded with URLs to dispatch once, and it
                handles when they are dispatched based on frequency values.  In queue mode
                a list of URLs are loaded into the dispatched and they are dispatched, in 
                order, as soon as a scraper anouncement is seen.  Frequency information is
                ignored in queue mode.

        uuid
            default
                str(uuid.uuid4()) [ creates a UUID for you that is statisticly unused ]
            description
                All elements within the BarkingOwl universe must have unique id's.  The 
                default value here creates a uuid for you to use.  If there is no reason
                to have the uuid a specfic string, then use the default.

        DEBUG
            default
                False
            description
                When set to True, lots of debug information will be printed to the screen.
                Not recommended for production.
               
        """ 

        # create our uuid
        self.uid = uuid

        self.address = address
        self.exchange = exchange
        self.DEBUG = DEBUG
        self.selfdispatch = selfdispatch

        self.currenturlindex = 0

        self.urls = []
        self.scrapers = []
        self.runs = []

        #setup message bus
        self.reqcon = pika.BlockingConnection(pika.ConnectionParameters(host=address))
        self.reqchan = self.reqcon.channel()
        self.reqchan.exchange_declare(exchange=exchange,type='fanout')
        result = self.reqchan.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.reqchan.queue_bind(exchange=exchange,queue=queue_name)
        self.reqchan.basic_consume(self._reqcallback,queue=queue_name,no_ack=True)
       
        self.respcon = pika.BlockingConnection(pika.ConnectionParameters(host=self.address))
        self.respchan = self.respcon.channel()
        self.respchan.exchange_declare(exchange=self.exchange,type='fanout')

    def seturls(self,urls):
        """
        seturls() expects an array of dictionaries that hold url data.  The format should
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

        self.urls = urls

    def getnexturlindex(self):
        """
        getnexturlindex() returns the index withint he self.urls list of the url to dispatch to 
        the scrapers.  This function has the logic within it to return the index of the url that
        has not run within the last defined frequency window.
        """

        urlindex = -1
        for i in range(0,len(self.urls)):
            now = datetime.datetime.now() #strptime(strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S") # gross ...
            # check to see if it ever ran
            if self.urls[i]['startdatetime'] == "":
                if self.DEBUG:
                    print "URL has not run yet: '{0}'".format(self.urls[i]['target_url'])
                urlindex = i
                break
            else:
                startdatetime = datetime.datetime.strptime(self.urls[i]['startdatetime'],"%Y-%m-%d %H:%M:%S")
                if self.DEBUG:
                    print "Start DateTime: {0}".format(startdatetime)
                    print "24 Hours: {0}".format(datetime.timedelta(hours=24))
                    print "Start DateTime + 24 Hours: {0}".format(startdatetime + datetime.timedelta(hours=24))
                    print "Now: {0}".format(now)
                if self.urls[i]['finishdatetime'] == "":
                    if self.DEBUG:
                        print "The URL has not finished yet, waiting ..."
                    if now >= startdatetime + datetime.timedelta(hours=24):
                        if self.DEBUG:
                            print "WARNING! The scraper has been out scraper for over 24 hours.  Returning URL to pool."
                        # the scraper has been running for over a day, rerun it
                        urlindex = i
                        break
                else:
                    freq = self.urls[i]['frequency']
                    finishdatetime = datetime.datetime.strptime(self.urls[i]['finishdatetime'],"%Y-%m-%d %H:%M:%S")
                    #finishdatetime = self.urls[i]['finishdatetime']
                    if now >= finishdatetime + datetime.timedelta(minutes=int(freq)):
                        # it's been over the frequcy, we need to run again
                        urlindex = i
                        break
        return urlindex
             
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
        self.reqchan.start_consuming()

    #def clearurl(self,url):
    #   urlid,targeturl,maxlinklevel,creationdatetime,doctypetitle,docdescription,doctype = url
    #   creationdatetime = str(creationdatetime)
    #   newurl = (urlid,targeturl,maxlinklevel,creationdatetime,doctypetitle,docdescription,doctype)
    #   return newurl

    #def broadcasturls(self):
    #    theurls = []
    #    for url in self.urls:
    #        urlid,targeturl,maxlinklevel,creationdatetime,doctypetitle,docdescription,doctype = url
    #        creationdatetime = str(creationdatetime)
    #        newurl = (urlid,targeturl,maxlinklevel,creationdatetime,doctypetitle,docdescription,doctype)
    #        theurls.append(newurl)
    #
    #    packet = {
    #        'urls': theurls,
    #    }
    #    payload = {
    #        'command': 'dispatcher_urls',
    #        'sourceid': self.uid,
    #        'destinationid': 'broadcast',
    #        'message': packet,
    #    }
    #    jbody = simplejson.dumps(payload)
    #    self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    def sendurl(self,urlindex,destination_id):
        """
        sendurl() dispatches a URL to a waiting scraper.  It takes in the urlindex which points to a
        url within the self.urls list, as well as a destination ID of the scraper to dispatch the
        url to.
        """

        #targeturl = self.urls[urlindex]['targeturl']
        #title = self.urls[urlindex]['title']
        #description = self.urls[urlindex]['description']
        #maxlinklevel = self.urls[urlindex]['maxlinklevel']
        #doctype = self.urls[urlindex]['doctype']
        #isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        
        #packet = {
        #    #'urlid': urlid,
        #    'targeturl': targeturl,
        #    'maxlinklevel': maxlinklevel,
        #    'title': title,
        #    'description': description,
        #    #'creationdatetime': str(creationdatetime),
        #    #'doctypetitle': doctypetitle,
        #    #'docdescription': docdescription,
        #    'doctype': doctype,
        #    'dispatchdatetime': str(isodatetime),
        #}

        packet = self.urls[urlindex]

        #print "\n{0}\n".format(packet)
        payload = {
            'command': 'url_dispatch',
            'source_id': self.uid,
            'destination_id': destination_id,
            'message': packet
        }
        jbody = json.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    def getremainingurlcount(self):
        """
        getremainingurlcount() - returns the remaining number of urls to be sent.  this
                                 is only used when the dispatched is in queue mode.
        """

        # calc remaining urls to be sent
        # len - 1 to represent index correctly (zero based)
        # index - 1 because we are always one ahead of the last dispatched
        remaining = (len(self.urls)-1) - (self.currenturlindex-1) 

        return remaining

    def stop(self):
        self.reqchan.stop_consuming()
        

    # message handler
    def _reqcallback(self,ch,method,properties,body):
        response = json.loads(body)
        #print "Processing Message:\n\t{0}".format(response)
        if response['command'] == 'scraper_finished':
            if self.DEBUG:
                print "Seen Scraper Finished Command."
            for i in range(0,len(self.urls)):
                targeturl = response['message']['url_data']['target_url']
                sourceid = response['source_id']
                if self.DEBUG:
                    print self.urls[i]
                    print "Comparing targeturl: {0} to {1}, sourceid: {2} to {3}".format(targeturl,
                                                                                         self.urls[i]['target_url'],
                                                                                         sourceid,
                                                                                         self.urls[i]['scraper_id']
                    )
                now = str(strftime("%Y-%m-%d %H:%M:%S"))
                if self.urls[i]['target_url'] == targeturl and self.urls[i]['scraper_id'] == sourceid:
                    self.urls[i]['finishdatetime'] = now
                    if self.DEBUG:
                        print "Scraper Announced URL Finish."
        if response['command'] == 'scraper_available':
            
            #
            # TODO: update this for new method of loading URLs
            #

            if self.DEBUG:
                print "Processing URL Request ..."

            if self.selfdispatch == True:
                urlindex = self.getnexturlindex()
            else:
                urlindex = -1
                if self.DEBUG:
                    print "Number of URLS: {0}".format(len(self.urls))
                    print "Current URL Index: {0}".format(self.currenturlindex)
                    print "getremainingurlcount(): {0}".format(self.getremainingurlcount())
                if len(self.urls) != 0 and self.currenturlindex <= len(self.urls)-1:
                    # there are still urls to be sent, get the index of the next one
                    urlindex = self.currenturlindex
                    self.currenturlindex+=1
                    if self.DEBUG:
                        print "URL Found for Dispatch, urlindex: {0}".format(urlindex)

            if not urlindex == -1:
                self.urls[urlindex]['start_datetime'] = str(strftime("%Y-%m-%d %H:%M:%S"))
                self.urls[urlindex]['scraper_id'] = response['source_id']
                self.urls[urlindex]['status'] = 'running'

                if self.DEBUG:
                    print "URL request seen, sending next URL."

                self.sendurl(urlindex,response['source_id'])
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

    dispatcher = Dispatcher(address='localhost',exchange='barkingowl',selfdispatch=False,DEBUG=True)
    
    url = {'target_url': "http://timduffy.me/",
           'title': "TimDuffy.Me",
           'description': "Tim Duffy's Personal Website",
           'max_link_level': 3,
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

    dispatcher.seturls(urls)

    if True:
    #try:
        dispatcher.start()
    #except:
    #    pass

    print "BarkingOwl Dispatcher Exiting."
