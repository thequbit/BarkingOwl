import pika
import json
import uuid
from time import strftime
import time

import dateutil.parser
import datetime

#from models import *

class Dispatcher():

    def __init__(self,address='localhost',exchange='barkingowl',uuid=str(uuid.uuid4()),DEBUG=False):
        """
        __init__() constructor will setup message bus as well as status variables.
        """ 

        # create our uuid
        self.uid = uuid

        self.address = address
        self.exchange = exchange
        self.DEBUG = DEBUG

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
        
            url = {'targeturl': targeturl, # the root url to be scraped
                   'title': title, # a title for the URL
                   'description': descritpion, # a description of the url
                   'maxlinklevel': maxlinklevel, # the max link level for the scraper to follow to
                   'creationdatetime': creationdatetime, # ISO creation date and time
                   'doctype': doctype, # the text for the magic lib to look for (ex. 'application/pdf')
                   'frequency': frequency, # the frequency in minutes the URL should be scraped
                   'allowdomains': [], # a list of allowable domains for the scraper to follow
                  }
        
        note: every time this function is called, the url list is reset, including the last
        time any scraper ran agaist a url.
        """

        # setup urls for future data
        for i in range(0,len(urls)):
            urls[i]['startdatetime'] = ""
            urls[i]['finishdatetime'] = ""
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
                    print "URL has not run yet: '{0}'".format(self.urls[i]['targeturl'])
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
                    finishdatetime = self.urls[i]['finishdatetime']
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

    def sendurl(self,urlindex,destinationid):
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
            'sourceid': self.uid,
            'destinationid': destinationid,
            'message': packet
        }
        jbody = json.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    # message handler
    def _reqcallback(self,ch,method,properties,body):
        response = json.loads(body)
        #print "Processing Message:\n\t{0}".format(response)
        if response['command'] == 'scraper_finished':
            if self.DEBUG:
                print "Seen Scraper Finished Command."
            for i in range(0,len(self.urls)):
                targeturl = response['message']['urldata']['targeturl']
                sourceid = response['sourceid']
                if self.DEBUG:
                    print urls[i]
                    print "Comparing targeturl: {0} to {1}, sourceid: {2} to {3}".format(targeturl,
                                                                                         self.urls[i]['targeturl'],
                                                                                         sourceid,
                                                                                         self.urls[i]['scraperid']
                    )
                now = datetime.datetime.strptime(strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S") # gross ...
                if self.urls[i]['targeturl'] == targeturl and self.urls[i]['scraperid'] == sourceid:
                    self.urls[i]['finishdatetime'] = now
                    if self.DEBUG:
                        print "Scraper Announced URL Finish."
        if response['command'] == 'scraper_available':
            
            #
            # TODO: update this for new method of loading URLs
            #

            if self.DEBUG:
                print "Processing URL Request ..."

            urlindex = self.getnexturlindex()

            if not urlindex == -1:
                self.urls[urlindex]['startdatetime'] = str(strftime("%Y-%m-%d %H:%M:%S"))
                self.urls[urlindex]['scraperid'] = response['sourceid']
                self.urls[urlindex]['status'] = 'running'

                if self.DEBUG:
                    print "URL request seen, sending next URL."

                self.sendurl(urlindex,response['sourceid'])
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
            raise Exception("Dispatcher Exiting.")

if __name__ == '__main__':

    print "BarkingOwl Dispatcher Starting."

    dispatcher = Dispatcher(address='localhost',exchange='barkingowl', DEBUG=True)
    
    #url = {'targeturl': "http://timduffy.me/",
    #       'title': "TimDuffy.Me",
    #       'description': "Tim Duffy's Personal Website",
    #       'maxlinklevel': 3,
    #       'creationdatetime': str(strftime("%Y-%m-%d %H:%M:%S")),
    #       'doctype': 'application/pdf',
    #       'frequency': 2,
    #      }
    
    url = {
        'targeturl': "http://www.cityofrochester.gov/",
        'title': "City of Rochester",
        'description': "City of Rochester, NY Website",
        'maxlinklevel': 3,
        'creationdatetime': str(strftime("%Y-%m-%d %H:%M:%S")),
        'doctype': 'application/pdf',
        'frequency': 1,
    }

    urls = []
    urls.append(url)

    dispatcher.seturls(urls)

    try:
        dispatcher.start()
    except:
        pass

    print "BarkingOwl Dispatcher Exiting."
