import pika
import simplejson
import uuid
from time import strftime
import time

import dateutil.parser
import datetime

#from models import *

class Dispatcher():

    def __init__(self,address='localhost',exchange='barkingowl'):
        # create our uuid
        self.uid = str(uuid.uuid4())

        self.address = address
        self.exchange = exchange

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
        self.reqchan.basic_consume(self.reqcallback,queue=queue_name,no_ack=True)
       
        self.respcon = pika.BlockingConnection(pika.ConnectionParameters(host=self.address))
        self.respchan = self.respcon.channel()
        self.respchan.exchange_declare(exchange=self.exchange,type='fanout')

    def _geturls(self):
        urls = Urls()
        allurls = urls.getallurldata()
        return allurls

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
        urlindex = -1
        for i in range(0,len(self.urls)):
            now = datetime.datetime.strptime(strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S") # gross ...
            # check to see if it ever ran
            if self.urls[i]['startdatetime'] == "":
                print "URL has not run yet: '{0}'".format(self.urls[i]['targeturl'])
                urlindex = i
                break
            else:
                startdatetime = datetime.datetime.strptime(self.urls[i]['startdatetime'],"%Y-%m-%d %H:%M:%S")
                print "Start DateTime: {0}".format(startdatetime)
                print "24 Hours: {0}".format(datetime.timedelta(hours=24))
                print "Start DateTime + 24 Hours: {0}".format(startdatetime + datetime.timedelta(hours=24))
                print "Now: {0}".format(now)
                if self.urls[i]['finishdatetime'] == "":
                    print "The URL has not finished yet, waiting ..."
                    if now >= startdatetime + datetime.timedelta(hours=24):
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
        #self.urls = self._geturls()
        #self.broadcasturls()
        #self.urlindex = len(self.urls)-1
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
        jbody = simplejson.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    # message handler
    def reqcallback(self,ch,method,properties,body):
        response = simplejson.loads(body)
        #print "Processing Message:\n\t{0}".format(response)
        if response['command'] == 'scraper_finished':
            print "Seen Scraper Finished Command."
            for i in range(0,len(self.urls)):
                targeturl = response['message']['urldata']['targeturl']
                sourceid = response['sourceid']
                print "Comparing targeturl: {0} to {1}, sourceid: {2} to {3}".format(targeturl,self.urls[i]['targeturl'],sourceid,self.urls[i]['scraperid'])
                now = datetime.datetime.strptime(strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S") # gross ...
                if self.urls[i]['targeturl'] == targeturl and self.urls[i]['scraperid'] == sourceid:
                    self.urls[i]['finishdatetime'] = now
                    print "Scraper Announced URL Finish."
        if response['command'] == 'scraper_available':
            
            #
            # TODO: update this for new method of loading URLs
            #

            print "URL Request ..."

            urlindex = self.getnexturlindex()

            print "urlindex = {0}".format(urlindex)

            if not urlindex == -1:
                self.urls[urlindex]['startdatetime'] = str(strftime("%Y-%m-%d %H:%M:%S"))
                self.urls[urlindex]['scraperid'] = response['sourceid']
                self.urls[urlindex]['status'] = 'running'

                print "URL request seen, sending next URL."

                self.sendurl(urlindex,response['sourceid'])
            else:
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
    dispatcher = Dispatcher(address='localhost',exchange='barkingowl')
    
    url = {'targeturl': "http://timduffy.me/",
           'title': "TimDuffy.Me",
           'description': "Tim Duffy's Personal Website",
           'maxlinklevel': 3,
           'creationdatetime': str(strftime("%Y-%m-%d %H:%M:%S")),
           'doctype': 'application/pdf',
           'frequency': 2,
          }
    urls = []
    urls.append(url)

    dispatcher.seturls(urls)

    dispatcher.start()
    print "BarkingOwl Dispatcher Exiting."
