import pika
import Queue
import logging
import simplejson
import datetime
import time

#logging.getLogger('pika').setLevel(logging.ERROR)

from models import *

class Dispatcher():

    def __init__(self,address='localhost'):

        self._addr = address
        self._queuename = 'barkingowl'
        self._urlqueue = Queue.Queue(0)
        
        # setup incomming messages
        #self._reqcon = pika.BlockingConnection(pika.ConnectionParameters(
        #                                         host=self._addr))
        #self._reqchan = self._reqcon.channel()
        #self._reqchan.queue_declare(queue=self._queuename)
        #self._reqchan.basic_consume(self._reqcallback,
        #                            queue=self._queuename,
        #                            no_ack=True)
        self._reqcon = pika.BlockingConnection(pika.ConnectionParameters(
                                                 host=self._addr))
        self._reqchan = self._reqcon.channel()
        self._reqchan.exchange_declare(exchange='s2d_barkingowl',
                              type='fanout')
        result = self._reqchan.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self._reqchan.queue_bind(exchange='s2d_barkingowl',
                                 queue=queue_name)
        self._reqchan.basic_consume(self._reqcallback,
                                    queue=queue_name,
                                    no_ack=True,
                                   )

        # setup outgoing messages
        self._respcon = pika.BlockingConnection(pika.ConnectionParameters(
                                                  host=self._addr))
        self._respchan = self._respcon.channel()
        #self._respchan.queue_declare(queue=self._queuename,durable=True)
        self._respchan.exchange_declare(exchange='d2s_barkingowl',
                                          type='fanout')

    def __del__(self):
        self._reqcon.close()
        self._respcon.close()

    def _report(self,text):
        print "[Dispatcher] {0}".format(text)

    def _getorgs(self,):
        orgs = Orgs()
        allorgs = orgs.getall()
        return allorgs

    def _geturls(self,orgid):
        urls = Urls()
        orgurls = urls.byorgid(orgid)
        #orgurls = []
        #orgurls.append(urls.get(25))
        #orgurls.append(urls.get(10))
        return orgurls

    def _collecturls(self):
        urlcollection = []
        orgs = self._getorgs()
        count = 0
        for org in orgs:
            orgid,orgname,description,creationdatetime,ownerid = org
            orgurls = self._geturls(orgid)
            for orgurl in orgurls:
                self._urlqueue.put((orgname,orgurl))
                count += 1
        return count

    def _sanitizedate(self,url):
        urlid,orgid,url,urlname,description,createdatetime,creationuserid,linklevel = url
        createdatetimeiso = createdatetime.strftime("%Y-%m-%d %H:%M:%S")
        returl = (urlid,orgid,url,urlname,description,createdatetimeiso,creationuserid,linklevel)
        return returl

#    def _fixurldate(self,rawurl):
#        urlid,orgid,url,urlname,description,createdatetimeiso,creationuserid,linklevel = rawurl
#        createdatetime = datetime.datetime.strptime(createdatetimeiso,"%Y-%m-%d %H:%M:%S")
#        returl = (urlid,orgid,url,urlname,description,createdatetime,creationuserid,linklevel)
#        return returl

    def _reqcallback(self,ch,method,properties,body):
        self._report("New Message Recieved ...")
        reponse = ""
        try:
            response = simplejson.loads(body)
            if response['command'] == 'url_request':

                time.sleep(.25)

                # pull the url from the queue
                nexturl = self._urlqueue.get()
                orgname,url = nexturl

                # create the url payload
                payload = {'command': 'url_payload',
                           'scrapperid': response['scrapperid'],
                           'orgname': orgname,
                           'url_json': simplejson.dumps(self._sanitizedate(url))}
                jurl = simplejson.dumps(payload)

                # send the message
                self._respchan.basic_publish(exchange='d2s_barkingowl',
                                             routing_key='', #self._queuename,
                                             body=jurl,
                                             )

                # report to user actions
                self._report("URL request processed:")
                self._report("    Org name:    '{0}'".format(orgname))
                self._report("    URL:         '{0}'".format(url[2]))
                self._report("    Scrapper ID: '{0}'".format(response['scrapperid']))

        except Exception,e:
           self._report("ERROR: bad message format: '{0}'".format(reponse))

    def start(self):
        self._report("Dispatcher Launched.")
        count = self._collecturls()
        self._report("Ready to dispatch {0} URLs for scrapping".format(count))
        self._reqchan.start_consuming()

#def hi(ch,method,properties,body):
#    print "hi."

def main():
    dispatcher = Dispatcher()
    dispatcher.start()

main()

