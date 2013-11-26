import pika
import simplejson
import uuid
from time import strftime

from models import *

class Dispatcher():

    def __init__(self,address='localhost',exchange='barkingowl'):
        # create our uuid
        self.uid = str(uuid.uuid4())

        self.address = address
        self.exchange = exchange

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

    def geturls(self):
        urls = Urls()
        allurls = urls.getallurldata()
        return allurls

    def start(self):
        self.urls = self.geturls()
        self.urlindex = len(self.urls)-1
        print "Listening for messages on Message Bus ..."
        self.reqchan.start_consuming()

    def sendurl(self,url,destinationid):
        urlid,targeturl,maxlinklevel,creationdatetime,doctypetitle,docdescription,doctype = url
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'urlid': urlid,
            'targeturl': targeturl,
            'maxlinklevel': maxlinklevel,
            'creationdatetime': str(creationdatetime),
            'doctypetitle': doctypetitle,
            'docdescription': docdescription,
            'doctype': doctype,
            'disparchdatetime': str(isodatetime),
        }
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
        if response['command'] == 'scraper_available':
            if self.urlindex < 0:
                self.urls = self.geturls()
                self.urlindex = len(self.urls)-1
            if self.urlindex >= 0:
                self.sendurl(self.urls[self.urlindex],response['sourceid'])
                #self.urlindex -= 1
                print "URL dispatched to '{0}'".format(response['sourceid'])
            else:
                print "No URLs available for dispatch, ignoring request."

def main():
    print "BarkingOwl Dispatcher Starting."
    dispatcher = Dispatcher(address='localhost',exchange='barkingowl')
    dispatcher.start()
    print "BarkingOwl Dispatcher Exiting."

main()    
