import pika
import simplejson
import uuid
from time import strftime
import time
import threading

from scraper import Scraper

class ScraperWrapper(threading.Thread):

    def __init__(self,address='localhost',exchange='barkingowl'):

        threading.Thread.__init__(self)

        self.uid = str(uuid.uuid4())

        self.address = address
        self.exchange = exchange

        self.scraper = Scraper(self.uid)

        #setup message bus
        self.respcon = pika.BlockingConnection(pika.ConnectionParameters(
                                                           host=self.address))
        self.respchan = self.respcon.channel()
        self.respchan.exchange_declare(exchange=self.exchange,type='fanout')

        self.reqcon = pika.BlockingConnection(pika.ConnectionParameters(host=address))
        self.reqchan = self.reqcon.channel()
        self.reqchan.exchange_declare(exchange=exchange,type='fanout')
        result = self.reqchan.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.reqchan.queue_bind(exchange=exchange,queue=queue_name)
        self.reqchan.basic_consume(self.reqcallback,queue=queue_name,no_ack=True)


        print "Scraper Wrapper INIT complete."

    def run(self):
        #print "Listening for messages on Message Bus ..."
        self.broadcastavailable()
        time.sleep(1)
        self.reqchan.start_consuming()
        time.sleep(1)      

    def broadcastavailable(self):
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'availabledatetime': str(isodatetime)
        }
        payload = {
            'command': 'scraper_available',
            'sourceid': self.uid,
            'destinationid': 'broadcast',
            'message': packet
        }
        jbody = simplejson.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    def broadcaststatus(self):
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'status': self.scraper.status,
            'urldata': self.status['urldata'],
            'statusdatetime': str(isodatetime)
        }
        payload = {
            'command': 'scraper_status',
            'sourceid': self.uid,
            'destinationid': 'broadcast',
            'message': packet
        }
        jbody = simplejson.dumps(payload)
        time.sleep(.5)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    def broadcastsimplestatus(self):
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")

        if self.scraper.status['urldata'] == {}:
            targeturl = 'null'
        else:
            targeturl = self.scraper.status['urldata']['targeturl']

        packet = {
            #'status': self.scraper.status,
            #'urldata': self.status['urldata'],
            'busy': self.scraper.status['busy'],
            'linkcount': self.scraper.status['linkcount'],
            'processedlinkcount': len(self.scraper.status['processed']),
            'badlinkcount': len(self.scraper.status['badlinks']),
            'targeturl': targeturl,
            'statusdatetime': str(isodatetime)
        }
        payload = {
            'command': 'scraper_status_simple',
            'sourceid': self.uid,
            'destinationid': 'broadcast',
            'message': packet
        }
        jbody = simplejson.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    # message handler
    def reqcallback(self,ch,method,properties,body):
        #try:
            response = simplejson.loads(body)
            if response['sourceid'] == self.uid:
                return
            #print "Processing Message:\n\t{0}".format(response['command'])
            if response['command'] == 'url_dispatch':
                if response['destinationid'] == self.uid:
                    #print "Launching Scraper on new URL."
                    #print response
                    self.scraper.seturldata(response['message'])
                    self.scraper.start()

            elif response['command'] == 'get_status':
                #print "Responding to Status Request."
                self.broadcaststatus()

            elif response['command'] == 'get_status_simple':
                #print "Responding to Simple Status Request."
                self.broadcastsimplestatus()

            elif response['command'] == 'shutdown':
                if response['destinationid'] == self.uid:
                    #print "Shutting Down."
                    # stop consuming messages, so ScraperWrapper can exit
                    self.reqchan.stop_consuming()
                    print "Trying to kill scraper ..."
                    self.scraper.stop()
            elif response['command'] == 'global_shutdown':
                self.reqchan.stop_consuming()
                print "Trying to kill scraper ..."
                self.scraper.stop()

        #except:
        #    print "Message Error"

