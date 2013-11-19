import pika
import simplejson
import uuid
from time import strftime

from scraper import Scraper

class ScraperWrapper():

    def __init__(self,address='localhost',exchange='barkingowl'):

        self.uid = str(uuid.uuid4())

        self.address = address
        self.exchange = exchange

        self.scraper = Scraper(self.uid)

        #setup message bus
        self.reqcon = pika.BlockingConnection(pika.ConnectionParameters(host=address))
        self.reqchan = self.reqcon.channel()
        self.reqchan.exchange_declare(exchange=exchange,type='fanout')
        result = self.reqchan.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.reqchan.queue_bind(exchange=exchange,queue=queue_name)
        self.reqchan.basic_consume(self.reqcallback,queue=queue_name,no_ack=True)

        self.respcon = pika.BlockingConnection(pika.ConnectionParameters(
		                                           host=self.address))
        self.respchan = self.respcon.channel()
        self.respchan.exchange_declare(exchange=self.exchange,type='fanout')

    def start(self):
        print "Listening for messages on Message Bus ..."
        self.broadcastavailable()
        self.reqchan.start_consuming()
       
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
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    # message handler
    def reqcallback(self,ch,method,properties,body):
        response = simplejson.loads(body)
        print "Processing Message:\n\t{0}".format(response)
        if response['command'] == 'url_dispatch':
            if response['destinationid'] == self.uid:
                print "Launching Scraper on new URL."
                print response
                self.scraper.start(response['message'])

        elif response['command'] == 'scraper_status':
                #
                # TODO: read status from Scraper thread and broadcast to bus
                #
                self.broadcaststats()

        elif response['command'] == 'shutdown':
            if response['destinationid'] == self.uid:
                #
                # TODO: stop Scraper thread
                #

                # stop consuming messages, so ScraperWrapper can exit
                self.reqchan.stop_consuming() 

