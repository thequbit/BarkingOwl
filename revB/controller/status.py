import pika
import simplejson
import threading
import uuid
import time
from time import strftime

class Status(threading.Thread):

    def __init__(self,exchange='barkingowl',address='localhost'):

        #print "Starting Status Monitor ..."

        threading.Thread.__init__(self)

        self.exchange = exchange
        self.address = address

        self.uid = str(uuid.uuid4())

        self.status = {}

        # how often we send a request for status
        self.interval = 1

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

        #print "Monitor Started."

    def sendrequest(self):
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'requestiondatetime': str(isodatetime)
        }
        payload = {
            'command': 'get_status_simple',
            'sourceid': self.uid,
            'destinationid': 'broadcast',
            'message': packet
        }
        jbody = simplejson.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)
        threading.Timer(self.interval, self.sendrequest).start()

    def run(self):
        #print "Starting Status() thread."
        threading.Timer(self.interval, self.sendrequest).start()
        self.reqchan.start_consuming()

    def stop(self):
        self.reqchan.stop_consuming()

    def reqcallback(self,ch,method,properties,body):
        response = simplejson.loads(body)
        if response['command'] == 'scraper_status_simple':
            self.status[response['sourceid']] = response['message']
        if response['command'] == 'scraper_shutdown':
            self.status.pop(request['sourceid'],None)
