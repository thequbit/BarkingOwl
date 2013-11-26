import pika
import simplejson
import threading
import uuid
import time
from time import strftime

class RequestStatus(threading.Thread):

    def __init__(self,exchange='barkingowl',address='localhost'):

        self.exchange = exchange
        self.address = address

        self.uid = str(uuid.uuid4())

        print "Loading ..."

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

        print "Done."

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
        print "Status Request Sent."
        threading.Timer(10, self.sendrequest).start()

    def start(self):
        threading.Timer(10, self.sendrequest).start()
        self.reqchan.start_consuming()

    def stop(self):
        self.reqchan.stop_consuming()

    def reqcallback(self,ch,method,properties,body):
        response = simplejson.loads(body)
        if response['command'] == 'scraper_status_simple':
            print "{0}:\n\tBusy: {1}\n\tLink Count: {2}\n\tProcessed Count: {3}\n\tBad Link Count: {4}\n\tTarget URL: {5}\n".format(response['sourceid'],response['message']['busy'],response['message']['linkcount'],response['message']['processedlinkcount'],response['message']['badlinkcount'],response['message']['targeturl'])

def main():
    requeststatus = RequestStatus()
    requeststatus.start()

main()
