import pika
import simplejson
from time import strftime
import uuid
import sys

class ScraperShutdown():

    def __init__(self,address='localhost',exchange='barkingowl'):

        self.address = address
        self.exchange = exchange
        
        self.uid = str(uuid.uuid4())

        #setup message bus
        self.respcon = pika.BlockingConnection(pika.ConnectionParameters(
                                                           host=self.address))
        self.respchan = self.respcon.channel()
        self.respchan.exchange_declare(exchange=self.exchange,type='fanout')

        #self.reqcon = pika.BlockingConnection(pika.ConnectionParameters(host=address))
        #self.reqchan = self.reqcon.channel()
        #self.reqchan.exchange_declare(exchange=exchange,type='fanout')
        #result = self.reqchan.queue_declare(exclusive=True)
        #queue_name = result.method.queue
        #self.reqchan.queue_bind(exchange=exchange,queue=queue_name)
        #self.reqchan.basic_consume(self.reqcallback,queue=queue_name,no_ack=True)

    def shutdown(self,destinationid):
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {}
        payload = {
            'command': 'shutdown',
            'sourceid': self.uid,
            'destinationid': destinationid,
            'message': packet
        }
        jbody = simplejson.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    #def reqcallback(self,ch,method,properties,body):
        #pass

def main():

    scraperid = ""
    if len(sys.argv) != 2:
        print "Usage:\n\n\tpython scrapershutdown.py <scraperid>\n"
        return
    else:
        scraperid = sys.argv[1]

    print "Trying to shutdown scraper: '{0}'".format(scraperid) 

    ss = ScraperShutdown()
    ss.shutdown(scraperid)

    print "Success!"

    return

main()
