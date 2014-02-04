import pika
import simplejson
import uuid
from time import strftime
import time
import threading

from scraper import Scraper

class ScraperWrapper(threading.Thread):

    def __init__(self,address='localhost',exchange='barkingowl',DEBUG=False):

        threading.Thread.__init__(self)

        self.uid = str(uuid.uuid4())

        self.address = address
        self.exchange = exchange
        self.DEBUG=DEBUG

        self.interval = 1

        self.scraper = Scraper(self.uid,address=self.address,exchange=self.exchange,DEBUG=DEBUG)
        self.scraping = False

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
        #threading.Timer(self.interval, self.broadcastavailable).start()

        # setup call backs
        self.scraper.setFinishedCallback(self.scraperFinishedCallback)
        self.scraper.setStartedCallback(self.scraperStartedCallback)
        self.scraper.setBroadcastDocCallback(self.scraperBroadcastDocCallback)

        # broadcast availability
        self.broadcastavailable()
        self.reqchan.start_consuming()

    def stop(self):
        self.scraper.stop()
        self.reqchan.stop_consuming()

    def broadcastavailable(self):
        if self.scraper.status['busy'] == True:
            return

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
        if self.scraper.stopped():
            raise Exception("Scraper Wrapper Exiting")
        else:
            threading.Timer(self.interval, self.broadcastavailable).start()
        
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

    def scraperFinishedCallback(self,payload):
        jbody = simplejson.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)
        return

    def scraperStartedCallback(self,payload):
        jbody = simplejson.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)
        return

    def scraperBroadcastDocCallback(self,payload):
        jbody = simplejson.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)
        return

    # message handler
    def reqcallback(self,ch,method,properties,body):
        #try:
            response = simplejson.loads(body)
            #if response['sourceid'] == self.uid:
            #    return
            #print "Processing Message:\n\t{0}".format(response['command'])
            if response['command'] == 'url_dispatch':
                if response['destinationid'] == self.uid:
                    #print "URL Dispatch Command Seen."
                    #print response
                    if self.scraping == False:
                        #print "[Wrapper] Launching Scraper on URL: '{0}'".format(response['message']['targeturl'])
                        self.scraper.seturldata(response['message'])
                        if self.scraper.started == False:
                            self.scraper.start()
                        self.scraper.begin()
                        self.scraping = True

            elif response['command'] == 'scraper_finished':
                #print "Seen Scraper Finished Command."
                if response['sourceid'] == self.scraper.uid:
                    self.scraping = False

            elif response['command'] == 'get_status':
                #print "Responding to Status Request."
                self.broadcaststatus()

            elif response['command'] == 'get_status_simple':
                #print "Responding to Simple Status Request."
                self.broadcastsimplestatus()

            elif response['command'] == 'shutdown':
                #print "ScraperWapper: shutdown seen."
                if response['destinationid'] == self.uid:
                    print "[{0}] Shutting Down Recieved".format(self.uid)
                    # stop consuming messages, so ScraperWrapper can exit
                    #self.reqchan.stop_consuming()
                    #self.scraper.stop()
                    self.stop()

            elif response['command'] == 'global_shutdown':
                print "Global Shutdown Recieved"
                #self.reqchan.stop_consuming()
                #self.scraper.stop()
                self.stop()

        #except:
        #    print "Message Error"

