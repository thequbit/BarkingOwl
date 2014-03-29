import pika
import json
import uuid
from time import strftime
import time
import threading

from scraper import Scraper

class ScraperWrapper(threading.Thread):

    def __init__(self,address='localhost',exchange='barkingowl',DEBUG=False):
        """
        __init__() constructor setups up the message bus, inits the thread, and sets up 
        local status variables.
        """

        threading.Thread.__init__(self)

        self.uid = str(uuid.uuid4())
        self.address = address
        self.exchange = exchange
        self.DEBUG=DEBUG
        self.interval = 1

        # create scraper instance
        self.scraper = Scraper(uid=self.uid,DEBUG=DEBUG)
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
        self.reqchan.basic_consume(self._reqcallback,queue=queue_name,no_ack=True)

        if self.DEBUG:
            print "Scraper Wrapper INIT complete."

    def run(self):
        """
        run() is called by the threading sub system when ScraperWrapper.start() is called.  This function
        sets up all of the call abcks needed, as well as begins consuming on the message bus. 
        """
        # setup call backs
        self.scraper.setFinishedCallback(self.scraperFinishedCallback)
        self.scraper.setStartedCallback(self.scraperStartedCallback)
        self.scraper.setBroadcastDocCallback(self.scraperBroadcastDocCallback)

        # broadcast availability
        self.broadcastavailable()
        self.reqchan.start_consuming()

    def stop(self):
        """
        stop() is called to stop consuming on the message bus, and to stop the scraper from running.
        """
        self.scraper.stop()
        self.reqchan.stop_consuming()

    def resetscraper(self):
        """
        resetscraper() calls reset() within the Scraper class.  This resets the state of the scraper.
        This should not be called unless the scraper has been stoped.
        """
        self.scraper.reset()

    def broadcastavailable(self):
        """
        broadcastavailable() broadcasts a message to the message bus saying the scraper is available
        to be dispatched a new url to begin scraping.
        """
        if self.scraper.status['busy'] == True:
            # we are currently scraping, so we are not available - don't broadcast
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
        jbody = json.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

        #
        # TODO: move this over to it's own timer, no need to do it here.
        #
        if self.scraper.stopped():
            raise Exception("Scraper Wrapper Exiting")
        else:
            threading.Timer(self.interval, self.broadcastavailable).start()
        
    def broadcaststatus(self):
        """
        broadcaststatus() broadcasts the status of the scraper to the bus.  This includes all of the information
        kept in all of the state variables within the scraper.  Note: this can be a LOT of information.
        """
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
        jbody = json.dumps(payload)
        #time.sleep(.5)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    def broadcastsimplestatus(self):
        """
        broadcastsimplestatus() broadcasts a smaller subset of information about the scraper to the bus.  This
        information includes:

            packet = {
                'busy': self.scraper.status['busy'],                         # boolean of busy status
                'linkcount': self.scraper.status['linkcount'],               # number of links seen by the scraper
                'processedlinkcount': len(self.scraper.status['processed']), # number of links processed by the scraper
                'badlinkcount': len(self.scraper.status['badlinks']),        # number of bad links seen by the scraper
                'targeturl': targeturl,                                      # the target url the scraper is working on
                'statusdatetime': str(isodatetime)                           # the date/time of the status being sent
            }

        """
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")

        if self.scraper.status['urldata'] == {}:
            targeturl = 'null'
        else:
            targeturl = self.scraper.status['urldata']['targeturl']

        packet = {
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
        jbody = json.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    def scraperFinishedCallback(self,payload):
        """
        scraperFinishedCallBack() is the built in, and default, async call back for when the 'scraper finished' command is seen.
        """
        jbody = json.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)
        return

    def scraperStartedCallback(self,payload):
        """
        scraperFinishedCallBack() is the built in, and default, async call back for when the 'scraper started' command is seen.
        """
        jbody = json.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)
        return

    def scraperBroadcastDocCallback(self,payload):
        """
        scraperBroadcastDocCallBack() is the built in, and default, async call back for when the 'scraper finds a new document' command is seen.
        """
        jbody = json.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)
        return

    def _scraperstart(self):
        if self.scraper.start == False:
            self.scraper.start()
        self.scraper.begin()

    # message handler
    def _reqcallback(self,ch,method,properties,body):
        #try:
        if True:
            response = json.loads(body)
            if self.DEBUG:
                print "Processing Message:\n\t{0}".format(response['command'])
            if response['command'] == 'url_dispatch':
                if response['destinationid'] == self.uid:
                    #print "URL Dispatch Command Seen."
                    #print response
                    if self.scraping == False:
                        #print "[Wrapper] Launching Scraper on URL: '{0}'".format(response['message']['targeturl'])
                        self.scraper.seturldata(response['message'])
                        #if self.scraper.started == False:
                        #    self.scraper.start()
                        if self.DEBUG:
                            print "Launching scraper thread ..."
                        thread = threading.Thread(target=self._scraperstart)
                        thread.start()
                        #self._scraperstart()
                        if self.DEBUG:
                            print " ... Scraper launched successfully."
                        self.scraping = True

            elif response['command'] == 'scraper_finished':
                if response['sourceid'] == self.scraper.uid:
                    self.scraping = False

            elif response['command'] == 'get_status':
                self.broadcaststatus()

            elif response['command'] == 'get_status_simple':
                self.broadcastsimplestatus()

            elif response['command'] == 'reset_scraper':
                self.resetscraper()

            elif response['command'] == 'shutdown':
                if response['destinationid'] == self.uid:
                    print "[{0}] Shutting Down Recieved".format(self.uid)
                    self.stop()

            elif response['command'] == 'global_shutdown':
                print "Global Shutdown Recieved"
                self.stop()

        #except:
        #    if self.DEBUG:
        #        print "Message Error"

if __name__ == '__main__':

    print 'Launching BarkingOwl scraper ...'

    scraperwrapper = ScraperWrapper(address='localhost',exchange='barkingowl', DEBUG=True)

    try:
        scraperwrapper.start()
    except:
        print 'exiting.'
