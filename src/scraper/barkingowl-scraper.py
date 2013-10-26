import threading
import sched,time

from datetime import datetime

import simplejson

from scraper import Scraper

import pika

class Wrapper():

    verbose = True
    _scraper = None

    def __init__(self,address='localhost'):

        self.r("Starting Barking Owl Scraper Wrapper ...")

        self._addr = address
        self._queuename = 'barkingowl'
        
        # setup incomming message
        self._reqcon = pika.BlockingConnection(pika.ConnectionParameters(
                                                 host=self._addr))
        self._reqchan = self._reqcon.channel()
        self._reqchan.exchange_declare(exchange='barkingowl',
                              type='fanout')
        result = self._reqchan.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self._reqchan.queue_bind(exchange='barkingowl',
                                 queue=queue_name)
        self._reqchan.basic_consume(self._reqcallback,
                                    queue=queue_name,
                                    no_ack=True,
                                   )

        # setup outgoing messages
        self._respcon = pika.BlockingConnection(pika.ConnectionParameters(
                                                  host=self._addr))
        self._respchan = self._respcon.channel()
        self._respchan.exchange_declare(exchange='barkingowl',
                                          type='fanout')

        self.r("Wrapper Started Successfully.")

 
    def r(self,text):
        if self.verbose:
            if self._scraper == None:
                id = ""
            else:
                id = self._scraper.scraperid
            print "[{0}][Wrapper {1}] {2}".format(datetime.now().strftime("%Y%m%d %H:%M:%S"),id,text)

    def start(self,download_directory):
        self.r("Starting the Scraper ...")
        self._scraper = Scraper(download_directory)
        self._scraper.start()
        #self._scraper.join()
        self.r("Scraper Started.")

        self._reqchan.start_consuming()

        self.s = sched.scheduler(time.time, time.sleep)
        self.s.enter(5, 1, self.tick, ())
        self.s.run()

        #self._reqchan.start_consuming()

    def tick(self):
        self.r("WD Tick")
        self.s.enter(5,1,self.tick, ())
        self._respchan.basic_publish(exchange=self._queuename,
                                     routing_key='', #self._queuename,
                                     body='{"command": "wd_tick"}',
                                    )


    def _reqcallback(self,ch,method,properties,body):
        self.r("Processing New Message ...")
        try:
            response = simplejson.loads(body)
            #self.r("Message Body: {0}".format(response))
            if response['command'] == 'request_status': # and response['scraperid'] == self._scraper.scraperid:
                self._busy = True
                self.r("Generating STATUS payload ...")
  
                payload = {
                    'scraperid': self._scraper.scraperid,
                    'linkcount': self._scraper._linkcount,
                    'destdir':   self._scraper.destdir,
                    'address':   self._scraper._addr,
                    'processed': self._scraper._processed,
                    'badlinks':  self._scraper._badlinks,
                    'queuename': self._scraper._queuename,
                    'busy':      self._scraper._busy,
                    'level':     self._scraper._level,
                }

                body = {
                    'command': 'scraper_status',
                    'scraperid': self._scraper.scraperid,
                    'payload': simplejson.dumps(payload)
                }

                jbody = simplejson.dumps(body)

                self._respchan.basic_publish(exchange=self._queuename,
                                             routing_key='', #self._queuename,
                                             body=jbody,
                                             )

                self.r("Payload processed successfully.")
                self._busy = False 

        except Exception, e:
            self.r("ERROR: an error happened while processing the message:\n\t{0}".format(e))
        return True

def main(): 
    
    download_directory = "./downloads"
    #scraper = Scraper(download_directory)
    #scraper.start()
    wrapper = Wrapper()
    wrapper.start(download_directory)

main()

