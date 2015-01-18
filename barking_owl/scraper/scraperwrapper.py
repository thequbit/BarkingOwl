import pika
import json
import uuid
from time import strftime
import time
import threading
import datetime

from scraper import Scraper

from barking_owl.busaccess import BusAccess

import logging
logging.basicConfig()

def log(text, DEBUG=False):
    if DEBUG == True:
        print "[{0}] {1}".format(str(datetime.datetime.now()),text)

class ScraperWrapper(threading.Thread):

    def __init__(self,address='localhost',exchange='barkingowl',
            broadcast_interval=5,url_parameters=None, uid=str(uuid.uuid4()), \
            ,DEBUG=False):

        threading.Thread.__init__(self)

        self.uid = str(uuid.uuid4())
        self.address = address
        self.exchange = exchange
        self.interval = broadcast_interval
        self.url_parameters = url_parameters
        self.DEBUG=DEBUG

        self.scraper = Scraper(
            DEBUG = self.DEBUG,
        )
        self.scraping = False
        self.scraper_thread = None

        self.stopped = False

        self.bus_access = BusAccess(
            uid = self.uid,
            address = self.address,
            exchange = self.exchange,
            url_parameters = self.url_parameters,
            DEBUG = self.DEBUG,
        )
        self.bus_access.set_callback(
            callback = self._reqcallback,
        )

        threading.Timer(self.interval, self.broadcast_available).start()
        threading.Timer(self.interval, self.broadcast_simple_status).start()

        #log( "ScraperWrapper.__init__(): Scraper Wrapper INIT complete.", self.DEBUG )

    def run(self):
        self.scraper.set_call_backs(
            start_callback = self.scraper_started_callback,
            finished_callback = self.scraper_finished_callback,
            found_doc_callback = self.scraper_broadcast_document_callback,
            new_url_callback = None,
            bandwidth_limit_callback = None,
            memory_limit_callback = None,
            error_callback = None,
        )
        self.bus_access.listen()

    def stop(self):
        self.bus_access.stop_listening()
        self.scraper.stop()
        self.stopped = True

    def reset_scraper(self):
        self.scraper.reset()

    def broadcast_available(self):
        if self.scraper._data['working'] == False:
            packet = {
                'available_datetime': str(datetime.datetime.now())
            }
            self.bus_access.send_message(
                command = 'scraper_available',
                destination_id = 'broadcast',
                message = packet,
            )
        if not self.scraping and not self.stopped:
            threading.Timer(self.interval, self.broadcast_available).start()

    def broadcast_status(self):
        packet = {
            'status': self.scraper.status,
            'url_data': self.status['url_data'],
            'status_datetime': str(datetime.datetime.now())
        }
        self.bus_access.send_message(
            command = 'scraper_status',
            destination_id = 'broadcast',
            message = packet,
        )

    def broadcast_simple_status(self):
        if self.scraper._data['url_data'] == {}:
            targeturl = None
        else:
            targeturl = self.scraper._data['url_data']['target_url']
        packet = {
            'working': self.scraper._data['working'],
            'url_count': len(self.scraper._data['seen_urls']),
            'bad_url_count': len(self.scraper._data['bad_urls']),
            'target_url': targeturl,
            'status_datetime': str(datetime.datetime.now())
        }
        self.bus_access.send_message(
            command = 'scraper_available',
            destination_id = 'broadcast',
            message = packet,
        )

    def scraper_finished_callback(self,payload):
        self.bus_access.send_message(
            command = payload['command'],
            destination_id = payload['destination_id'],
            message = payload['message'],
        )

    def scraper_started_callback(self,payload):
        self.bus_access.send_message(
            command = payload['command'],
            destination_id = payload['destination_id'],
            message = payload['message'],
        )

    def scraper_broadcast_document_callback(self,payload):
        self.bus_access.send_message(
            command = payload['command'],
            destination_id = payload['destination_id'],
            message = payload['message'],
        )

    def _scraperstart(self):
        if self._DEBUG == True:
            print "Starting scraper ..."
        documents = self.scraper.start()
        if self._DEBUG == True:
            print "Scraper complete."
            print documents

    def _reqcallback(self,payload): #ch,method,properties,body):
        try:
            response = payload
            
            if response['command'] == 'url_dispatch':
                if response['destination_id'] == self.uid:
                    if self.scraping == False:
                        self.scraper.set_url_data(response['message'])
                        #log( "ScraperWrapper._reqcallback(): Launching scraper thread ...", self.DEBUG )
                        self.scraping = True
                        self.scraper_thread = threading.Thread(target=self._scraperstart)
                        self.scraper_thread.start()
                        #log( "ScraperWrapper._reqcallback(): ... Scraper launched successfully.", self.DEBUG )

            elif response['command'] == 'scraper_finished':
                if response['source_id'] == self.scraper.uid:
                    self.scraping = False

            elif response['command'] == 'get_status':
                self.broadcaststatus()

            elif response['command'] == 'get_status_simple':
                self.broadcastsimplestatus()

            elif response['command'] == 'reset_scraper':
                if response['destination_id'] == self.uid:
                    self.resetscraper()

            elif response['command'] == 'shutdown':
                if response['destination_id'] == self.uid:
                    #log( "ScraperWrapper._reqcallback(): [{0}] Shutting Down Recieved".format(self.uid), self.DEBUG ) 
                    self.stop()

            elif response['command'] == 'global_shutdown':
                #log( "ScraperWrapper._reqcallback(): Global Shutdown Recieved", self.DEBUG )
                self.stop()
        except Exception, e:
            print "ERROR: {0}".format(str(e))
