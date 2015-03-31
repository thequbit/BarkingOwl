import pika
import json
import uuid
from time import strftime
import time
import threading
import datetime

from scraper import Scraper

from barking_owl.busaccess import BusAccess

import traceback

import logging
logging.basicConfig()

class ScraperWrapper(object): #threading.Thread):

    def __init__(self,
                 address='localhost',
                 exchange='barkingowl',
                 heartbeat_interval=30,
                 url_parameters=None,
                 broadcast_interval=5,
                 uid=str(uuid.uuid4()),
                 DEBUG=False):

        #threading.Thread.__init__(self,name="ScraperWrapper : %s" % uid)

        self.uid = str(uuid.uuid4())
        self.address = address
        self.exchange = exchange
        self.heartbeat_interval = heartbeat_interval
        self.url_parameters = url_parameters
        
        self.broadcast_interval = broadcast_interval
        self.uid = uid
        self._DEBUG=DEBUG

        print "ScraperWrapper().__init__(): Creating scraper ..."
 
        self.scraper = Scraper(
            DEBUG = self._DEBUG,
        )
        self.scraping = False
        self.scraper_thread = None

        print "ScraperWrapper().__init__(): Scraper Created."

        self.stopped = False

        self.bus_access = BusAccess(
            uid = self.uid,
            address = self.address,
            exchange = self.exchange,
            heartbeat_interval = self.heartbeat_interval,
            url_parameters = self.url_parameters,
            DEBUG = self._DEBUG,
        )
        self.bus_access.set_callback(
            callback = self._reqcallback,
        )

        #threading.Timer(self.interval, self.broadcast_available).start()
        #threading.Timer(self.interval, self.broadcast_simple_status).start()

        #self.broadcast_status()

        #log( "ScraperWrapper.__init__(): Scraper Wrapper INIT complete.", self.DEBUG )

    #def run(self):
    def start(self):
        self.scraper.set_callbacks(
            start_callback = self.scraper_started_callback,
            finished_callback = self.scraper_finished_callback,
            found_doc_callback = self.scraper_broadcast_document_callback,
            new_url_callback = None,
            bandwidth_limit_callback = None,
            memory_limit_callback = None,
            error_callback = None,
        )

        self.broadcast_status()

    def stop(self):
        self.bus_access.stop_listening()
        self.scraper.stop()
        self.stopped = True

    def reset_scraper(self):
        self.scraper.reset()

    def broadcast_status(self):

        if self._DEBUG == True:
            print "ScraperWrapper().broadcast_status(): Entering status loop."
        
        while not self.scraping and not self.stopped:

            if self._DEBUG == True:
                print "ScraperWrapper.broadcast_status() sending status pulse ..."

            if self.scraping == False and self.scraper._data['working'] == False:
                packet = {
                    'available_datetime': str(datetime.datetime.now())
                }
                self.bus_access.send_message(
                    command = 'scraper_available',
                    destination_id = 'broadcast',
                    message = packet,
                )

            '''
            packet = {
                'working': self.scraper._data['working'],
                'seen_url_count': len(self.scraper._data['seen_urls']),
                'document_count': len(self.scraper._data['documents']),
                'url_data': self.scraper._data['url_data'],
                'status_datetime': str(datetime.datetime.now())
            }
            self.bus_access.send_message(
                command = 'scraper_status',
                destination_id = 'broadcast',
                message = packet,
            )

            if self._DEBUG == True:
                print "ScraperWrapper.broadcast_status() status sent successfully."
            '''

            if not self.stopped:
                #threading.Timer(self.interval, self.broadcast_available).start()
                self.bus_access.tsleep(self.broadcast_interval)

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

    def scraper_finished_callback(self, _data):
        self.bus_access.send_message(
            command = 'scraper_finished',
            destination_id = 'broadcast',
            message = _data,
        )

    def scraper_started_callback(self, _data):
        self.bus_access.send_message(
            command = 'scraper_started',
            destination_id = 'broadcast',
            message = _data,
        )

    def scraper_broadcast_document_callback(self, _data, document):
        self.bus_access.send_message(
            command = 'scraper_found_document',
            destination_id = 'broadcast',
            message = {
                'url_data': _data['url_data'],
                'document': document,
            },
        )

    def _scraperstart(self):
        if self._DEBUG == True:
            print "ScraperWrapper()._scraperstart(): Starting scraper ..."
        documents = self.scraper.start()
        if self._DEBUG == True:
            print "ScraperWrapper()._scraperstart(): Scraper complete."
            #print documents
        self.scraping = False

    def _reqcallback(self,payload): #ch,method,properties,body):
        try:
            response = payload
           
            if self._DEBUG == True:
                print "ScraperWrapper()._reqcallback(): new message: {0}".format(response)
 
            if response['command'] == 'url_dispatch':
                if response['destination_id'] == self.uid:
                    if self.scraping == False:
                        self.scraper.set_url_data(response['message'])
                        #log( "ScraperWrapper._reqcallback(): Launching scraper thread ...", self.DEBUG )
                        self.scraping = True
                        self.scraper_thread = threading.Thread(target=self._scraperstart)
                        self.scraper_thread.start()
                        #self._scraperstart()
                        #log( "ScraperWrapper._reqcallback(): ... Scraper launched successfully.", self.DEBUG )

            #elif response['command'] == 'scraper_finished':
            #    if response['source_id'] == self.scraper.uid:
            #        self.scraping = False

            elif response['command'] == 'get_status':
                self.broadcast_status()

            elif response['command'] == 'get_status_simple':
                self.broadcast_simple_status()

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
            print "ScraperWrapper._reqcallback(): ERROR: {0}".format(str(e))
            print traceback.format_exc()
