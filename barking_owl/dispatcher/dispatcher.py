import pika
import json
import uuid
from time import strftime
import time
import threading

import datetime

from barking_owl.busaccess import BusAccess

class Dispatcher():

    def __init__(self,
                 address='localhost',
                 exchange='barkingowl',
                 self_dispatch=True,
                 uid=str(uuid.uuid4()),
                 url_parameters=None,
                 broadcast_interval=5,
                 DEBUG=False):

        self.address = address
        self.exchange = exchange
        self.self_dispatch = self_dispatch
        self.uid = uid
        self.url_parameters = url_parameters
        self.broadcast_interval = 5 # 5 seconds
        
        self._DEBUG = DEBUG

        self.exiting = False

        self.current_url_index = 0

        self.urls = []
        self.scrapers = []

        self.bus_access = BusAccess(
            uid = self.uid,
            address = self.address,
            exchange = self.exchange,
            url_parameters = self.url_parameters,
            DEBUG = self._DEBUG,
        )

        self.bus_access.set_callback(
            callback = self._reqcallback,
        )

        #self.broadcast_status()

    def set_urls(self,urls):
        if self._DEBUG == True:
            print "Dispatcher.set_urls(): loading url data"
        for i in range(0,len(urls)):
            urls[i]['start_datetime'] = None
            urls[i]['finish_datetime'] = None
            urls[i]['runs'] = []
        self.current_url_index = 0
        self.urls = urls

    def get_next_url_index(self):

        print "\n"
        print self.urls
        print "\n"

        url_index = -1
        for i in range(0,len(self.urls)):
            now = datetime.datetime.now() 
            if self.urls[i]['start_datetime'] == None:
                url_index = i
                break
            else:
                start_datetime = strdatetime.datetime.now()
                if self.urls[i]['finish_datetime'] == "":
                    if now >= start_datetime + datetime.timedelta(hours=24):
                        url_index = i
                        break
                else:
                    if not 'frequency' in self.urls[i]:
                        freq = (24*60) # default to 24 hours
                    else:
                        freq = self.urls[i]['frequency']
                    if self.urls[i]['finish_datetime'] == None:
                        url_index = i
                        break
                    else:
                        finish_datetime = datetime.datetime.strptime(
                            self.urls[i]['finish_datetime'],
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if now >= finish_datetime + datetime.timedelta(minutes=int(freq)):
                            url_index = i
                            break
        return url_index
             
    def start(self):
        self.broadcast_status()

    def broadcast_status(self):
        while not self.exiting:
            packet = {
                'urls': self.urls,
                'current_url_index': self.current_url_index,
                'scrapers': self.scrapers,
                'status_datetime': str(datetime.datetime.now()), 
            }
            self.bus_access.send_message(
                command = 'dispatcher_status',
                destination_id = 'broadcast',
                message = packet,
            )

            if not self.exiting:
                self.bus_access.tsleep(self.broadcast_interval)

    def send_url(self,url_index,destination_id):
        packet = self.urls[url_index]
        self.bus_access.send_message(
            command = 'url_dispatch',
            destination_id = destination_id,
            message = packet,
        )

    def get_remaining_url_count(self):
        remaining = (len(self.urls)-1) - (self.current_url_index-1) 
        return remaining

    def stop(self):
        self.exiting = True
        self.bus_access.stop_listening()

    def _reqcallback(self,payload):
        #print "Dispatcher._reqcallback(): handling callback"
        #try:
        if True:
            response = payload
            if response['command'] == 'set_dispatcher_urls':
                self.set_urls(response['message']['urls'])

            if response['command'] == 'scraper_finished':
                for i in range(0,len(self.urls)):
                    target_url = response['message']['url_data']['target_url']
                    source_id = response['source_id']
                    now = str(datetime.datetime.now())
                    if self.urls[i]['target_url'] == target_url and self.urls[i]['scraper_id'] == source_id:
                        self.urls[i]['finish_datetime'] = now

            if response['command'] == 'scraper_available':
                if self._DEBUG == True:
                    print "Dispatcher._reqcallback(): Scraper availability seen"
                if self.self_dispatch == True:
                    if self._DEBUG == True:
                        print "Dispatcher._reqcallback(): Self dispatching"
                    url_index = self.get_next_url_index()
                else:
                    if self._DEBUG == True:
                        print "Dispatcher._reqcallback(): Not self dispatching, getting durrent url index ..."
                        print "len(self.urls): {0}, self.current_url_index: {1}, len(self.urls)-1: {2}".format(
                            len(self.urls), self.current_url_index, len(self.urls)-1,
                        )
                    url_index = -1
                    if len(self.urls) != 0 and self.current_url_index <= len(self.urls)-1:
                        url_index = self.current_url_index
                        self.current_url_index+=1
                    else:
                        if self._DEBUG == True:
                            print "Dispatcher._reqcallback(): All queued URLs dispatched"
                if not url_index == -1:
                    if self._DEBUG == True:
                        print "Dispatcher._reqcallback(): Dispatching URL"
                    self.urls[url_index]['start_datetime'] = str(datetime.datetime.now())
                    self.urls[url_index]['scraper_id'] = response['source_id']
                    self.urls[url_index]['status'] = 'running'
                    self.send_url(url_index,response['source_id'])
                else:
                    if self._DEBUG == True:
                        print "Dispatcher._reqcallback(): No URLs to dispatch"

            if response['command'] == 'global_shutdown':
                if self._DEBUG == True:
                    print "Exiting."
                self.stop()
        #except Exception, e:
        #    print "BarkingOwl Dispatcher, ERROR: {0}".format(str(e))
