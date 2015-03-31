import datetime

import pika
import json
import uuid
import time

import threading

import traceback

class ReceiveThread(threading.Thread):

    def __init__(self,
                 uid=str(uuid.uuid4()),
                 address="localhost",
                 exchange="barkingowl",
                 heartbeat_interval=30,
                 url_parameters=None,
                 DEBUG=False):

        threading.Thread.__init__(self,name="ReceiveThread : %s" % uid)

        self.uid = uid
        self.address = address
        self.exchange = exchange
        self.heartbeat_interval = heartbeat_interval
        self.url_parameters = url_parameters
        self._DEBUG = DEBUG

        self.reqcon = None
        self.reqchan = None
        self._callback = None
        self.consuming = False

    def set_callback(self, callback):
        self._callback = callback

    def run(self):

        if self.url_parameters == None:
            self.reqcon = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.address,
                    heartbeat_interval=self.heartbeat_interval,
                )
            )
        else:
            self.reqcon = pika.BlockingConnection(
                self.url_parameters
            )

        self.reqchan = self.reqcon.channel()
        self.reqchan.exchange_declare(
            exchange=self.exchange,
            type='fanout'
        )
        result = self.reqchan.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.reqchan.queue_bind(
            exchange=self.exchange,
            queue=queue_name
        )
        self.reqchan.basic_consume(
            self._req_callback,
            queue=queue_name,
            no_ack=True
        )

        #self.sleep(5)

        if self._DEBUG == True:
            print "BusAccess.ReceiveThread.run(): starting message consuming ..."

        self._sleeping = False

        self.consuming = True

        self.reqchan.start_consuming()

    def _req_callback(self, ch, method, properties, body):

        try:
            response = json.loads(body)
            if self._callback != None:
                self._callback(response)
                if self._DEBUG == True:
                    print "BusAccess.req_callback(): Call back called successfully."
        except Exception, e:
            print  "BusAccess.ReceiveThread._req_callback(): error: {0}".format(e)

    def stop_listening(self):
        try:
            if self._DEBUG == True:
                print "BusAccess.ReceiveThread.stop_listening(): Stopping message consuming ..."
            self.reqchan.basic_cancel(nowait=True)
            self.reqchan.stop_consuming()
            #self.reqcon.close()
            if self._DEBUG == True:
                print "BusAccess.ReceiveThread.stop_listening(): Message consuming stopped." 

        except Exception, e:
            if self._DEBUG == True:
                print "BusAccess.ReceiveThread.stop_listening(): ERROR: {0}".format(e)
    def sleep(self, duration):
        if not self.reqchan == None:
            if self._DEBUG == True:
                print "BusAccess.ReceiveThread.sleep(): Sleeping ..."
            self._sleeping = True
            self.reqcon.sleep(duration)
            self._sleeping = False

    def is_sleeping(self):
        return self._sleeping

    def ready(self):
        _ready = False
        if self.reqcon != None and self.reqchan != None and self.consuming == True:
            _ready = (self.reqcon.is_open and self.reqchan.is_open)
        return _ready

class TransmitThread(threading.Thread):

    def __init__(self,
                 uid=str(uuid.uuid4()),
                 address="localhost",
                 exchange="barkingowl",
                 heartbeat_interval=30,
                 url_parameters=None,
                 DEBUG=False):

        threading.Thread.__init__(self,name="TransmitThread : %s" % uid)

        self.uid = uid
        self.address = address
        self.exchange = exchange
        self.heartbeat_interval = heartbeat_interval
        self.url_parameters = url_parameters
        self._DEBUG = DEBUG

        self.respcon = None
        self.respchan = None

        self._started = False

    def run(self):
        if self.url_parameters == None:
            self.respcon = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.address,
                    heartbeat_interval=self.heartbeat_interval,
                )
            )
        else:
            self.respcon = pika.BlockingConnection(
                self.url_parameters
            )
        self.respchan = self.respcon.channel()
        self.respchan.confirm_delivery()
        self.respchan.exchange_declare(
            exchange=self.exchange,
            type='fanout'
        )

        #self.sleep(5)

        self._sleeping = False
        self._started = True

        if self._DEBUG == True:
            print "BusAccess.TransmitThread: Ready!"

    def send_message(self, command, destination_id, message):
        if self._started == False:
            return

        success = False
        try:
            payload = {
                'command': command,
                'source_id': self.uid,
                'destination_id': destination_id,
                'message': message,
            }
            jbody = json.dumps(payload)

            # some odd stuff happens if we don't do this ... like
            # functions within pika not resolving ...
            while self._sleeping:
                continue

            self.respchan.basic_publish(
                exchange = self.exchange,
                routing_key = '',
                body = jbody,
                #mandatory = False,
                immediate = False,
            )
            if self._DEBUG == True:
                print "BusAccess.send_message(): Message sent successfully to message bus."
                #print "BusAccess.send_messsage(): payload: {0}".format(payload)
            success = True
        except Exception, e:
            if self._DEBUG == True:
                print "BusAccess.TransmitThread.send_message(): ERROR: {0}".format(e)
                print traceback.format_exc()
                print "BusAccess.TransmitThread.send_message(): Error Payload Command: {0}".format(command)

    def sleep(self, duration):
        if not self.respchan == None:
            if self._DEBUG == True:
                print "BusAccess.TransmitThread.sleep(): Sleeping ..."
            self._sleeping = True
            self.respcon.sleep(duration)
            self._sleeping = False

    def is_sleeping(self):
        return self._sleeping

    def ready(self):
        return self._started

class BusAccess(object):

    def __init__(self,
                 uid=str(uuid.uuid4()),
                 address="localhost",
                 exchange="barkingowl",
                 heartbeat_interval=30,
                 url_parameters=None,
                 DEBUG=False):

        self.uid = uid
        self.address = address
        self.exchange = exchange
        self.heartbeat_interval = heartbeat_interval
        self.url_parameters = url_parameters
        self._DEBUG = DEBUG

        if self._DEBUG == True:
            print "BusAccess.__init__(): starting transmit thread ..." 
        self._transmit_thread = TransmitThread(
            uid = self.uid,
            address = self.address,
            exchange = self.exchange,
            heartbeat_interval = self.heartbeat_interval,
            url_parameters = self.url_parameters,
            DEBUG = self._DEBUG,
        )
        self._transmit_thread.start()
        if self._DEBUG == True:
            print "BusAccess.__init__(): transmit thread started successfully."        

        
        if self._DEBUG == True:
            print "BusAccess.__init__(): starting receive thread ..."
        self._receive_thread = ReceiveThread(
            uid = self.uid,
            address = self.address,
            exchange = self.exchange,
            heartbeat_interval = self.heartbeat_interval,
            url_parameters = self.url_parameters,
            DEBUG = self._DEBUG,
        )
        self._receive_thread.start()
        if self._DEBUG == True:
            print "BusAccess.__init__(): receive thread started successfully."

        # wait until we're ready
        while self._receive_thread.ready() == False or \
                self._transmit_thread.ready() == False:
            pass
        

        if self._DEBUG == True:
            print "BusAccess.__init__(): init complete."

    def set_callback(self, callback):
        self._receive_thread.set_callback(callback)

    def send_message(self, command, destination_id, message):
        self._transmit_thread.send_message(command, destination_id, message) 

    def stop_listening(self):
        self._receive_thread.stop_listening()

    def tsleep(self, duration):
        self._transmit_thread.sleep(duration)

    def rsleep(self,duration):
        self._receive_thread.sleep(duration)

