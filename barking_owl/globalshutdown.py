import pika
import json
from time import strftime
import datetime
import uuid

from barking_owl.busaccess import BusAccess

class GlobalShutdown():

    def __init__(self,address='localhost',exchange='barkingowl', \
            uid=str(uuid.uuid4()), url_parameters=None, DEBUG=False):

        self.address = address
        self.exchange = exchange
        self.uid = uid
        self.url_parameters = url_parameters        
        self._DEBUG = DEBUG

        #setup message bus
        #self.reqcon = pika.BlockingConnection(pika.ConnectionParameters(host=address))
        #self.reqchan = self.reqcon.channel()
        #self.reqchan.exchange_declare(exchange=exchange,type='fanout')
        #result = self.reqchan.queue_declare(exclusive=True)
        #queue_name = result.method.queue
        #self.reqchan.queue_bind(exchange=exchange,queue=queue_name)
        #self.reqchan.basic_consume(self.reqcallback,queue=queue_name,no_ack=True)

        #self.respcon = pika.BlockingConnection(pika.ConnectionParameters(
        #                                                   host=self.address))
        #self.respchan = self.respcon.channel()
        #self.respchan.exchange_declare(exchange=self.exchange,type='fanout')

        self.bus_access = BusAccess(
            uid = self.uid,
            address = self.address,
            exchange = self.exchange,
            heartbeat_interval = 30,
            url_parameters = self.url_parameters,
            DEBUG = self._DEBUG,
        )

        self.bus_access.set_callback(
            callback = self._reqcallback,
        )

        self.bus_access.stop_listening()
        #self.bus_access.tsleep(1)

        if self._DEBUG == True:
            print "GlobalShutdown.__init__(): init complete."

    def shutdown(self):
        """
        shutdown() sends the global shutdown command to the message bus.
        """

        if self._DEBUG == True:
            print "GlobalShutdown.shutdown(): sending shutdown command" 

        message = {
            'shutdown_datetime': str(datetime.datetime.now())
        }
        #payload = {
        #    'command': 'global_shutdown',
        #    'sourceid': self.uid,
        #    'destinationid': 'broadcast',
        #    'message': packet
        #}
        #jbody = json.dumps(payload)
        #self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

        self.bus_access.send_message(
            command = 'global_shutdown',
            destination_id = 'broadcast',
            message = message,
        )

        if self._DEBUG == True:
            print "GlobalShutdown.shutdown(): shutdown message sent successfully."

    def _reqcallback(self,payload): #ch,method,properties,body):
        pass

if __name__ == '__main__':

    print 'Sending Global Shutdown Broadcast ...'

    globalshutdown = GlobalShutdown(
        address='localhost',
        exchange='barkingowl',
        #uid = ,
        #url_parameters = ,
        DEBUG = True,
    )

    globalshutdown.shutdown()

    print 'Exiting.'
