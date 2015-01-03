import pika
import json
from time import strftime
import uuid

from barking_owl.busaccess import BusAccess

class GlobalShutdown():

    def __init__(self,address='localhost',exchange='barkingowl', \
            uid=str(uuid.uuid4()), url_parameters=None, DEBUG=False):

        self.address = address
        self.exchange = exchange
        self.uid = uid
        self.url_parameters = url_parameters        
        self.DEBUG = DEBUG

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
            url_parameters = self.url_parameters,
            DEBUG = self.DEBUG,
        )

        self.bus_access.set_callback(
            callback = self._reqcallback,
        )


    def shutdown(self):
        """
        shutdown() sends the global shutdown command to the message bus.
        """
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'availabledatetime': str(isodatetime)
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
            message = packet
        )

    def _reqcallback(self,payload): #ch,method,properties,body):
        pass

if __name__ == '__main__':

    print 'Sending Global Shutdown Broadcast ...'

    globalshutdown = GlobalShutdown(
        address='localhost',
        exchange='barkingowl',
        #uid = ,
        #url_parameters = ,
        #DEBUG = ,
    )

    globalshutdown.shutdown()

    print 'Exiting.'
