import pika
import json
import uuid

class BusAccess(object):

    def __init__(self,myid,address="localhost",exchange="barkingowl",DEBUG=False):

        self.address = address
        self.exchange = exchange
        self.myid = myid
        self.DEBUG = DEBUG

        # outgoing messages
        self.reqcon = pika.BlockingConnection(pika.ConnectionParameters(host=address))
        self.reqchan = self.reqcon.channel()
        self.reqchan.exchange_declare(exchange=exchange,type='fanout')
        result = self.reqchan.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.reqchan.queue_bind(exchange=exchange,queue=queue_name)
        self.reqchan.basic_consume(self.reqcallback,queue=queue_name,no_ack=True)

        # incoming messages
        self.respcon = pika.BlockingConnection(pika.ConnectionParameters(host=self.address))
        self.respchan = self.respcon.channel()
        self.respchan.exchange_declare(exchange=self.exchange,type='fanout')

        self.callback = None

        if self.DEBUG:
            print "BusAccess INIT Successfull."

    def listen(self):

        """
        Start listening on the message bus.
        """

        if self.DEBUG:
            print "Listening on message bus ..."

        self.reqchan.start_consuming()

    def setcallback(self,callback):

        """
        This sets the callback function to be called when a new message comes in.
        """

        self.callback = callback

        if self.DEBUG:
            print "Callback set."

    def sendmsg(self,command,destinationid,message):
        
        """
        This takes in a command, a destinationid (can be an id or 'broadcast'),
        and a dictionary message to be sent out.  This then broadcasts the message
        to the message bus.
        """

        payload = {
            'command': command,
            'sourceid': self.myid,
            'destinationid': destinationid,
            'message': message,
        }
        jbody = json.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

        print "Message sent successfully to message bus."

    def reqcallback(self,ch,method,properties,body):

        """
        
        This function converts the body of the message recieved from
        json into a dictionary.  It then calls the self.callback().
        
        """

        response = json.loads(body)
        if self.callback != None:
            self.callback(response)

            if self.DEBUG:
                print "Call back called successfully."

def callback(message):

    print message

if __name__ == '__main__':

    myid = str(uuid.uuid4())
    ba = BusAccess(myid=myid,DEBUG=True)
    
    ba.setcallback(callback)

    ba.listen()

    
