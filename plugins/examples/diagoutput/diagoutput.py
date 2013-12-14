import pika
import simplejson

class DiagOutput():

    def __init__(self,address='localhost',exchange='barkingowl'):

        #setup message bus
        self.reqcon = pika.BlockingConnection(pika.ConnectionParameters(host=address))
        self.reqchan = self.reqcon.channel()
        self.reqchan.exchange_declare(exchange=exchange,type='fanout')
        result = self.reqchan.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.reqchan.queue_bind(exchange=exchange,queue=queue_name)
        self.reqchan.basic_consume(self.reqcallback,queue=queue_name,no_ack=True)

    def start(self):
        print "Listening for new documents ..."
        self.reqchan.start_consuming()

    def reqcallback(self,ch,method,properties,body):
        response = simplejson.loads(body)
        if response['command'] == 'found_doc':
            docurl = response['message']['docurl']
            linktext = response['message']['linktext']
            print "{0}: {1}".format(linktext,docurl)

def main():
    diagoutput = DiagOutput()
    diagoutput.start()

main()
