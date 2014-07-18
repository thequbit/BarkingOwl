import pika
import json
import uuid

class BusAccess(object):

    def __init__(self,my_id,address="localhost",exchange="barkingowl",DEBUG=False):

        self.address = address
        self.exchange = exchange
        self.my_id = my_id
        self.DEBUG = DEBUG

        # outgoing messages
        self.reqcon = pika.BlockingConnection(pika.ConnectionParameters(host=address))
        self.reqchan = self.reqcon.channel()
        self.reqchan.exchange_declare(exchange=exchange,type='fanout')
        result = self.reqchan.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.reqchan.queue_bind(exchange=exchange,queue=queue_name)
        self.reqchan.basic_consume(self.req_callback,queue=queue_name,no_ack=True)

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

    def stop_listening(self):

        """
        Stops the pika listener on the message bus.
        """

        if self.DEBUG:
            print "Halting listening on bus ..."

    def set_callback(self,callback):

        """
        This sets the callback function to be called when a new message comes in.
        """

        self.callback = callback

        if self.DEBUG:
            print "Callback set."

    def send_message(self,command,destination_id,message):
        
        """
        This takes in a command, a destinationid (can be an id or 'broadcast'),
        and a dictionary message to be sent out.  This then broadcasts the message
        to the message bus.

        Commands Include:

            Global:

                global_shutdown:
                    direction:
                        inbound/outbound
                    description:
                        This command tells everyone on the message bus to 
                        stop what they are doing and shutdown.  All listeners
                        should respond to this command.

            Scraper:

                scraper_available:
                    direction:
                        outbound
                    description:
                        This command is boradcast every second that the scraper
                        is not working on a URL.

                url_dispatch
                    direction:
                        inbound
                    description:
                        This command tells the scraper that it should begin
                        scraping a new URL.  the 'message' field should contain
                        a dictionary with all required filds for the scraper
                        to work successfully.  Note: if the scraper is already
                        working on a URL, this will override that operation and
                        may cause errors.

                scraper_finished:
                    direction:
                        outbound
                    description:
                        This command is sent by the scraper when it is completed
                        scraping it's assigned URL.

                get_status:
                    direction:
                        inbound
                    description:
                        This command is a request for the scraper to broadcast its
                        complete status packet.  This packet will contain all information
                        that scraper calss currently holds.  Note: depending on the 
                        link depth and size of the site being scraped, this payload
                        can be *very* large.

                get_status_simple:
                    direction:
                        inbound
                    description:
                        This command is a request for the scraper ot broadcast its
                        simple status packet.  This packet includes the following
                        information:

                            packet = {
                                'busy': self.scraper.status['busy'],
                                'linkcount': self.scraper.status['linkcount'],
                                'processedlinkcount': len(self.scraper.status['processed']),
                                'badlinkcount': len(self.scraper.status['badlinks']),
                                'targeturl': targeturl,
                                'statusdatetime': str(isodatetime)
                            }
                
                reset_scraper:
                    direction:
                        inbound
                    description:
                        This command tells the scraper to stop scraping the current 
                        URL and to reset its state.  It is recommended to call this
                        command before dispatching another URL unless the scraper's
                        busy state is False.
                      
                shutdown:
                    direction:
                        inbound
                    description:
                        This command tells the scraper to stop what it's doing and
                        shutdown.  This kills the scraper.

            Dispatcher:
 
               scraper_finished:
                   direction:
                       inbound
                   description:
                       The dispatcher records when the URL has been successfully
                       scraped.  This is used in conjunction with the frequency
                       that was assigned to the URL to only dispatch it as often as
                       it should be.

               url_dispatch:
                   direction:
                       outbound:
                   description:
                       This command is accompanied by all of the information the
                       scraper needs to begin scraping the target URL.  This 
                       command is sent every duration that is defiend in the
                       frequency of the target URL payload.

               scraper_available:
                   direction:
                       inbound
                   description:
                       This command is sent by the scraper and means that it is
                       available to recieve a new URL if there is one available.
                       The dispatcher checks the times of all of the URLs that it
                       has available for dispatch, and if any are more than the
                       defined frequency, a URL is dispatched to the scraper.



        """

        if self.DEBUG:
            print "Attempting to send message to bus ..."

        payload = {
            'command': command,
            'source_id': self.my_id,
            'destination_id': destination_id,
            'message': message,
        }
        jbody = json.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

        print "Message sent successfully to message bus."

    def req_callback(self,ch,method,properties,body):

        """
        
        This function converts the body of the message recieved from
        json into a dictionary.  It then calls the self.callback().
        
        """

        response = json.loads(body)
        if self.callback != None:
            self.callback(response)

            if self.DEBUG:
                print "Call back called successfully."

def callback(payload):

    print payload

if __name__ == '__main__':

    my_id = str(uuid.uuid4())
    ba = BusAccess(
        my_id = my_id,
        address = "localhost",
        exchange = "barkingowl",
        DEBUG = True
    )
    
    ba.set_callback(callback)

    ba.send_message('*','broadcast',{'msg':'test'})

    ba.listen()

    
