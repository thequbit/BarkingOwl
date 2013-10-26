import pika
import time
import simplejson
import sched

class ScraperStatus():

    _scraperstatus = []

    def __init__(self,address='localhost'):

        #print "Starting Barking Owl Status ..."

        self._scraperstatus = []

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
        self._reqcon.add_timeout(1, self._reqcallback)

        # setup outgoing messages
        self._respcon = pika.BlockingConnection(pika.ConnectionParameters(
                                                  host=self._addr))
        self._respchan = self._respcon.channel()
        self._respchan.exchange_declare(exchange='barkingowl',
                                          type='fanout')        

        #print "Barking Owl Status Started Successfully."

    def run(self):
        #print "Starting message consuming ..."
        self._scraperstatus = []
        self._sendrequest()
        self._reqchan.start_consuming()
        #print "Done."

    def _sendrequest(self):
        #print "Sending 'request_status' command to all scrapers"
        body = {
            'command': 'request_status',
            'scraperid': '0'
        }
        jbody = simplejson.dumps(body)
        
        # send the message
        self._respchan.basic_publish(exchange='barkingowl',
                                     routing_key='', #self._queuename,
                                     body=jbody,
                                    )
    def _printstatus(self):
        print "Scraper Status:"
        for status in self.scraperstatus:
            #print status
            print "scraperid: {0}".format(status['scraperid'])
            print "linkcount: {0}".format(status['linkcount'])
            print "destdir: {0}".format(status['destdir'])
            print "address: {0}".format(status['address'])
            print "processed: {0}".format(status['processed'])
            print "badlinks: {0}".format(status['badlinks'])
            print "queuename: {0}".format(status['queuename'])
            print "busy: {0}".format(status['busy'])
            print "level: {0}".format(status['level'])
            print "-----------------------------------"

    def _reqcallback(self,ch=None,method=None,properties=None,body=None):
        if ch == None and method == None and properties == None and body == None:
            self._reqchan.stop_consuming()
            return
        #print "Processing Response ..."
        response = simplejson.loads(body)
        #print "Message Body: {0}".format(response)
        if response['command'] == 'scraper_status':
            #print "Saving Scraper {0} Status".format(response['scraperid'])
            self._scraperstatus.append(simplejson.loads(response['payload']))

    def getstatus(self):
        return self._scraperstatus

#def main():
#    s = Status()
#    s.sendrequest()

    #sch = sched.scheduler(time.time, time.sleep)
    #sch.enter(5, 1, s.printstatus, ())
#    print "Waiting 5 seconds for scrapers to response to status request ..."
    #sch.run()
#    s.listen()
#    s.printstatus()
#    print "Done."

#main()
