from pdfminer.pdfinterp import PDFResourceManager, process_pdf
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdftypes import PDFObjRef
from cStringIO import StringIO

import hashlib

import time
import datetime

import pika

class Interface():

    def __init__():

        self._queuename = 'barkingowl'

        # setup incomming messages
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
                                    no_ack=True
                                   )

        # setup outgoing messages
        self._respcon = pika.BlockingConnection(pika.ConnectionParameters(
                                                  host=self._addr))
        self._respchan = self._respcon.channel()
        self._respchan.exchange_declare(exchange='barkingowl',
                                          type='fanout')

    def _reqcallback(self,ch,method,properties,body):
        self.r("Processing New Message ...")
        try:
            response = simplejson.loads(body)
            self.r("Message Body: {0}".format(response))
            if reponse['command'] == 'doc_payload':
                self._busy = True
                self.r("Processing PDF payload ...")

                # TODO: Process PDF here.

                self.r("Payload processed successfully.")
                self._busy = False
        except Exception, e:
            self.r("ERROR: an error happened while processing the message:\n\t{0}".format(e)
        return True

    def 
