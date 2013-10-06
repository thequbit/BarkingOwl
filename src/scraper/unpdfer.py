from pdfminer.pdfinterp import PDFResourceManager, process_pdf
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdftypes import PDFObjRef
from cStringIO import StringIO

from time import mktime, strptime
from datetime import datetime

import hashlib

import nltk

class UnPDFer:

    _verbose = False

    def __init__(self,verbose):
        self._verbose = verbose

    def _report(self,text):
        if self._verbose:
            print "[unPDFer ] {0}".format(text)

    def _pdf2text(self,fp):
        try:
            rsrcmgr = PDFResourceManager()
            retstr = StringIO()
            codec = 'ascii'
            laparams = LAParams()
            laparams.all_texts = True
            device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)

            process_pdf(rsrcmgr, device, fp)
            device.close()

            # fix the non-utf8 string ...
            result = retstr.getvalue()
            txt = result.encode('ascii','ignore')

            # TODO: clean this up, I feel like I'm doing the converstion twice ...
            # http://stackoverflow.com/a/16503222/2154772
            parser = PDFParser(fp)
            doc = PDFDocument()
            parser.set_document(doc)
            doc.set_parser(parser)
            doc.initialize()
            #print doc.info[0]['CreationDate'].resolve()
            
            #
            # as messed up as this is ... CreationDate isn't always the same type as it
            # comes back from the PDFParser, so we need to base it on an instance of a
            # basestring or not.  I'm starting to dislike PDFs ...
            #
            if not isinstance(doc.info[0]['CreationDate'],basestring):
                datestring = doc.info[0]['CreationDate'].resolve()[2:-7]
            else:
                datestring = doc.info[0]['CreationDate'][2:-7]

            # test to see if it's just the data, or date and time
            if len(datestring) != 14:
                # just the date, need to grab the time
                #print doc.info[0]['CreationTime']
                #for key, value in doc.info[0].iteritems() :
                #    print key
                #timestring = doc.info[0]['CreationTime'][2:-7]
                #datetimestring = "".join(datestring,timestring)
                ts = strptime(datestring, "%Y%m%d")
            else:
                # hanging out together
                ts = strptime(datestring, "%Y%m%d%H%M%S")

            created = datetime.fromtimestamp(mktime(ts))

            retVal = (created,txt,True,"")
            retstr.close()
        except Exception, exceptiontext:
            retVal = (None,"",False,exceptiontext)
            pass
        return retVal

    def _scrubtext(self,text):
        text = text.replace(',','').replace('.','').replace('?','')
        text = text.replace('/',' ').replace(':','').replace(';','')
        text = text.replace('<','').replace('>','').replace('[','')
        text = text.replace(']','').replace('\\',' ').replace('"','')
        text = text.replace("'",'').replace('`','')
        return text

    def unpdf(self,filename,SCRUB=False):
        self._report("Processing '{0}'".format(filename))
        with open(filename,'rb') as fp:
            created,pdftext,success,exceptiontext = self._pdf2text(fp)
            if SCRUB:
                pdftext = self._scrubtext(pdftext)
            pdfhash = hashlib.md5(fp.read()).hexdigest()
            _tokens = nltk.word_tokenize(pdftext)
            tokens = nltk.FreqDist(word.lower() for word in _tokens)
        return (created,pdftext,pdfhash,tokens,success,exceptiontext)

