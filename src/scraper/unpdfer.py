from pdfminer.pdfinterp import PDFResourceManager, process_pdf
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
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
            datestring = doc.info[0]['CreationDate'][2:-7]
            ts = strptime(datestring, "%Y%m%d%")
            created = datetime.fromtimestamp(mktime(ts))

            retVal = (created,txt,True)
            retstr.close()
        except:
            retVal = (None,"",False)
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
            created,pdftext,success = self._pdf2text(fp)
            if SCRUB:
                pdftext = self._scrubtext(pdftext)
            pdfhash = hashlib.md5(fp.read()).hexdigest()
            _tokens = nltk.word_tokenize(pdftext)
            tokens = nltk.FreqDist(word.lower() for word in _tokens)
        return (created,pdftext,pdfhash,tokens,success)

