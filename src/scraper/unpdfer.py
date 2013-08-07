from pdfminer.pdfinterp import PDFResourceManager, process_pdf
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from cStringIO import StringIO

import hashlib

import nltk

class UnPDFer:
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

            retVal = (txt,True)
            retstr.close()
        except:
            retVal = ("",False)
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
        with open(filename,'rb') as fp:
            pdftext,success = self._pdf2text(fp)
            if SCRUB:
                pdftext = self._scrubtext(pdftext)
            pdfhash = hashlib.md5(fp.read()).hexdigest()
            _tokens = nltk.word_tokenize(pdftext)
            tokens = nltk.FreqDist(word.lower() for word in _tokens)
        return (pdftext,pdfhash,tokens,success)

