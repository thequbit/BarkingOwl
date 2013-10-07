import time
from time import strftime, mktime, strptime
from time import clock
import re
import os
import threading
import hashlib
from random import randint

from time import mktime, strptime
from datetime import datetime

import urllib
import urllib2

import magic

from bs4 import BeautifulSoup

#import nltk

from pdfminer.pdfinterp import PDFResourceManager, process_pdf
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdftypes import PDFObjRef
from cStringIO import StringIO


#from pdfimp import pdfimp
#from dler import DLer
from unpdfer import UnPDFer


# sql2api db access 
from models import *

verbose = True

class Scrapper(threading.Thread):

    verbose = True

    def __init__(self,threadnumber,orgid,orgname,orgurls,destdir,linklevel):

        threading.Thread.__init__(self)

        self.threadnumber = threadnumber
        self.orgid = orgid
        self.orgname = orgname
        self.orgurls = orgurls
        self.destdir = destdir
        self.linklevel = linklevel

        #self.imp = pdfimp(self.verbose)

        self._processed = []
        self._linkcount = 0

        self.urls= Urls()
        self.orgs = Orgs()
        self.scrapes = Scrapes()
        self.docs = Docs()

    def run(self):
        self.r("Thread #{0} Loaded".format(self.threadnumber))
        self.scrapeurls(self.orgname,
                        self.orgurls,
                        self.destdir,
                        self.linklevel
        )
        self.r("Thread #{0} Exiting".format(self.threadnumber))

    def r(self,text):
        if self.verbose:
            print "[Scrapper {0}] {1}".format(self.name,text)

    #def _unpdf(self,filename):
    #    unpdfer = UnPDFer(self._threadnumber)
    #    created,pdftext,pdfhash,tokens,success,exceptiontext = unpdfer._unpdf(filename=filename,SCRUB=False)
    #    return created,pdftext,pdfhash,tokens,success,exceptiontext

    #def _getpdfs(self,url,maxlevels):
    #    #imp = pdfimp(verbose)
    #    links,linkcount = self.imp._getpdfs(maxlevel=maxlevels,siteurl=url,links=[(url,url)])
    #    return links,linkcount

    def _gethashs(self,urlid):
        #docs = Docs()
        hashs = self.docs._gethashs(urlid) 
        return hashs

    def _addscrape(self,orgid,start,end,success,urlid,linkcount):
        #scrapes = Scrapes()
        scrapeid = self.scrapes.add(orgid,start,end,success,urlid,linkcount)
        return scrapeid

    def _updatescrape(self,scrapeid,orgid,start,end,success,urlid,linkcount):
        #scrapes = Scrapes()
        self.scrapes.update(scrapeid,orgid,start,end,success,urlid,linkcount)

    def _adddoc(self,orgid,docurl,filename,linktext,downloaddatetime,
               creationdatetime,doctext,dochash,urlid,processed):
        #docs = Docs()
        docid = self.docs.add(orgid,docurl,filename,linktext,downloaddatetime,
                              creationdatetime,doctext,dochash,urlid,processed)
        return docid

    def _unpdf(self,filename):
        try:

            rsrcmgr = PDFResourceManager()
            retstr = StringIO()
            codec = 'ascii'
            laparams = LAParams()
            laparams.all_texts = True
            device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)

            fp = open(filename,'rb')

            pdfhash = hashlib.md5(fp.read()).hexdigest()

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

            fp.close()

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

            retVal = (created,txt,pdfhash,True)
            retstr.close()
        except Exception, e:
            self.r("ALERT: pdf error: {0}".format(e))
            retVal = (None,"","",False)
            pass
        return retVal

    def _createlink(self,siteurl,link):
        if ( (len(link) >= 7 and link[0:7].lower() == "http://") or
             (len(link) >= 8 and link[0:8].lower() == "https://") or
             (len(link) >= 3 and link[0:6].lower() == "ftp://") ):
            if(link[:link.find("/",7)+1] != siteurl):
                siteurlmatch = False
            retval = (False,link)
        else:
            retval = (True,siteurl + link)
        return retval

    def _getpagelinks(self,siteurl,url):
        links = []
        #self.r("Getting page links ...")
        _sucess,linktype = self._typelink(url,1024)
        #self.r("Link Type = '{0}'".format(linktype))
        if linktype != "text/html":
            #self._report("Ignoring on-html URL.")
            return links
        try:
            html = urllib2.urlopen(url)
            soup = BeautifulSoup(html)
            atags = soup.find_all('a', href=True)
            #self.r("Found {0} possible links ...".format(len(atags)))
            for tag in atags:
                if len(tag.contents) >= 1:
                    linktext = unicode(tag.string).strip()
                else:
                    linktext = ""
                match,link = self._createlink(siteurl,tag['href'])
                links.append((match,link,linktext))
        except Exception, e:
            links = []
            self.r("ERROR: An error has happened in _getpagelinks():\n\n\t{0}".format(e))
        self._linkcount += len(links)
        return links

    def _typelink(self,link,filesize):
        req = urllib2.Request(link, headers={'Range':"byte=0-{0}".format(filesize)})
        success = True
        filetype = ""
        try:
            payload = urllib2.urlopen(req,timeout=5).read(filesize)
            filetype = magic.from_buffer(payload,mime=True)
        except Exception, e:
            success = False;
            #elf.r("ERROR: An error has happened in _typelink():\n\n\t{0}".format(e))
        return success,filetype

    def _fileexists(self,filename):
        exists = False
        if filename == None:
            exists = False
        else:
            try:
                with open(filename):
                    exists = True
                    pass
            except:
                exists = False
        return exists

    def _downloadfile(self,url):
        success = True
        try:
            urlfile = url[url.rfind("/")+1:]
            t = time.time()
            r = randint(0,10000000)
            _filename = "{0}/{1}_{2}{3}.download".format(self.destdir,urlfile,t,r)
            while self._fileexists(_filename):
                _filename = "{0}/{1}_{2}.download".format(self.destdir,urlfile,t)
            #print _filename
            filename,_headers = urllib.urlretrieve(url,_filename)
            #self.r("Successfully downloaded file:")
            #self.r("\t{0}".format(filename))
        except Exception, e:
            self.r("ERROR: an error occured while trying to download the file:\n\n\t{0}".format(e))
            filename = ""
            success = False
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        return (filename,isodatetime,success)

    def _processpdf(self,urlid,docurl,linktext):
        #self._report("Found Document: '{0}'".format(gotlink))
        self.r("Saving: {0}".format(docurl))
        filename,downloaddatetime,success = self._downloadfile(docurl)
        if success == True:
            creationdatetime,doctext,dochash,success = self._unpdf(filename)
            if success == True:
                doctext = re.sub(' +',' ',doctext)
                doctext = re.sub('\n+','\n',doctext)
                processed = True
            else:
                processed = False
            docid = self._adddoc(self.orgid,docurl,filename,linktext,downloaddatetime,
                                 creationdatetime,doctext,dochash,urlid,processed)
        else:
            self.r("Ignoring document due to error")
            pass

    def _followlinks(self,urlid,maxlevel,siteurl,links,level=0,filesize=1024):
        retlinks = []
        if( level >= maxlevel ):
            #self.r("ALERT: Max link depth reached")
            pass
        else:
            level += 1
            for _link in links:
                link,linktext = _link
                # see if we can ignore this link
                if link in self._processed:
                    continue
                ignored = 0
                #self.r("Getting links for '{0}'".format(link))
                allpagelinks = self._getpagelinks(siteurl,link)
                _m,_l = self._createlink(siteurl,link)
                self._processed.append(_l)
                self.r("Processing {0} links for {1}".format(len(allpagelinks),link))
                
                # Look at the links found on the page, and add those that are within
                # the domain to 'thelinks'
                pagelinks = []
                for pagelink in allpagelinks:
                    match,link,linktext = pagelink
                    if( match == True ):
                        pagelinks.append((link,linktext))

                # Follow all of the link within the 'thelink' array
                gotlinks = self._followlinks(urlid=urlid,maxlevel=maxlevel,siteurl=siteurl,
                                             links=pagelinks,level=level,filesize=filesize)
                
                # go through all of the returned links and see if any of them are pdfs
                for _gotlink in gotlinks:
                    gotlink,linktext = _gotlink
                    # only process if we haven't processed it
                    if not any(gotlink in r for r in retlinks):
                        success,linktype = self._typelink(gotlink,filesize)
                        # if it is a pdf, then add it to the self._pdfs array
                        if success == True and linktype == 'application/pdf':
                            retlinks.append((gotlink,linktext))
                            #self._pdfs.append((gotlink,linktext))
                            #self.r("PDF found, processing and saving to database")
                            self._processed.append(gotlink)
                            self._processpdf(urlid,gotlink,linktext)
                        else:
                            ignored += 1
                
                # Some of the links that were returned from this page might be pdfs,
                # if they are, add them to the list of pdfs to be returned 'retlinks'
                for _thelink in pagelinks:
                   thelink,linktext = _thelink
                   if not any(thelink in r for r in retlinks):
                        success,linktype = self._typelink(thelink,filesize)
                        if success == True and linktype == 'application/pdf':
                            #self._pdfs.append((thelink,linktext))
                            retlinks.append((thelink,linktext))
                            self._processed.append(thelink)
                            self._processpdf(urlid,thelink,linktext)
                            #self._report("Added '{0}'".format(thelink))
                            #self._report("Found Document: '{0}'".format(thelink))
                        else:
                            ignored += 1
                #self.r("Ignored Page Links: {0}/{1}".format(ignored,len(pagelinks)))
            level -= 1
        for l in links:
            self._processed.append(l)
        return retlinks

    #def scrapeurls():
    #    self._processed = []
    #    self._followlinks()
    #    #self._processed = []

#    def scrapper(self,url,downloaddirectory,linklevel):
#        docs = []
#        links,linkcount = self._getpdfs(url,linklevel)
#        self.r("Processed {0} links, found {1} PDF documents from {2}".format(linkcount,len(links),url))
#        for link in links:
#            u,n = link
#            self.r("    {0}".format(u))
#        if not os.path.exists(downloaddirectory):
#            os.makedirs(downloaddirectory)
#        files,success = self.downloadfiles(links=links,destinationfolder=downloaddirectory)
#        #eport("Downloaded {0} files with a status of {1}".format(len(files),success)
#        if not success:
#            self.r("ERROR: Unable to download all files.")
#            retsuccess = False
#        else:
#            self.r("Processing PDF documents for URL:")
#            self.r("    {0}".format(url))
#            for f in files:
#                url,filename,linktext,downloaded = f
#                created,pdftext,pdfhash,tokens,success,exceptiontext = self._unpdf(filename,True)
#                if success:
#                    #docs.append((url,filename,linktext,downloaded,created,pdftext,pdfhash,tokens))
#                    processed = True
#                else:
#                    #retsuccess = False
#                    self.r("ERROR: unpdf returned the following error: {0}".format(exceptiontext))
#                    processed = False
#                docs.append((url,filename,linktext,downloaded,created,pdftext,pdfhash,tokens,processed))
#                    #
#                    # TODO: Log exceptiontext
#                    #
#                    #break
#        return docs,linkcount

    def scrapeurls(self,orgname,orgurls,destinationdirectory,linklevel):
        time.sleep(1)
        retsuccess = True
        self.r("Working on {0} URLs for '{1}'...".format(len(orgurls),orgname))
        for _url in orgurls:
            urlid,orgid,url,urlname,description,createdatetime,creationuserid,linklevel = _url
            starttime = strftime("%Y-%m-%d %H:%M:%S")
            
            scrapeid = self._addscrape(orgid,starttime,"",False,urlid,0)

            self.r("Scrapper Started at: {0}".format(starttime))
            self.r("Link Level = {0}".format(linklevel))
            self.r("Running on: '{0}'".format(urlname))
            self.r("    {0}".format(url))
            self.r("")
            
            self._processed = []
            links=[(url,url)]
            self._followlinks(urlid=urlid,maxlevel=linklevel,siteurl=url,
                              links=links,level=0,filesize=2048)

            #docs,linkcount = self.scrapper(url,destinationdirectory,linklevel)
            #if success:
            #self.r("Found {0} PDF documents".format(len(docs)))
            #for doc in docs:
            #    hashs = self._gethashsurlid)
            #    docurl,filename,linktext,downloaddatetime,creationdatetime,doctext,dochash,tokens,processed = doc
                #self.r("this hash: {0}".format(dochash))
                #self.r("doc hashs: {0}".format(hashs))
            #    if not dochash in hashs or dochash == "":
            #        self.r("Pushing PDF to database:")
            #        self.r("\t{0}".format(docurl))
                    #self.r("\t{0}".format(filename))
                    #docid = _adddoc(docurl,filename,linktext,downloaded,created,doctext,dochash,url.id)
                   
                    # cleanup the text a bit.  get rid of repeating spaces and \n to save DB space
            #        doctext = re.sub(' +',' ',doctext)
            #        doctext = re.sub('\n+','\n',doctext)

                    # prevents 'None' from being inserted into the database
            #        if linktext == None:
            #            linktext = ""

            #        docid = self._adddoc(orgid,docurl,filename,linktext,downloaddatetime,
            #                            creationdatetime,doctext,dochash,urlid,processed)

            #    else:
            #        self.r("PDF already processed, deleting local file")
            #        self.deletefile(filename)
            
            endtime = strftime("%Y-%m-%d %H:%M:%S")
            self._updatescrape(scrapeid,orgid,starttime,endtime,retsuccess,urlid,self._linkcount)
            self._linkcount = 0
        return retsuccess

    #def split_list(alist, wanted_parts=1):
    #    length = len(alist)
    #    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts]
    #             for i in range(wanted_parts) ]

    def deletefiles(self,folder):
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except:
                pass

    def deletefile(self,filename):
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except:
            pass

lol = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]

def getorgs():
    orgs = Orgs()
    allorgs = orgs.getall()
    return allorgs

def geturls(orgid):
    urls = Urls()
    #orgurls = urls.byorgid(orgid)
    orgurls = []
    orgurls.append(urls.get(25))
    orgurls.append(urls.get(10))
    return orgurls


def runscrapper(destdir,linklevel):
    # get all of the organizations from the database and pull the urls for each of them
    orgs = getorgs()
    thrds = []
    report("Working on {0} Organizations.".format(len(orgs)))
    for org in orgs:
        orgid,orgname,description,creationdatetime,ownerid = org
        report("Dispatching threads for '{0}'".format(orgname))
        orgurls = geturls(orgid)
        threadcount = 8
        parts = lol(orgurls,1) #len(orgurls)/(threadcount-2))
        #parts = []
        #parts.append(urls[0])
        #parts.append(urls[1])
        #print parts
        i = 1
        for part in parts:
            #print "part #{0} = {1}".format(i,part)
            report("Launching Thread ...")
            thrd = Scrapper(i,orgid,orgname,part,destdir,linklevel)
            thrd.start()
            thrds.append(thrd)
            report("Successfully Launched Thread #{0}".format(i)) 
            i += 1
    for thrd in thrds:
        thrd.join()

        #print "Found {0} urls".format(len(urls))
        #srapurls(orgname,urls,destinationdirectory,linklevel)

# Taken from http://stackoverflow.com/users/165216/paul-mcguire
# http://stackoverflow.com/questions/1557571/how-to-get-time-of-a-python-program-execution
def secondsToStr(t):
    return "%d:%02d:%02d.%03d" % \
        reduce(lambda ll,b : divmod(ll[0],b) + ll[1:],
            [(t*1000,),1000,60,60])

def report(text):
    print "[Scrapper] {0}".format(text)

def main():
    linklevel = 5
    report("Running scraper with a link depth of {0} ...".format(linklevel))
    starttime = clock()
    runscrapper('./downloads',linklevel)
    #report("Cleaning up downloaded files.")
    #deletefiles('./downloads')
    endtime = clock()
    report("Scrapper completed in {0}.".format(secondsToStr(endtime-starttime)))
    
main()
