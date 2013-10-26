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

from urlparse import urljoin

import simplejson

import magic

from bs4 import BeautifulSoup

#import nltk

import pika

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

class Scraper(threading.Thread):

    verbose = True

    def __init__(self,destdir,address='localhost'): #self,threadnumber,orgid,orgname,orgurls,destdir,linklevel):

        threading.Thread.__init__(self)
        #self.daemon = True
        self._busy = False

        #threading.Thread.__init__(self)

        #self.threadnumber = threadnumber
        #self.orgid = orgid
        #self.orgname = orgname
        #self.orgurls = orgurls
        self.destdir = destdir
        self._addr = address
        self.scraperid = randint(1000,10000000000)
        #self.linklevel = linklevel

        #self.imp = pdfimp(self.verbose)

        # pdfimp crawler
        self._processed = []
        self._badlinks = []
        self._linkcount = 0
        self._level = -1

        # database access
        self.urls= Urls()
        self.orgs = Orgs()
        self.scrapes = Scrapes()
        self.docs = Docs()

        self._queuename = 'barkingowl'

        # setup incomming messages
        self._reqcon = pika.BlockingConnection(pika.ConnectionParameters(
                                                 host=self._addr))
        self._reqchan = self._reqcon.channel()
        self._reqchan.exchange_declare(exchange=self._queuename,
                              type='fanout')
        result = self._reqchan.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self._reqchan.queue_bind(exchange=self._queuename,
                                 queue=queue_name)
        self._reqchan.basic_consume(self._reqcallback,
                                    queue=queue_name,
                                    no_ack=True
                                   )

        # setup outgoing messages
        self._respcon = pika.BlockingConnection(pika.ConnectionParameters(
                                                  host=self._addr))
        self._respchan = self._respcon.channel()
        self._respchan.exchange_declare(exchange=self._queuename,
                                          type='fanout')

    def r(self,text):
        if self.verbose:
            print "[{0}][Scraper {1}] {2}".format(datetime.now().strftime("%Y%m%d %H:%M:%S"),self.scraperid,text)

    def _gethashs(self,urlid):
        #docs = Docs()
        hashs = self.docs.gethashs(urlid) 
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

        # do some sanitizing of the data
        doctext = doctext.encode('utf-8')

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
            txt = result.encode('utf-8','ignore')

            # TODO: clean this up, I feel like I'm doing the converstion twice ...
            # http://stackoverflow.com/a/16503222/2154772
            parser = PDFParser(fp)
            doc = PDFDocument()
            parser.set_document(doc)
            doc.set_parser(parser)
            doc.initialize()
            #print doc.info[0]['CreationDate'].resolve()

            fp.close()

            # this is wrapped in a try/catch because apparently the CreationDate
            # field can be WHATEVER YOU WANT IT TO BE ZOMG
            try:
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
            except:
                created = ""
            retVal = (created,txt,pdfhash,True)
            retstr.close()
        except Exception, e:
            self.r("ALERT: pdf error: {0}".format(e))
            retVal = (None,"","",False)
            pass
        return retVal

    def _checkmatch(self,siteurl,link):
        sitematch = True
        if ( (len(link) >= 7 and link[0:7].lower() == "http://") or
             (len(link) >= 8 and link[0:8].lower() == "https://") or
             (len(link) >= 3 and link[0:6].lower() == "ftp://") ): 
            if(link[:link.find("/",7)+1] != siteurl):
                sitematch = False
        return sitematch
        #if siteurl.split('//')[1].split('/')[0] == link.split('/')[0]:
        #else:
        #    # the clarkson, ny case ...
        #    if link[:2] == "../":
        #        retval = (True,siteurl + link[2:])
        #    else:
        #        retval = (True,siteurl + link)

        return retval

    def _getpagelinks(self,siteurl,url):
        links = []
        success,linktype = self._typelink(url,2048)
        if success == False:
            self._badlinks.append(url)
            return links,success

        sucess = True
        if linktype != "text/html":
            return links,False

        try:
            html = urllib2.urlopen(url)
            soup = BeautifulSoup(html)
            atags = soup.find_all('a', href=True)
            for tag in atags:
                if len(tag.contents) >= 1:
                    linktext = unicode(tag.string).strip()
                else:
                    linktext = ""
                rawhref = tag['href']
                match = self._checkmatch(siteurl,rawhref)
                abslink = urljoin(siteurl,rawhref)
                links.append((match,abslink,linktext))

                # there are some websites that have absolute links that go above
                # the root ... why this is I have no idea, but this is my super not
                # cool way to solve it!
                uprelparts = rawhref.split('../')
                if len(uprelparts) == 1:
                    abslink = urljoin(siteurl,rawhref)
                    links.append((match,abslink,linktext))
                elif len(uprelparts) == 2:
                    abslink = urljoin(siteurl,uprelparts[1])
                    links.append((match,abslink,linktext))
                elif len(uprelparts) == 3:
                    newhref = "../{0}".format(uprelparts[2])
                    abslink = urljoin(siteurl,newhref)
                    links.append((match,abslink,linktext))
                    abslink = urljoin(siteurl,uprelparts[2])
                else:
                    abslink = urljoin(siteurl,rawhref)
        except Exception, e:
            links = []
            sucess = False
            self.r("ERROR: An error has happened in _getpagelinks():\n\n\t{0}".format(e))
        
        self._linkcount += len(links)
        return links,True

    def _typelink(self,link,filesize):
        req = urllib2.Request(link, headers={'Range':"byte=0-{0}".format(filesize)})
        success = True
        filetype = ""
        try:
            payload = urllib2.urlopen(req,timeout=5).read(filesize)
            filetype = magic.from_buffer(payload,mime=True)
        except Exception, e:
            success = False;
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
            # regen the name if it already exists
            while self._fileexists(_filename):
                r = randint(0,10000000)
                _filename = "{0}/{1}_{2}{3}.download".format(self.destdir,urlfile,t,r)
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

    def _processpdf(self,orgid,urlid,docurl,linktext):
        #self._report("Found Document: '{0}'".format(gotlink))
        #elf.r("Saving: {0}".format(docurl))

        #
        # TODO: Take ths out
        #
        if docurl == "http://www.parmany.org/pdf/special-police/PSPD_APPLICATION_NOV04.PDF":
            return

        filename,downloaddatetime,success = self._downloadfile(docurl)
        if success == True:
            # get hash of downloaded binary, so we don't convert it more than once
            with open(filename,'rb') as fp:
                pdfhash = hashlib.md5(fp.read()).hexdigest()
            hashs = self._gethashs(urlid)
            if not pdfhash in hashs:
                #self.r("Converting Document: {0}".format(docurl))
                creationdatetime,doctext,dochash,success = self._unpdf(filename)
                if success == True:
                    doctext = re.sub(' +',' ',doctext)
                    doctext = re.sub('\n+','\n',doctext)
                    processed = True
                else:
                    processed = False
                hashs = self._gethashs(urlid)
                self.r("Saving Document:   {0}".format(docurl))
                docid = self._adddoc(orgid,docurl,filename,linktext,downloaddatetime,
                                     creationdatetime,doctext,pdfhash,urlid,processed)
            else:
                self.r("Skipping Document: {0}".format(docurl))
        else:
            self.r("Ignoring document due to error")
            pass

    def _followlinks(self,orgid,urlid,maxlevel,siteurl,links,level=0,filesize=1024):
        retlinks = []
        self._level = level
        if( level >= maxlevel ):
            #self.r("ALERT: Max link depth reached")
            pass
        else:
            level += 1
            for _link in links:
                link,linktext = _link
                
                # we need to keep track of what links we have visited at each level.  Here
                # we are adding to our array each time a new level is seen
                #print "len(self._processed) = {0}".format(self._processed)
                if len(self._processed)-1 < level:
                    self._processed.append([])

                # see if we have already processed the link at max leve, and we are at
                # maxlevel.  If that is the case, it is pointless to do the bottom of the
                # tree over and over again.  Also don't do anything if it is 404/bad link
                if (link in self._processed[level-1]) or link in self._badlinks:
                    continue

                ignored = 0
                #self.r("Getting links for '{0}'".format(link))
                allpagelinks,success = self._getpagelinks(siteurl,link)
                if success == False:
                    continue
                
                _l = urljoin(siteurl,link)
                self._processed[level-1].append(_l)
                self.r("Level: {0}, Processing {1} links for {2}".format(level,len(allpagelinks),link))
                
                # Look at the links found on the page, and add those that are within
                # the domain to 'thelinks'
                pagelinks = []
                for pagelink in allpagelinks:
                    match,link,linktext = pagelink
                    if( match == True ):
                        pagelinks.append((link,linktext))

                # Some of the links that were returned from this page might be pdfs,
                # if they are, add them to the list of pdfs to be returned 'retlinks'
                for _thelink in pagelinks:
                   thelink,linktext = _thelink
                   if not any(thelink in r for r in retlinks):
                        success,linktype = self._typelink(thelink,filesize)
                        if success == True and linktype == 'application/pdf':
                            retlinks.append((thelink,linktext))
                            self._processed[level-1].append(thelink)
                            self._processpdf(orgid,urlid,thelink,linktext)
                        else:
                            ignored += 1

                # Follow all of the link within the 'thelink' array
                gotlinks = self._followlinks(orgid=orgid,urlid=urlid,maxlevel=maxlevel,siteurl=siteurl,
                                             links=pagelinks,level=level,filesize=filesize)
                
                # go through all of the returned links and see if any of them are pdfs
                for _gotlink in gotlinks:
                    gotlink,linktext = _gotlink
                    if not any(gotlink in r for r in retlinks):
                        success,linktype = self._typelink(gotlink,filesize)
                        if success == True and linktype == 'application/pdf':
                            retlinks.append((gotlink,linktext))
                            self._processed[level-1].append(gotlink)
                            self._processpdf(orgid,urlid,gotlink,linktext)
                        else:
                            ignored += 1
                
                #self.r("Ignored Page Links: {0}/{1}".format(ignored,len(pagelinks)))
            level -= 1
        for l in links:
            if not l in self._processed:
                self._processed.append(l)
        return retlinks

    def scrapeurl(self,orgname,orgurl):
        #time.sleep(1)
        #retsuccess = True
        #self.r("Working on {0} URLs for '{1}'...".format(len(orgurls),orgname))
        #for _url in orgurls:

        urlid,orgid,url,urlname,description,createdatetime,creationuserid,linklevel = orgurl

        starttime = strftime("%Y-%m-%d %H:%M:%S")
           
        scrapeid = self._addscrape(orgid,starttime,"",False,urlid,0)

        self.r("Scraper Started at: {0}".format(starttime))
        self.r("Link Level = {0}".format(linklevel))
        self.r("Running on: '{0}'".format(urlname))
        self.r("    {0}".format(url))
        self.r("")
            
        self._processed = []
        self._badlinks = []
        links=[(url,url)]
  
        success = True
        try:
            self._followlinks(orgid=orgid,urlid=urlid,maxlevel=linklevel,siteurl=url,
                                links=links,level=0,filesize=2048)
        except Exception, e:
            self.r("ERROR: A general error happened while executing _followlinks():\n\t{0}".format(e))
            success = False

        endtime = strftime("%Y-%m-%d %H:%M:%S")
        self._updatescrape(scrapeid,orgid,starttime,endtime,success,urlid,self._linkcount)
        self._linkcount = 0
        self.r("Scraper Stopped at: {0}".format(endtime))
        self.r("")
        #return retsuccess

    def deletefile(self,filename):
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except:
            pass

    def _fixurldate(self,rawurl):
        urlid,orgid,url,urlname,description,createdatetimeiso,creationuserid,linklevel = rawurl
        createdatetime = datetime.strptime(createdatetimeiso,"%Y-%m-%d %H:%M:%S")
        returl = (urlid,orgid,url,urlname,description,createdatetime,creationuserid,linklevel)
        return returl

    def _reqcallback(self,ch,method,properties,body):
        self.r("Processing New Message ...")
        try:
            response = simplejson.loads(body)
            self.r("\nMessage Body: {0}\n".format(response))
            self.r("Command: {0}".format(response['command']))
            self.r("Dest ScraperID: {0}".format(response['scraperid']))
            self.r("Local ScraperID: {0}".format(self.scraperid))
            if response['command'] == 'url_payload' and response['scraperid'] == '{0}'.format(self.scraperid): #and self._busy == False:
                self._busy = True
                self.r("Processing URL Payload ...")
                orgname = response['orgname']
                rawurl = simplejson.loads(response['url_json'])
                url = self._fixurldate(rawurl)
                self.scrapeurl(orgname,
                               url,
                )
                self._busy = False
                #self._requesturl()
                #self.reqchan.basic_ack(delivery_tag = method.delivery_tag)
                #self.r("Done with URL!")

        except Exception, e:
           self.r("ERROR: an error happened while processing the message:\n\t{0}".format(e))
        return True 

    def _requesturl(self):
        self.r("Sending URL Request ...")
        body = {'scraperid': self.scraperid,
                'command': 'url_request'}
        jbody = simplejson.dumps(body)
        #elf._respchan.basic_publish(exchange='',
        #                       routing_key=self._queuename,
        #                       body=jbody)
        self._respchan.basic_publish(exchange=self._queuename,
                                             routing_key='', #self._queuename,
                                             body=jbody,
                                             )

    def run(self):
        self.r("Starting Barking Owl Scraper")
        self._requesturl()
        self.r("Starting Message Consuming Engine")
        self._reqchan.start_consuming()
        
