import re
import time
import magic
import urllib
import urllib2
from bs4 import BeautifulSoup
import hashlib

class pdfimp:

    _verbose = False

    _processed = []
    _pdfs = []

    def __init__(self,verbose):
        self._verbose = verbose

    def _nonascii(self,s):
        return "".join(i for i in s if ord(i)<128)

    def _report(self,text):
        if self._verbose == True:
            print "[PDFImp  ] {0}".format(text)
   
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
        _sucess,linktype = self._typelink(url,1024)
        if linktype != "text/html":
            #self._report("Ignoring on-html URL.")
            return links
        try:
            html = urllib2.urlopen(url)
            soup = BeautifulSoup(html)
            atags = soup.find_all('a', href=True)
            for tag in atags:
                if len(tag.contents) >= 1:
                    linktext = unicode(tag.string).strip()
                else:
                    linktext = ""
                match,link = self._createlink(siteurl,tag['href'])
                links.append((match,link,linktext))
        except:
            links = []
        return links
    
    def _typelink(self,link,filesize):
        req = urllib2.Request(link, headers={'Range':"byte=0-{0}".format(filesize)})
        success = True
        filetype = ""
        try:
            payload = urllib2.urlopen(req,timeout=5).read(filesize)
            filetype = magic.from_buffer(payload,mime=True)
        except:
            success = False;
        return success,filetype
    
    def _followlinks(self,maxlevel,siteurl,links,level=0,filesize=1024,verbose=False):
        retlinks = []
        if( level >= maxlevel ):
            self._report("Max depth reached.")
            pass
        else:
            level += 1
            for _link in links:
                link,linktext = _link
                if link in self._processed:
                    #print "Link already processed. '{0}'".format(link)
                    continue

                ignored = 0            
    
                self._report("Getting links for '{0}'".format(link))
                pagelinks = self._getpagelinks(siteurl,link)
                _m,_l = self._createlink(siteurl,link)
                self._processed.append(_l)
                self._report("Processing {0} links ...".format(len(pagelinks)))

                #
                # Look at the links found on the page, and add those that are within the domain to 'thelinks'
                #
                thelinks = []
                for _pagelink in pagelinks:
                    match,pagelink,linktext = _pagelink
                    if( match == True ): #and ( (level != maxlevel) or (level == maxlevel and (not pagelink in retlinks) ) ) ):
                        thelinks.append((pagelink,linktext))
                #for l in thelinks:
                #    if level >= maxlevel:
                #        self._processed.append(l)
                
                #
                # Follow all of the link within the 'thelink' array
                #
                gotlinks = self._followlinks(maxlevel=maxlevel,siteurl=siteurl,links=thelinks,level=level,filesize=filesize,verbose=verbose)
                for _gotlink in gotlinks:
                    gotlink,linktext = _gotlink
                    
                    # only process if we haven't processed it
                    if not any(gotlink in r for r in retlinks):
                        success,linktype = self._typelink(gotlink,filesize)
                        
                        # if it is a pdf, then add it to the self._pdfs array
                        if success == True and linktype == 'application/pdf': # and not gotlink in self._processed:
                            #retlinks.append((gotlink,linktext))
                            self._pdfs.append((gotlink,linktext))
                            self._processed.append(gotlink)
                            self._report("Found Document: '{0}'".format(gotlink))
                        else:
                            ignored += 1
    
                #
                # Some of the links that were returned from this page might be pdfs, if they are, add them to the list
                # of pdfs to be returned 'retlinks'
                #
                for _thelink in thelinks:
                   thelink,linktext = _thelink 
                   if not any(thelink in r for r in retlinks):
                        success,linktype = self._typelink(thelink,filesize)
                        if success == True and linktype == 'application/pdf': # and not thelink in self._processed:
                            self._pdfs.append((thelink,linktext))
                            retlinks.append((thelink,linktext))
                            self._processed.append(thelink)
                            #self._report("Added '{0}'".format(thelink))
                            self._report("Found Document: '{0}'".format(thelink))
                        else:
                            ignored += 1
    
                self._report("Ignored Links: {0}/{1}".format(ignored,len(pagelinks)))
    
            level -= 1
        for l in links:
            self._processed.append(l)
        return retlinks

    def getpdfs(self,maxlevel,siteurl,links,level=0,filesize=1024,verbose=False):
        self._followlinks(maxlevel,siteurl,links,level=0,filesize=1024,verbose=False)
        return self._pdfs
