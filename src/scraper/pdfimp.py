import re
import time
import magic
import urllib
import urllib2
from bs4 import BeautifulSoup
import hashlib

class pdfimp:

    _verbose = False

    def __init__(self):
        self._verbose = True

    def _nonascii(self,s):
        return "".join(i for i in s if ord(i)<128)

    #def _report(self,text):
        #if self._verbose == True:
            #print "[INFO   ] {0}".format(text)
    
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
                siteurlmatch = True
                if ( (len(tag['href']) >= 7 and tag['href'][0:7].lower() == "http://") or
                     (len(tag['href']) >= 8 and tag['href'][0:8].lower() == "https://") or
                     (len(tag['href']) >= 3 and tag['href'][0:6].lower() == "ftp://") ):
                    if(url[:url.find("/",7)+1] != siteurl):
                        siteurlmatch = False
                    links.append((siteurlmatch,tag['href'],linktext))
                else:
                    links.append((siteurlmatch,siteurl + tag['href'],linktext))
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
    
    def getpdfs(self,maxlevel,siteurl,links,level=0,filesize=1024,verbose=False):
        retlinks = []
        if( level >= maxlevel ):
            #self._report("Max depth reached.")
            None
        else:
            level += 1
            for _link in links:
                link,linktext = _link

                ignored = 0            
    
                #self._report("Getting links for '{0}'".format(link))
                pagelinks = self._getpagelinks(siteurl,link)
                #self._report("Found {0} links ...".format(len(pagelinks)))
                
                thelinks = []
                for _pagelink in pagelinks:
                    match,pagelink,linktext = _pagelink
                    if( match == True ): #and ( (level != maxlevel) or (level == maxlevel and (not pagelink in retlinks) ) ) ):
                        thelinks.append((pagelink,linktext))
                
                gotlinks = self.getpdfs(maxlevel=maxlevel,siteurl=siteurl,links=thelinks,level=level,filesize=filesize,verbose=verbose)
                for _gotlink in gotlinks:
                    gotlink,linktext = _gotlink
                    if not any(gotlink in r for r in retlinks):
                        success,linktype = self._typelink(gotlink,filesize)
                        if success == True and linktype == 'application/pdf':
                            retlinks.append((gotlink,linktext))
                            #self._report("Added '{0}'".format(gotlink))
                        else:
                            ignored += 1
    
                for _thelink in thelinks:
                   thelink,linktext = _thelink 
                   if not any(thelink in r for r in retlinks):
                        success,linktype = self._typelink(thelink,filesize)
                        if success == True and linktype == 'application/pdf':
                            retlinks.append((thelink,linktext))
                            #self._report("Added '{0}'".format(thelink))
                        else:
                            ignored += 1
    
                #self._report("Ignored Links: {0}/{1}".format(ignored,len(pagelinks)))
    
            level -= 1
        return retlinks

