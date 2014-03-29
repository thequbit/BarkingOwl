import threading
import urllib
import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import magic
from time import strftime
import sys

class Scraper(threading.Thread):

    def __init__(self,uid=str(uuid.uuid4()),DEBUG=False):
        """
        __init__() constructor setups the threading enviornment and status variables.
        """

        threading.Thread.__init__(self)
        
        self._stop = threading.Event()
        self.started = False
        self.uid = uid
        self.interval = 1

        self.status = {}
        self.status['busy'] = False
        self.status['processed'] = []
        self.status['badlinks'] = []
        self.status['linkcount'] = 0
        self.status['level'] = -1
        self.status['urldata'] = {}

        self.DEBUG = DEBUG

        # start a timer to see if we should be exiting
        threading.Timer(self.interval,self._checkshutdown).start()

        self.finishedCallback = None
        self.startedCallBack = None
        self.broadcastDocCallback = None

        if self.DEBUG:
            print "Scraper INIT successful."

    def setCallbacks(self,finishedCallback=None,startedCallback=None,broadcastDocCallback=None):
        """
        setCallbacks() sets the async call backs that should be called when an event happens.  There are three
        different events that are called:

            Finished - The scraper has finished scraping the target URL
            Started - The scraper has become busy and has sctarted scraping the target URL
            Document Found - The scraper has found a document

        """
        self.finishedCallback = finishedCallback
        self.startedCallback = startedCallback
        self.broadcastDocCallback = broadcastDocCallback 

    def setFinishedCallback(self,callback):
        """
        setFinishedCallback() defineds the function that should be called when the finished state occures.
        """
        self.finishedCallback = callback

    def setStartedCallback(self,callback):
        """
        setStartedCallback() defineds the function that should be called when the started state occures.
        """
        self.startedCallback = callback

    def setBroadcastDocCallback(self,callback):
        """
        setBroadcastDocCallback() defineds the function that should be called when the broadcast doc state occures.
        """
        self.broadcastDocCallback = callback

    def seturldata(self,urldata):
        """
        seturldata() sets the url information for the scraper.  This information should be presented in the following
        dictionary format:

            url = {
                'targeturl': targeturl, # the root url to be scraped
                'title': title, # a title for the URL
                'description': descritpion, # a description of the url
                'maxlinklevel': maxlinklevel, # the max link level for the scraper to follow to
                'creationdatetime': creationdatetime, # ISO creation date and time
                'doctype': doctype, # the text for the magic lib to look for (ex. 'application/pdf')
                'frequency': frequency, # the frequency in minutes the URL should be scraped
                'allowdomains': [], # a list of allowable domains for the scraper to follow
            }
    
        If the above fields are not included, then the scraper will not work as expected, but may not throw an error.

        Note: Any additional fields can be included, and will be include with and pass along when a document is found.
        """
        self.status['urldata'] = urldata

    def stop(self):
        """
        stop() begins the steps needed to stop the scraper.

        Note: it can be several seconds before the scraper actually stops.
        """
        self._stop.set()

    def stopped(self):
        """
        stopped() returns the status of the stopped event.  If true, the scraper is working on stopping.
        """
        return self._stop.isSet()

    def _checkshutdown(self):
        #
        # See if we need to stop ourselves
        #
        if self.stopped():
            if self.DEBUG:
                print "[{0}] Exiting.".format(self.uid)
            self.stop()
            raise Exception("Scraper Stopped - Scraper Exiting.")
        else:
            threading.Timer(self.interval, self._checkshutdown).start()

    def run(self):
        """
        run() this is the threading subsystem entry point that is called when Scraper.start() is called. 
        This function starts the scraper.
        """
        if self.DEBUG:
            print "Starting Scraper ..."
        self.started = True

    def reset(self):
        """
        reset() resets the state of the scraper.  This should not be called unless the scraper is stopped.
        """

        # reset globals
        self.status['processed'] = []
        self.status['badlinks'] = []
        self.status['linkcount'] = 0
        self.status['level'] = -1
        self.status['bandwidth'] = 0
        self.status['busy'] = False
        self.status['urldata'] = {}

        if self.DEBUG:
            print "Scraper data reset successfully."

    def begin(self):
        """
        begin() should be used as the entry point of the scraper once it has been configured using seturldata().
        This funciton resets the state of the scraper, and then begins traversing the target url based on the 
        rulls passed along with it.
        """

        self.broadcaststart()
        self.status['busy'] = True

        # reset globals
        self.status['processed'] = []
        self.status['badlinks'] = []
        self.status['linkcount'] = 0
        self.status['level'] = -1
        self.status['bandwidth'] = 0

        if self.DEBUG:
            print "Starting Scraper on '{0}' ...".format(self.status['urldata']['targeturl'])

        links = []
        links.append((self.status['urldata']['targeturl'],'<root>'))
        try:
            # begin following links (note: blocking)
            self.followlinks(links=links,
                             level=0)
            self.broadcastfinish()
        except:
            if self.DEBUG:
                print "Error: {0}".format(sys.exc_info()[0])
            raise Exception('Scraper Error - Scraper Exiting.')
        
        self.status['busy'] = False
        
    def broadcastfinish(self):
        """
        broadcastfinish() calls the async scraper finished call back with status information within its payload.
        """
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'processed': self.status['processed'],
            'badlinks': self.status['badlinks'],
            'linkcount': self.status['linkcount'],
            'urldata': self.status['urldata'],
            'bandwidth': self.status['bandwidth'],
            'startdatetime': str(isodatetime)
        }
        payload = {
            'command': 'scraper_finished',
            'sourceid': self.uid,
            'destinationid': 'broadcast',
            'message': packet
        }
        if self.finishedCallback != None:
            self.finishedCallback(payload)

    def broadcaststart(self):
        """
        broadcaststart() calls the scraper start async call back with status information within its payload.
        """
        if self.DEBUG:
            print "Scraper Starting."
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'urldata': self.status['urldata'],
            'startdatetime': str(isodatetime)
        }
        payload = {
            'command': 'scraper_start',
            'sourceid': self.uid,
            'destinationid': 'broadcast',
            'message': packet
        }
        if self.startedCallback != None:
            self.startedCallback(payload)

    def broadcastdoc(self,docurl,linktext):
        """
        broadcastdoc() calls the scraper document found async call abck with status and document information within
        its payload.
        """
        if self.DEBUG:
            print "Doc Found: '{0}'.".format(docurl)
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'docurl': docurl,
            'linktext': linktext,
            'urldata': self.status['urldata'],
            'scrapedatetime': str(isodatetime)
        }
        payload = {
            'command': 'found_doc',
            'sourceid': self.uid,
            'destinationid': 'broadcast',
            'message': packet
        }
        if self.broadcastDocCallback != None:
            self.broadcastDocCallback(payload)

    def typelink(self,link,filesize=1024):
        """
        typelink() will download a link, and then deturmine it's file type using magic numbers.
        """
        if self.stopped():
            if self.DEBUG:
                print 'typelink() saw the stopped flag - exiting scraper.'
            self.reset()
            raise Exception('Scraper thread stopped.')

        req = urllib2.Request(link, headers={'Range':"byte=0-{0}".format(filesize)})
        success = True
        filetype = ""
        try:
            #try:
            payload = urllib2.urlopen(req,timeout=5).read(filesize)
            #except Exception, e:
            #    if self.DEBUG:
            #        print "typelink(): an error occured: {0}".format(str(e))
            # record bandwidth used
            self.status['bandwidth'] += filesize
            filetype = magic.from_buffer(payload,mime=True)
        except Exception, e:
            success = False;
        return success,filetype

    def checkmatch(self,siteurl,link):
        """
        checkmatch() is used to derumine of a link is linking to the parent domain or another domain.
        """
        sitematch = True
        if ( (len(link) >= 7 and link[0:7].lower() == "http://") or
             (len(link) >= 8 and link[0:8].lower() == "https://") or
             (len(link) >= 3 and link[0:6].lower() == "ftp://") ): 
            if(link[:link.find("/",7)+1] != siteurl):
                sitematch = False
        return sitematch

    def getpagelinks(self,siteurl,url):
        """
        getpagelinks() will return a list of all of the link on a html page that is passed.
        """
        links = []
        success,linktype = self.typelink(url)
        if success == False:
            self.status['badlinks'].append(url)
            return success,links
        sucess = True
        if linktype != "text/html":
            return False,links
        try:
            #try:
            html = urllib2.urlopen(url)
            #except Exception, e:
            #    print "getpagelinks(): urllib2 error: '{0}'".format(str(e))
            # record bandwidth used
            self.status['bandwidth'] += len(str(html))
            soup = BeautifulSoup(html)
            atags = soup.find_all('a', href=True)
            for tag in atags:
                if len(tag.contents) >= 1:
                    linktext = unicode(tag.string).strip()
                else:
                    linktext = ""
                rawhref = tag['href']
                match = self.checkmatch(siteurl,rawhref)
                abslink = urljoin(siteurl,rawhref)
                links.append((match,abslink,linktext))
                
                # there are some websites that have absolute links that go above
                # the root ... why this is I have no idea, but this is how i'm
                # solving it
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
                    links.append((match,abslink,linktext))
                else:
                    abslink = urljoin(siteurl,rawhref)
                    links.append((match,abslink,linktext))
        except Exception, e:
            links = []
            sucess = False
            #if self.DEBUG:
            #    print "An error occurred in getpagelinks(): {0}".format(str(e))
        self.status['linkcount'] += len(links)
        return success,links

    def followlinks(self,links,level=0):
        """
        followlinks() is the heart of the Barking Owl Scraper.  It follows links to a specified link level,
        reporting if any of those links are documents that it should be identifying.  This function is a
        recursive function and can run for a very long time if the link level is not defined appropreately.
        """
        retlinks = []
        if( level >= self.status['urldata']['maxlinklevel'] ):
            # made it to bottom link level, no need to continue
            pass
        else:
            level += 1
            if self.DEBUG:
                print "Working on {0} links ...".format(len(links))
                #print "Processing Links: {0}".format(links)
            for _link in links:
                link,linktext = _link

                #if self.DEBUG:
                #    print "Working on '{0}'".format(link)

                # we need to keep track of what links we have visited at each 
                # level.  Here we are adding to our array each time a new level 
                # is seen
                if len(self.status['processed'])-1 < level:
                    self.status['processed'].append([])

                # see if we have already processed the link at max level, and we
                # are at maxlevel.  If that is the case, it is pointless to do the 
                # bottom of the tree over and over again.  Also don't do anything 
                # if it is 404/bad link
                if (link in self.status['processed'][level-1]) or link in self.status['badlinks']:
                    continue

                # get all of the links from the page
                ignored = 0
                success,allpagelinks = self.getpagelinks(self.status['urldata']['targeturl'],link)
                #if self.DEBUG:
                #    print "Page '{0}' has {1} links on it.".format(link,len(allpagelinks))

                if success == False:
                    continue

                # sanitize the url link, and save it to our list of processed links
                _l = urljoin(self.status['urldata']['targeturl'],link)
                self.status['processed'][level-1].append(_l)

                # Look at the links found on the page, and add those that are within
                # the domain to 'thelinks'
                pagelinks = []
                for pagelink in allpagelinks:
                    match,link,linktext = pagelink
                    if( match == True ):
                        pagelinks.append((link,linktext))

                #if self.DEBUG:
                #    print "Number of Page Links: {0}.".format(len(pagelinks))

                # Some of the links that were returned from this page might be pdfs,
                # if they are, add them to the list of pdfs to be returned 'retlinks'
                for _thelink in pagelinks:
                   thelink,linktext = _thelink
                   if not any(thelink in r for r in retlinks) and not any(thelink in r for r in self.status['processed']):
                       success,linktype = self.typelink(thelink)
                       if success == True:
                           self.status['processed'][level-1].append(thelink)
                           if linktype == self.status['urldata']['doctype']:
                               retlinks.append((thelink,linktext))
                               
                               #broadcast the doc to the bus!
                               self.broadcastdoc(thelink,linktext)
                       else:
                           ignored += 1

                # Follow all of the link within the 'thelink' array
                gotlinks = self.followlinks(links=pagelinks,level=level)

                # go through all of the returned links and see if any of them are docs
                for _gotlink in gotlinks:
                    gotlink,linktext = _gotlink
                    if not any(thelink in r for r in retlinks) and not any(thelink in r for r in self.status['processed']):
                       success,linktype = self.typelink(gotlink)
                       if success == True:
                           self.status['processed'][level-1].append(gotlink)
                           if linktype == self.status['urldata']['doctype']:
                               retlinks.append((gotlink,linktext))
                               
                               #broadcast the doc to the bus!
                               self.broadcastdoc(gotlink,linktext)
                       else:
                           ignored += 1
            level -= 1

        for l in links:
            if not l in self.status['processed']:
                self.status['processed'].append(l)

        return retlinks

