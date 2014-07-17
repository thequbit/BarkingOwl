import threading
import urllib
import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import magic
from time import strftime
import sys
import urlparse
import uuid
import json

_DEBUG = True

def log(entry):
    if _DEBUG:
        print entry

def type_link(link, file_size=1024, sleep_time=2):
    """ typelink() will download a link, and then deturmine it's file type using magic numbers.
    """

    log( "Attempting to type link: '{0}' (using filesize: {1})".format(link,file_size) )

    req = urllib2.Request(link, headers={'Range':"byte=0-{0}".format(file_size)})
    success = False
    file_type = ""
    try:

        # try and download the file five times ( in case the site is being fussy )
        error_count = 0
        while(error_count < 5):
            try:
                payload = urllib2.urlopen(req,timeout=5).read(file_size)
                log( "Successfully downloaded the first {0} bytes of '{1}'.".format(fil_esize, link) )
                break
            except Exception, e:
                log( "Error within typelink while trying to download {0} bytes from URL:\n\t{1}\n".format(link,str(e)) )
                if str(e) != 'time out':
                    raise Exception(e)
                else:
                    error_count += 1
                    time.sleep(sleep_time)
        # type file using libmagic
        file_type = magic.from_buffer(payload, mime=True)
        success = True
        
    except Exception, e:
        log( "An error has occured in the typelink() function:\n\tURL: {0}\n\tError: {1}".format(link,str(e)) )
            
    return success, file_type

def check_match(domain_url, link, allowed_domains):
    """ check_match() is used to derumine of a link is linking to the parent domain or another domain.
    """
    
    site_match = True
    url_data = urlparse.urlparse(link)

    log( "check_match(): urlparse results:" )
    log( url_data )

    if ( (len(link) >= 7 and link[0:7].lower() == "http://") or
         (len(link) >= 8 and link[0:8].lower() == "https://") or
         (len(link) >= 3 and link[0:6].lower() == "ftp://") ): 
        urlA = "{0}://{1}".format(url_data.scheme, url_data.netloc)
        urlB = "{0}://{1}/".format(url_data.scheme, url_data.netloc)
        if(urlA != domain_url and urlB != domain_url):
            if not urlA in allowed_domains and not urlB in allowed_domains:
                site_match = False

    log( "Comparing domain_url='{0}', netloc='{1}', link='{2}', with sitematch='{3}'.".format(siteurl,urldata.netloc,link,sitematch) )

    return site_match

def sanity_check_url(siteurl, rawhref):

    # there are some websites that have absolute links that go above
    # the root ... why this is I have no idea, but this is how i'm
    # solving it
    links = []
    uprelparts = rawhref.split('../')
    if len(uprelparts) == 1:
        abslink = urljoin(siteurl,rawhref)
        links.append(abslink)
    
    elif len(uprelparts) == 2:
        abslink = urljoin(siteurl,uprelparts[1])
        links.append(abslink)
    
    elif len(uprelparts) == 3:
        newhref = "../{0}".format(uprelparts[2])

        abslink = urljoin(siteurl,newhref)
        links.append(abslink)

        abslink = urljoin(siteurl,uprelparts[2])
        links.append(abslink)
    
    else:
        abslink = urljoin(siteurl,rawhref)
        links.append(abslink)

    return links

def get_page_links(domain_url, url, file_size=1024, sleep_time=2):
    """ getpagelinks() will return a list of all of the link on a html page that is passed.
    """

    log( "Getting page links for '{0}' ...".format(url) )

    success = False
    links = []
    document_length = 0
    bad_link = False
    try:
        type_link_success, file_type = type_link(url, file_size, sleep_time)
        if success == False:
            bad_link = True
            log( "Bad link found. ( {0} )".format(url) )
            raise Exception( 'Failed to type link.' );
        
        if linktype != "text/html":
            log( "Link is not of type text/html." )
            raise Exception( 'Link is not of type text/html' )
            
        try:
        
            try:
                html = urllib2.urlopen(url)
            except Exception, e:
                err_text = "getpagelinks(): urllib2 error: '{0}'".format(str(e))
                log( err_text )
                raise Exception( err_text )
            
            # record bandwidth used
            document_length = len(str(html))
            
            # process document DOM
            soup = BeautifulSoup(html)
            tagtypes = [
                ('a','href'),
                ('embed','src'),
                ('iframe','src'),
            ]
            for tagtype,verb in tagtypes:
                tags = soup.find_all(tagtype)
                for tag in tags:
                
                    if len(tag.contents) >= 1:
                        linktext = unicode(tag.string).strip()
                    else:
                        linktext = ""

                    try:
                        rawhref = tag[verb]
                    except:
                        log( "tag ('{0}') didn't have verb ('{1}'), ignoring.".format(tag,verb) )
                        continue

                    match = check_match(domain_url, rawhref)
                    abslink = urljoin(domain_url,rawhref)
                    links.append((match,abslink,linktext))
        
                    abslinks = self.sanity_check_url(domain_url,rawhref)
                    for l in abslinks:
                        links.append((match,l,linktext))

        except Exception, e:
            links = []
            sucess = False
            log( "An error occurred in getpagelinks():\n\tURL: {0}\n\tError: {1}".format(url,str(e)) )
        self.status['linkcount'] += len(links)
    except:
        pass
    
    log( "Found {0} links from URL: '{1}'.".format(len(links),url) )

    return success, links, document_length, bad_link


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
        self.status['bandwidth'] = 0
        self.status['ignoredcount'] = 0

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
        
        Note: Setting the URL data will reset the scraper.

        """
        
        if self.DEBUG:
            print "URL data payload: {0}".format(self.status['urldata'])


        # error check all the things

        if 'targeturl' not in urldata or urldata['targeturl'] == '':
            if self.DEBUG:
                print "Unassigned or invalid targeturl.  Check URL dictionary."
            raise Exception('Unassigned or invalid Target URL.')

        if 'doctype' not in urldata or urldata['doctype'] == '':
            if self.DEBUG:
                print "Unassigned or invalid doctype.  Check URL dictionary."
            raise Exception('Unassigned or invalid Document Type.');
        

        # set defaults for not found keys in dict

        if 'title' not in urldata:
            if self.DEBUG:
                print "'title' not found in URL dictionary, setting to targeturl."
            urldata['title'] = urldata['targeturl']
        
        if 'description' not in urldata:
            if self.DEBUG:
                print "'description' not found in URL dictionary, setting to ''"
            urldata['description'] = ''

        if 'maxlinklevel' not in urldata:
            if self.DEBUG:
                print "'maxlinklevel' not found in URL dictionary, setting to 1"
            urldata['maxlinklevel'] = 1

        if 'creationdatetiem' not in urldata:
            now = strftime("%Y-%m-%d %H:%M:%S") 
            if self.DEBUG:
                print "'creationdatetime' not found in URL dictionary, setting to '{0}'".format(now)
            urlddata['creationdatetime'] = now

        if 'frequency' not in urldata:
            if self.DEBUG:
                print "'frequency' not found in URL dictionary, setting to 24 hours"
            urldata['frequency'] = 1440 # 24 hours in minutes

        if 'allowdomains' not in urldata:
            if self.DEBUG:
                print "'allowdomains' not found in URL dictionary, setting to []"
            urldata['allowdomains'] = []
        

        # reset the scraper

        self.reset()


        # set the url data local
    
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

            if self.DEBUG:
                isodatetime = strftime("%Y-%m-%d %H:%M:%S")
                packet = {
                    'processed': self.status['processed'],
                    'badlinks': self.status['badlinks'],
                    'linkcount': self.status['linkcount'],
                    'ignoredcount': self.status['ignoredcount'],
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

                with open('finishedpayload.json','w') as f:
                    f.write(json.dumps(payload))
                    f.flush()
             

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
        self.status['ignoredcount'] = 0

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
        self.status['ignoredcount'] = 0

        if self.DEBUG:
            print "Starting Scraper on '{0}' ...".format(self.status['urldata']['targeturl'])

        links = []
        links.append((self.status['urldata']['targeturl'],'<root>'))
        try:
            # begin following links (note: blocking)
            self.followlinks(links=links,
                             level=0)
            self.broadcastfinish()
        except Exception, e:
            if self.DEBUG:
                print "Error: {0}, {1}".format(str(e),sys.exc_info()[0])
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
            'ignoredcount': self.status['ignoredcount'],
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

        if self.DEBUG:
            print "Calling 'finished' callback function ..."

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

        if self.DEBUG:
            print "Calling 'starting' callback function ..."

        if self.startedCallback != None:
            self.startedCallback(payload)

    def broadcastdoc(self,docurl,linktext):
        """ broadcastdoc() calls the scraper document found async call abck with status and document information within
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

        if self.DEBUG:
            print "Calling 'found document' callback function ..."

        if self.broadcastDocCallback != None:
            self.broadcastDocCallback(payload)

    def followlinks(self,links,level=0):
        """ followlinks() is the heart of the BarkingOwl Scraper.  It follows links to a specified link level,
            reporting if any of those links are documents that it should be identifying.  This function is a
            recursive function and can run for a very long time if the link level is not defined appropreately.
        """

        if self.DEBUG:
            print "Folloing {0} Links on level {1} ...".format(len(links),level)

        retlinks = []
        if( level >= self.status['urldata']['maxlinklevel'] ):
            # made it to bottom link level, no need to continue
            pass
        else:
            level += 1
            log( "Working on {0} links ...".format(len(links)) )
                #print "Processing Links: {0}".format(links)

            # we need to keep track of what links we have visited at each
            # level.  Here we are adding to our array each time a new level
            # is seen
            if len(self.status['processed'])-1 < level:
                log( "Current Level ({0}) does not exist within processed link list, adding.".format(level) )
                self.status['processed'].append([])

            for link,linktext in links:
                #link,linktext = _link

                # see if we have already processed the link at max level, and we
                # are at maxlevel.  If that is the case, it is pointless to do the 
                # bottom of the tree over and over again.  Also don't do anything 
                # if it is 404/bad link
                
                #if (link in self.status['processed'][level-1]) or link in self.status['badlinks']:
                l = level
                exists = False
                while(l != -1):
                    if link in self.status['processed'][l-1]:
                        exists = True
                        break
                    l=l-1

                #if any(link in r for r in self.status['processed']) or link in self.status['badlinks']:
                if exists or link in self.status['badlinks']:
                    if self.DEBUG:
                        print "Link already processed, skipping. ({0})".format(link)
                    self.status['ignoredcount']+=1

                # get all of the links from the page
                ignored = 0
                success,all_page_links = self.getpagelinks(self.status['urldata']['targeturl'],link)
                #if self.DEBUG:
                #    print "Page '{0}' has {1} links on it.".format(link,len(all_page_links))

                if success == False:
                    log( "Unable to get page links from link, skipping. ({0})".format(link) )
                    continue

                # sanitize the url link, and save it to our list of processed links
                log( "Adding '{0}' to the processed list.".format(_l) )
                _l = urljoin(self.status['urldata']['targeturl'],link)
                self.status['processed'][level-1].append(_l)

                # Look at the links found on the page, and add those that are within
                # the allowed domains to pagelinks to process
                pagelinks = []
                for match,link,linktext in all_page_links:
                    # match,link,linktext = pagelink
                    if( match == True ):
                        pagelinks.append((link,linktext))
                        
                # Some of the links that were returned from this page might be docs we are 
                # interested in if they are, add them to the list of pdfs to be returned 'retlinks'
                for thelink,linktext in pagelinks:
                    #thelink,linktext = _thelink
                    if not any(thelink in r for r in retlinks) and not any(thelink in r for r in self.status['processed']):
                        success,linktype = self.typelink(thelink)
                        if success == True:

                            if self.DEBUG:
                                print "Link successfully typed as '{0}'.".format(linktype)

                            # add the link to the list of processed links
                            if thelink not in self.status['processed'][level-1]:
                                self.status['processed'][level-1].append(thelink)
                            else:
                                self.status['ignoredcount']+=1

                            # if the link is of the type we are looking for, add it to the 
                            # list of docs to return
                            if linktype == self.status['urldata']['doctype']:
                                retlinks.append((thelink,linktext))
                               
                                #broadcast the doc to the bus!
                                self.broadcastdoc(thelink,linktext)
                        else:
                            log( "WARNING: Link unsuccessfully typed! ({0})".format(thelink) )
                            ignored += 1
                    else:
                        log( "Link already processed, skipping. ({0})".format(thelink) )

                # Follow all of the valid links on the page, and find all of the docs.
                gotlinks = self.followlinks(links=pagelinks,level=level)

                for glink in gotlinks:
                    l,t = glink
                    if (l,t) not in retlinks:
                        retlinks.append((l,t))

                log( "Done processing url: '{0}'".format(link) )

            level -= 1

        #for l in links:
        #    if not l in self.status['processed'][level-1]:
        #        thelink, thelinktext = l
        #        self.status['processed'][level-1].append(thelink)

        return retlinks

