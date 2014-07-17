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
    """ type_link() will download a link, and then deturmine it's file type using magic numbers.
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
                log( "Successfully downloaded the first {0} bytes of '{1}'.".format(file_size, link) )
                break
            except Exception, e:
                log( "Error within type_link while trying to download {0} bytes from URL:\n\t{1}\n".format(link,str(e)) )
                if str(e) != 'time out':
                    raise Exception(e)
                else:
                    error_count += 1
                    time.sleep(sleep_time)
        # type file using libmagic
        file_type = magic.from_buffer(payload, mime=True)
        success = True
        
    except Exception, e:
        log( "An error has occured in the type_link() function:\n\tURL: {0}\n\tError: {1}".format(link,str(e)) )
            
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

    log( "Comparing domain_url='{0}', netloc='{1}', link='{2}', with sitematch='{3}'.".format(domain_url, url_data.netloc, link, site_match) )

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

def get_page_links(domain_url, url, allowed_domains, file_size=1024, sleep_time=2):
    """ get_page_links() will return a list of all of the link on a html page that is passed.
    """

    log( "Getting page links for '{0}' ...".format(url) )

    success = False
    links = []
    document_length = 0
    bad_link = False
    try:
        type_link_success, link_type = type_link(url, file_size, sleep_time)
        if type_link_success == False:
            bad_link = True
            log( "Bad link found. ( {0} )".format(url) )
            raise Exception( 'Failed to type link.' );
        
        if link_type != "text/html":
            log( "Link is not of type text/html." )
            raise Exception( 'Link is not of type text/html' )
            
        try:
        
            try:
                html = urllib2.urlopen(url)
            except Exception, e:
                err_text = "get_page_links(): urllib2 error: '{0}'".format(str(e))
                log( err_text )
                raise Exception( err_text )
            
            # record bandwidth used
            document_length = len(str(html))
            
            # process document DOM
            soup = BeautifulSoup(html)
            tag_types = [
                ('a','href'),
                ('embed','src'),
                ('iframe','src'),
            ]
            for tag_type,verb in tag_types:
                tags = soup.find_all(tag_type)
                for tag in tags:
                
                    if len(tag.contents) >= 1:
                        link_text = unicode(tag.string).strip()
                    else:
                        link_text = ""

                    try:
                        raw_href = tag[verb]
                    except:
                        log( "tag ('{0}') didn't have verb ('{1}'), ignoring.".format(tag,verb) )
                        continue

                    match = check_match(domain_url, raw_href, allowed_domains)
                    abs_link = urljoin(domain_url, raw_href)
                    links.append((match,abs_link,link_text))
        
                    abs_links = sanity_check_url(domain_url, raw_href)
                    for l in abs_links:
                        links.append((match,l,link_text))

            success = True

        except Exception, e:
            links = []
            sucess = False
            log( "An error occurred in get_page_links():\n\tURL: {0}\n\tError: {1}".format(url,str(e)) )
    except Exception, e:
        log( 'Exception: {0}'.format(str(e)) )
        pass
    
    log( "Found {0} links from URL: '{1}'.".format(len(links),url) )

    return success, links, document_length, bad_link


class Scraper(): #threading.Thread):

    def __init__(self,uid=str(uuid.uuid4())): #,DEBUG=False):
        """
        __init__() constructor setups the threading enviornment and status variables.
        """

        #threading.Thread.__init__(self)
        
        #self._stop = threading.Event()
        self.started = False
        self.uid = uid
        self.interval = 1

        self.status = {}
        self.status['busy'] = False
        self.status['processed'] = []
        self.status['bad_links'] = []
        self.status['linkcount'] = 0
        self.status['level'] = -1
        self.status['url_data'] = {}
        self.status['bandwidth'] = 0
        self.status['ignored_count'] = 0

        #self.DEBUG = DEBUG

        # start a timer to see if we should be exiting
        #threading.Timer(self.interval,self._checkshutdown).start()

        self.finished_callback = None
        self.started_callback = None
        self.broadcast_document_found_callback = None

        log( "Scraper INIT successful." )

    def setCallbacks(self, finished_callback=None,
            started_callback=None, broadcast_document_found_callback=None):
        """
        setCallbacks() sets the async call backs that should be called when an event happens.  There are three
        different events that are called:

            Finished - The scraper has finished scraping the target URL
            Started - The scraper has become busy and has sctarted scraping the target URL
            Document Found - The scraper has found a document

        """
        self.finished_callback = finished_callback
        self.started_callback = started_callback
        self.broadcast_document_callback = broadcast_document_found_callback 

    def setFinishedCallback(self, finished_callback):
        """
        setFinishedCallback() defineds the function that should be called when the finished state occures.
        """
        self.finished_callback = finished_callback

    def setStartedCallback(self, started_callback):
        """
        setStartedCallback() defineds the function that should be called when the started state occures.
        """
        self.started_allback = started_callback

    def setBroadcast_document_callback(self, broadcast_document_found_callback):
        """
        setBroadcastDocCallback() defineds the function that should be called when the broadcast doc state occures.
        """
        self.broadcast_document_found_callback = broadcast_document_found_callback

    def set_url_data(self,url_data):
        """
        seturl_data() sets the url information for the scraper.  This information should be presented in the following
        dictionary format:

            url = {
                'target_url': target_url, # the root url to be scraped
                'title': title, # a title for the URL
                'description': descritpion, # a description of the url
                'max_link_level': max_link_level, # the max link level for the scraper to follow to
                'creationdatetime': creationdatetime, # ISO creation date and time
                'doctype': doctype, # the text for the magic lib to look for (ex. 'application/pdf')
                'frequency': frequency, # the frequency in minutes the URL should be scraped
                'allowdomains': [], # a list of allowable domains for the scraper to follow
            }
    
        If the above fields are not included, then the scraper will not work as expected, but may not throw an error.

        Note: Any additional fields can be included, and will be include with and pass along when a document is found.
        
        Note: Setting the URL data will reset the scraper.

        """

        self.reset()
        
        log( "URL data payload: {0}".format(self.status['url_data']) )

        # error check all the things

        if 'target_url' not in url_data or url_data['target_url'] == '':
            log( "Unassigned or invalid target_url.  Check URL dictionary." )
            raise Exception('Unassigned or invalid Target URL.')

        if 'doc_type' not in url_data or url_data['doc_type'] == '':
            log( "Unassigned or invalid doc_type.  Check URL dictionary." )
            raise Exception('Unassigned or invalid Document Type.');
        

        # set defaults for not found keys in dict

        if 'title' not in url_data:
            log( "'title' not found in URL dictionary, setting to target_url." )
            url_data['title'] = url_data['target_url']
        
        if 'description' not in url_data:
            log( "'description' not found in URL dictionary, setting to ''" )
            url_data['description'] = ''

        if 'max_link_level' not in url_data:
            log( "'max_link_level' not found in URL dictionary, setting to 1" )
            url_data['max_link_level'] = 1

        if 'creation_datetiem' not in url_data:
            now = strftime("%Y-%m-%d %H:%M:%S") 
            log( "'creationdatetime' not found in URL dictionary, setting to '{0}'".format(now) )
            url_data['creation_datetime'] = now

        if 'frequency' not in url_data:
            ( "'frequency' not found in URL dictionary, setting to 24 hours" )
            url_data['frequency'] = 1440 # 24 hours in minutes

        if 'allowed_domains' not in url_data:
            log( "'allowed_domains' not found in URL dictionary, setting to []" )
            url_data['allowed_domains'] = []
        

        # set the url data local
        self.status['url_data'] = url_data

    def reset(self):
        """
        reset() resets the state of the scraper.  This should not be called unless the scraper is stopped.
        """

        # reset globals
        self.status['processed'] = []
        self.status['bad_links'] = []
        self.status['linkcount'] = 0
        self.status['level'] = -1
        self.status['bandwidth'] = 0
        self.status['busy'] = False
        self.status['url_data'] = {}
        self.status['ignored_count'] = 0

        log( "Scraper data reset successfully." )

    def broadcast_finished(self):
        """
        broadcastfinish() calls the async scraper finished call back with status information within its payload.
        """
        
        log( "Scraper Finished." )

        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'processed': self.status['processed'],
            'bad_links': self.status['bad_links'],
            'linkcount': self.status['linkcount'],
            'ignored_count': self.status['ignored_count'],
            'url_data': self.status['url_data'],
            'bandwidth': self.status['bandwidth'],
            'startdatetime': str(isodatetime)
        }
        payload = {
            'command': 'scraper_finished',
            'sourceid': self.uid,
            'destinationid': 'broadcast',
            'message': packet
        }

        log( "Calling 'finished' callback function ..." )

        if self.finished_callback != None:
            self.finished_callback(payload)


    def broadcast_start(self):
        """
        broadcaststart() calls the scraper start async call back with status information within its payload.
        """
        
        log( "Scraper Starting." )
        
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'url_data': self.status['url_data'],
            'startdatetime': str(isodatetime)
        }
        payload = {
            'command': 'scraper_start',
            'sourceid': self.uid,
            'destinationid': 'broadcast',
            'message': packet
        }

        log( "Calling 'starting' callback function ..." )

        if self.started_callback != None:
            self.started_callback(payload)

    def broadcast_document(self,doc_url,link_text):
        """ broadcastdoc() calls the scraper document found async call abck with status and document information within
            its payload.
        """

        log( "Doc Found: '{0}'.".format(doc_url) )

        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'docurl': doc_url,
            'link_text': link_text,
            'url_data': self.status['url_data'],
            'scrapedatetime': str(isodatetime)
        }
        payload = {
            'command': 'found_doc',
            'sourceid': self.uid,
            'destinationid': 'broadcast',
            'message': packet
        }

        log( "Calling 'found document' callback function ..." )

        if self.broadcast_document_found_callback != None:
            self.broadcastDocCallback(payload)

    def find_docs(self, links):

        self.status['busy'] = True
        
        self.broadcast_start()

        links = []
        links.append( (self.status['url_data']['target_url'],'<root>') )
        docs = self.follow_links(links)

        self.broadcast_finished()

        return docs

    def follow_links(self, links, level=0):
        """ followlinks() is the heart of the BarkingOwl Scraper.  It follows links to a specified link level,
            reporting if any of those links are documents that it should be identifying.  This function is a
            recursive function and can run for a very long time if the link level is not defined appropreately.
        """

        log( "Folloing {0} Links on level {1} ...".format(len(links),level) )

        ret_links = []
        if( level >= self.status['url_data']['max_link_level'] ):
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

            for current_link, link_text in links:
                #link,link_text = _link

                # see if we have already processed the link at max level, and we
                # are at maxlevel.  If that is the case, it is pointless to do the 
                # bottom of the tree over and over again.  Also don't do anything 
                # if it is 404/bad link
                
                #if (link in self.status['processed'][level-1]) or link in self.status['bad_links']:
                l = level
                exists = False
                while(l != -1):
                    if current_link in self.status['processed'][l-1]:
                        exists = True
                        break
                    l=l-1

                #if any(link in r for r in self.status['processed']) or link in self.status['bad_links']:
                if exists or current_link in self.status['bad_links']:
                    log( "Link already processed, skipping. ({0})".format(current_link) )
                    self.status['ignored_count']+=1

                # get all of the links from the page
                ignored = 0
                success, current_page_links, doc_length, bad_link = get_page_links(
                    self.status['url_data']['target_url'], 
                    current_link,
                    self.status['url_data']['allowed_domains'],
                )

                if bad_link:
                    self.status['bad_links'].append(current_link)

                if not success:
                    log( "Unable to get page links from link, skipping. ({0})".format(current_link) )
                    continue

                # sanitize the url link, and save it to our list of processed links
                _l = urljoin(
                    self.status['url_data']['target_url'],
                    current_link
                )
                self.status['processed'][level-1].append(_l)
                log( "Adding '{0}' to the processed list.".format(_l) )

                # Look at the links found on the page, and add those that are within
                # the allowed domains to page_links to process
                page_links = []
                for match,link,link_text in current_page_links:
                    # match,link,link_text = pagelink
                    if( match == True ):
                        page_links.append((link,link_text))
                        
                # Some of the links that were returned from this page might be docs we are 
                # interested in if they are, add them to the list of pdfs to be returned 'ret_links'
                for the_link,link_text in page_links:
                    #the_link,link_text = _the_link
                    if not any(the_link in r for r in ret_links) and not any(the_link in r for r in self.status['processed']):
                        success, link_type = type_link(the_link)
                        if success:

                            log( "Link successfully typed as '{0}'.".format(link_type) )

                            # add the link to the list of processed links
                            if the_link not in self.status['processed'][level-1]:
                                self.status['processed'][level-1].append(the_link)
                            else:
                                self.status['ignored_count']+=1

                            # if the link is of the type we are looking for, add it to the 
                            # list of docs to return
                            if link_type == self.status['url_data']['doc_type']:
                                ret_links.append((the_link,link_text))
                               
                                #broadcast the doc to the bus!
                                self.broadcast_document(the_link,link_text)
                        else:
                            log( "WARNING: Link unsuccessfully typed! ({0})".format(the_link) )
                            ignored += 1
                    else:
                        log( "Link already processed, skipping. ({0})".format(the_link) )

                # Follow all of the valid links on the page, and find all of the docs.
                got_links = self.follow_links(page_links, level)

                for glink in got_links:
                    l,t = glink
                    if (l,t) not in ret_links:
                        ret_links.append((l,t))

                log( "Done processing url: '{0}'".format(link) )

            level -= 1

        #for l in links:
        #    if not l in self.status['processed'][level-1]:
        #        the_link, the_link_text = l
        #        self.status['processed'][level-1].append(the_link)

        return ret_links

