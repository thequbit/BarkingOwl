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

    #log( "Attempting to type link: '{0}' (using filesize: {1})".format(link,file_size) )

    success = False
    file_type = ""
    try:

        if 'mailto:' in link:
            raise Exception('Invalid link type.')

        req = urllib2.Request(link, headers={'Range':"byte=0-{0}".format(file_size)})

        # try and download the file five times ( in case the site is being fussy )
        error_count = 0
        while(error_count < 5):
            try:
                payload = urllib2.urlopen(req,timeout=5).read(file_size)
                #log( "Successfully downloaded the first {0} bytes of '{1}'.".format(file_size, link) )
                break
            except Exception, e:
                #log( "Error within type_link while trying to download {0} bytes from URL:\n\t{1}\n".format(link,str(e)) )
                if str(e) != 'time out':
                    raise Exception(e)
                else:
                    error_count += 1
                    time.sleep(sleep_time)
        # type file using libmagic
        file_type = magic.from_buffer(payload, mime=True)
        success = True
        
    except Exception, e:
        #log( "An error has occured in the type_link() function:\n\tURL: {0}\n\tError: {1}".format(link,str(e)) )
        pass
    
    return success, file_type

def check_match(domain_url, link, allowed_domains):
    """ check_match() is used to derumine of a link is linking to the parent domain or another domain.
    """
    
    site_match = True
    url_data = urlparse.urlparse(link)

    #log( "check_match(): urlparse results:" )
    #log( url_data )

    if ( (len(link) >= 7 and link[0:7].lower() == "http://") or
         (len(link) >= 8 and link[0:8].lower() == "https://") or
         (len(link) >= 3 and link[0:6].lower() == "ftp://") ): 
        url_a = "{0}://{1}".format(url_data.scheme, url_data.netloc)
        url_b = "{0}://{1}/".format(url_data.scheme, url_data.netloc)
        if(url_a != domain_url and url_b != domain_url):
            if not url_a in allowed_domains and not url_b in allowed_domains:
                site_match = False

    #log( "Comparing domain_url='{0}', netloc='{1}', link='{2}', with sitematch='{3}'.".format(domain_url, url_data.netloc, link, site_match) )

    return site_match

def sanity_check_url(site_url, raw_href):

    # there are some websites that have absolute links that go above
    # the root ... why this is I have no idea, but this is how i'm
    # solving it
    links = []
    relative_parts = raw_href.split('../')
    if len(relative_parts) == 1:
        abs_link = urljoin(site_url, raw_href)
        links.append(abs_link)
    
    elif len(relative_parts) == 2:
        abs_link = urljoin(site_url, relative_parts[1])
        links.append(abs_link)
    
    elif len(relative_parts) == 3:
        newhref = "../{0}".format(relative_parts[2])

        abs_link = urljoin(site_url, new_href)
        links.append(abs_link)

        abs_link = urljoin(sit_eurl, relative_parts[2])
        links.append(abs_link)
    
    else:
        abs_link = urljoin(site_url, raw_href)
        links.append(abs_link)

    ret_links = []
    for link in links:
        if not 'mailto:' in link:
            if ' ' in link:
                ret_links.append(urllib.urlencode(link))

    return ret_links

def get_page_links(domain_url, url, allowed_domains, file_size=1024, sleep_time=2):
    """ get_page_links() will return a list of all of the link on a html page that is passed.
    """

    #log( "Getting page links for '{0}' ...".format(url) )

    success = False
    links = []
    bandwidth_used = 0
    bad_link = True
    try:
        type_link_success, link_type = type_link(url, file_size, sleep_time)
        if type_link_success == False:
            bad_link = True
            #log( "Bad link found. ( {0} )".format(url) )
            raise Exception( 'Failed to type link.' );
        
        if link_type != "text/html":
            #log( "Link is not of type text/html." )
            raise Exception( 'Link is not of type text/html' )
            
        try:
       
            bandwidth_used = file_size
 
            try:
                html = urllib2.urlopen(url)
            except Exception, e:
                err_text = "get_page_links(): urllib2 error: '{0}'".format(str(e))
                #log( err_text )
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
                        #log( "tag ('{0}') didn't have verb ('{1}'), ignoring.".format(tag,verb) )
                        continue

                    match = check_match(domain_url, raw_href, allowed_domains)
                    abs_link = urljoin(domain_url, raw_href)
                    links.append((match,abs_link,link_text))
        
                    abs_links = sanity_check_url(domain_url, raw_href)
                    for l in abs_links:
                        links.append((match,l,link_text))

            success = True
            bad_link = False
            bandwidth_used = document_length + file_size
        except Exception, e:
            links = []
            sucess = False
            #log( "An error occurred in get_page_links():\n\tURL: {0}\n\tError: {1}".format(url,str(e)) )
    except Exception, e:
        #log( 'Exception: {0}'.format(str(e)) )
        pass
    
    #log( "Found {0} links from URL: '{1}', bad_link = {2}.".format(len(links),url,bad_link) )

    return success, links, bandwidth_used, bad_link


class Scraper(): #threading.Thread):

    def __init__(self, uid=str(uuid.uuid4()), type_file_size=1024): #,DEBUG=False):
        """
        __init__() constructor setups the threading enviornment and status variables.
        """

        #threading.Thread.__init__(self)
        
        #self._stop = threading.Event()
        self.started = False
        self.uid = uid
        #self.interval = 1

        self.status = {}
        self.status['busy'] = False
        self.status['processed_links'] = []
        self.status['bad_links'] = []
        self.status['link_count'] = 0
        #self.status['level'] = -1
        self.status['url_data'] = {}
        self.status['bandwidth'] = 0
        self.status['ignored_count'] = 0

        self.type_file_size = type_file_size

        #self.DEBUG = DEBUG

        # start a timer to see if we should be exiting
        #threading.Timer(self.interval,self._checkshutdown).start()

        self.finished_callback = None
        self.started_callback = None
        self.broadcast_document_found_callback = None

        #log( "Scraper INIT successful." )

    def set_callbacks(self, finished_callback=None,
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

    def set_finished_callback(self, finished_callback):
        """
        setFinishedCallback() defineds the function that should be called when the finished state occures.
        """
        self.finished_callback = finished_callback

    def set_started_callback(self, started_callback):
        """
        setStartedCallback() defineds the function that should be called when the started state occures.
        """
        self.started_allback = started_callback

    def set_broadcast_document_callback(self, broadcast_document_found_callback):
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
                'creation_datetime': creationdatetime, # ISO creation date and time
                'doc_type': doctype, # the text for the magic lib to look for (ex. 'application/pdf')
                'frequency': frequency, # the frequency in minutes the URL should be scraped
                'allowed_domains': [], # a list of allowable domains for the scraper to follow
            }
    
        If the above fields are not included, then the scraper will not work as expected, but may not throw an error.

        Note: Any additional fields can be included, and will be include with and pass along when a document is found.
        
        Note: Setting the URL data will reset the scraper.

        """

        self.reset()
        
        #log( "URL data payload: {0}".format(self.status['url_data']) )

        # error check all the things

        if not 'target_url' in url_data or url_data['target_url'] == '':
            log( "Unassigned or invalid target_url.  Check URL dictionary." )
            raise Exception('Unassigned or invalid Target URL.')

        if not 'doc_type' in url_data or url_data['doc_type'] == '':
            log( "Unassigned or invalid doc_type.  Check URL dictionary." )
            raise Exception('Unassigned or invalid Document Type.');
        

        # set defaults for not found keys in dict

        if not 'title' in url_data:
            log( "'title' not found in URL dictionary, setting to target_url." )
            url_data['title'] = url_data['target_url']
        
        if not 'description' in url_data:
            log( "'description' not found in URL dictionary, setting to ''" )
            url_data['description'] = ''

        if not 'max_link_level' in url_data:
            log( "'max_link_level' not found in URL dictionary, setting to 1" )
            url_data['max_link_level'] = 1

        if not 'creation_datetiem' in url_data:
            now = strftime("%Y-%m-%d %H:%M:%S") 
            log( "'creationdatetime' not found in URL dictionary, setting to '{0}'".format(now) )
            url_data['creation_datetime'] = now

        if not 'frequency' in url_data:
            log( "'frequency' not found in URL dictionary, setting to 24 hours" )
            url_data['frequency'] = 1440 # 24 hours in minutes

        if not 'allowed_domains' in url_data:
            log( "'allowed_domains' not found in URL dictionary, setting to []" )
            url_data['allowed_domains'] = []

        print "\n"
        print type(url_data['max_link_level'])
        print "\n"

        # if the data is being supplied by a CSV max_link_level can end up as a string
        # we'll test for this, and convert to a number
        if isinstance(url_data['max_link_level'], str) \
                or isinstance(url_data['max_link_level'], unicode):
            url_data['max_link_level'] = int(url_data['max_link_level'])

        # set the url data local
        self.status['url_data'] = url_data

    def reset(self):
        """
        reset() resets the state of the scraper.  This should not be called unless the scraper is stopped.
        """

        # reset globals
        self.status['processed_links'] = []
        self.status['bad_links'] = []
        self.status['link_count'] = 0
        #self.status['level'] = -1
        self.status['bandwidth'] = 0
        self.status['busy'] = False
        self.status['url_data'] = {}
        self.status['ignored_count'] = 0

        #log( "Scraper data reset successfully." )

    def broadcast_finished(self):
        """
        broadcastfinish() calls the async scraper finished call back with status information within its payload.
        """
        
        #log( "Scraper Finished." )

        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'processed_links': self.status['processed_links'],
            'bad_links': self.status['bad_links'],
            'link_count': self.status['link_count'],
            'ignored_count': self.status['ignored_count'],
            'url_data': self.status['url_data'],
            'bandwidth': self.status['bandwidth'],
            'start_datetime': str(isodatetime)
        }
        payload = {
            'command': 'scraper_finished',
            'source_id': self.uid,
            'destination_id': 'broadcast',
            'message': packet
        }

        log( "Calling 'finished' callback function ..." )

        if self.finished_callback != None:
            self.finished_callback(payload)


    def broadcast_start(self):
        """
        broadcaststart() calls the scraper start async call back with status information within its payload.
        """
        
        #log( "Scraper Starting." )
        
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'url_data': self.status['url_data'],
            'startdatetime': str(isodatetime)
        }
        payload = {
            'command': 'scraper_start',
            'source_id': self.uid,
            'destination_id': 'broadcast',
            'message': packet
        }

        log( "Calling 'starting' callback function ..." )

        if self.started_callback != None:
            self.started_callback(payload)

    def broadcast_document(self,doc_url,link_text):
        """ broadcastdoc() calls the scraper document found async call abck with status and document information within
            its payload.
        """

        #log( "Doc Found: '{0}'.".format(doc_url) )

        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        packet = {
            'doc_url': doc_url,
            'link_text': link_text,
            'url_data': self.status['url_data'],
            'scrape_datetime': str(isodatetime)
        }
        payload = {
            'command': 'found_doc',
            'source_id': self.uid,
            'destination_id': 'broadcast',
            'message': packet
        }

        log( "Calling 'found document' callback function ..." )

        if self.broadcast_document_found_callback != None:
            self.broadcast_document_found_callback(payload)

    def find_docs(self):

        log( "Starting scraper to find all documents on '{0}' ...".format(self.status['url_data']['target_url']) )

        self.status['busy'] = True
        
        self.broadcast_start()

        links = []
        links.append( (self.status['url_data']['target_url'],'<root>') )
        
        max_level = self.status['url_data']['max_link_level']
        for i in range(0,max_level+1):
            self.status['processed_links'].append([])
        
        docs = self.follow_links(links, level=max_level+1)

        self.broadcast_finished()

        self.status['busy'] = False

        log( "Scraping complete!" )

        return docs

    def follow_links(self, links, parent_links=[], level=0):
        """ followlinks() is the heart of the BarkingOwl Scraper.  It follows links to a specified link level,
            reporting if any of those links are documents that it should be identifying.  This function is a
            recursive function and can run for a very long time if the link level is not defined appropreately.
        """

        #log( "Following {0} Links on level {1} ...".format(len(links),level) )

        ret_links = []
        if( level == 0 ): #>= self.status['url_data']['max_link_level'] ):
            # made it to bottom link level, no need to continue
            pass
        else:
            level -= 1
            #log( "Link level = {0}.  working on {1} links: {2}".format(level,len(links),json.dumps(links)) )
                #print "Processing Links: {0}".format(links)

            # we need to keep track of what links we have visited at each
            # level.  Here we are adding to our array each time a new level
            # is seen
 
            #log( 'Current Level: {0}\n'.format(level) )
 
            #log( "len(processed_links) = {0}".format(len(self.status['processed_links']) ))

            #if len(self.status['processed_links'])-1 < level:
            #    #log( "Current Level ({0}) does not exist within processed_links link list, adding.".format(level) )
            #    self.status['processed_links'].append([])

            for current_link, link_text in links:

                #link,link_text = _link

                if current_link in self.status['bad_links']:
                    #log( 'current link in bad links list, continue.' )
                    continue

                log( "Working on '{0}' ...".format(current_link) )

                # see if we have already processed_links the link at max level, and we
                # are at maxlevel.  If that is the case, it is pointless to do the 
                # bottom of the tree over and over again.  Also don't do anything 
                # if it is 404/bad link

                #if current_link in parent_links:
                #    continue

                # first check if we have already processed the link at our level
                if current_link in self.status['processed_links'][level]:
                    #log( '{0} already processed at CURRENT level (current level: {1}).'.format(current_link, level) )
                    continue
                
                exists = False
                if level != 1:
                    for i in range(0,level):
                        if current_link in self.status['processed_links'][i]:
                            #log( '{0} already processed at level {1} (current level: {2})'.format(current_link, level-1-i, level) )
                            exists = True
                            break

                #if any(link in r for r in self.status['processed_links']) or link in self.status['bad_links']:
                if exists:
                    #log( "{0} already processed, skipping.".format(current_link) )
                    self.status['ignored_count']+=1
                    continue


                # record that we have looked at the link
                self.status['link_count'] += 1

                # get all of the links from the page
                ignored = 0
                success, current_page_links, document_length, bad_link = get_page_links(
                    self.status['url_data']['target_url'], 
                    current_link,
                    self.status['url_data']['allowed_domains'],
                )
                self.status['bandwidth'] += document_length

                #log( "At level {0}, for url '{1}', current page links: {2}".format(level, current_link, json.dumps(current_page_links)) )

                if bad_link:
                    self.status['bad_links'].append(current_link)
                    continue

                if not success:
                    #log( "Unable to get page links from link, skipping. ({0})".format(current_link) )
                    continue

                # sanitize the url link, and save it to our list of processed_links links
                #_l = urljoin(
                #    self.status['url_data']['target_url'],
                #    current_link
                #)
                #self.status['processed_links'][level].append(_l)

                # Look at the links found on the page, and add those that are within
                # the allowed domains to page_links to process
                page_links = []
                for match, link, link_text in current_page_links:
                    # match,link,link_text = pagelink
                    if( match == True ):
                        page_links.append((link,link_text))
               
                #log( 'found {0} page links to follow'.format(len(page_links)) )
 
                # Follow all of the valid links on the page, and find all of the docs.
                got_links = self.follow_links(page_links, links, level)

                #log( 'Processing page links to find documents' )

                # Some of the links that were returned from this page might be docs we are 
                # interested in if they are, add them to the list of pdfs to be returned 'ret_links'
                for the_link, link_text in page_links:
                    #the_link,link_text = _the_link
                    if not any(the_link in r for r in ret_links) and not any(the_link in r for r in self.status['processed_links']):
                        
                        # record that we have looked at the link
                        self.status['link_count'] += 1

                        success, link_type = type_link(the_link, file_size = self.type_file_size)
                        self.status['bandwidth'] += self.type_file_size
                        if success:

                            #log( "Link successfully typed as '{0}'.".format(link_type) )

                            # add the link to the list of processed_links links
                            if not the_link in self.status['processed_links'][level]:
                                self.status['processed_links'][level].append(the_link)
                            else:
                                self.status['ignored_count'] += 1

                            # if the link is of the type we are looking for, add it to the 
                            # list of docs to return
                            if link_type == self.status['url_data']['doc_type']:
                                ret_links.append((the_link,link_text))
                               
                                #broadcast the doc to the bus!
                                self.broadcast_document(the_link,link_text)
                        else:
                        #    #log( "WARNING: Link unsuccessfully typed! ({0})".format(the_link) )
                        #    ignored += 1
                            self.status['ignored_count'] += 1
                    #else:
                    #    #log( "Page link '{0}' already processed, skipping.".format(the_link) )

                # Follow all of the valid links on the page, and find all of the docs.
                #got_links = self.follow_links(page_links, links, level)

                for glink in got_links:
                    l,t = glink
                    if not (l,t) in ret_links:
                        ret_links.append((l,t))

                # sanitize the url link, and save it to our list of processed_links links
                _l = urljoin(
                    self.status['url_data']['target_url'],
                    current_link
                )
                self.status['processed_links'][level].append(_l)
                #log( "Adding '{0}' to the processed_links list.".format(_l) )

                #log( "Done processing url: '{0}'".format(link) )

            level -= 1

        return ret_links

