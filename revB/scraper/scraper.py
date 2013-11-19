import pika
import simplejson
import threading
import urllib
import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import magic
from time import strftime

class Scraper(threading.Thread):

    status = {}

    def __init__(self,uid,address='localhost',exchange='barkingowl'):

        threading.Thread.__init__(self)

        self.uid = uid

        self.status['busy'] = False
        self.status['processed'] = []
        self.status['badlinks'] = []
        self.status['linkcount'] = 0
        self.status['level'] = -1
        self.status['urldata'] = {}

        self.address = address
        self.exchange = exchange

        # setup message broadcasting
        self.respcon = pika.BlockingConnection(pika.ConnectionParameters(
		                                           host=self.address))
        self.respchan = self.respcon.channel()
        self.respchan.exchange_declare(exchange=self.exchange,type='fanout')

    def start(self,urldata):
        print "Starting Scraper ..."
        self.broadcaststart()
        self.status['urldata'] = urldata
        self.status['busy'] = True
        
        links = []
        links.append((urldata['targeturl'],'<root>'))
        self.followlinks(links=links,
                         level=0)
        
        self.status['busy'] = False
        #
        # TODO: kill thread
        #
        print "Stopping Scraper."

    def broadcaststart(self):
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
        jbody = simplejson.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    def broadcastdoc(self,docurl,linktext):
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
        jbody = simplejson.dumps(payload)
        self.respchan.basic_publish(exchange=self.exchange,routing_key='',body=jbody)

    def typelink(self,link,filesize=2024):
        req = urllib2.Request(link, headers={'Range':"byte=0-{0}".format(filesize)})
        success = True
        filetype = ""
        try:
            payload = urllib2.urlopen(req,timeout=5).read(filesize)
            filetype = magic.from_buffer(payload,mime=True)
        except Exception, e:
            success = False;
        return success,filetype

    def checkmatch(self,siteurl,link):
        sitematch = True
        if ( (len(link) >= 7 and link[0:7].lower() == "http://") or
             (len(link) >= 8 and link[0:8].lower() == "https://") or
             (len(link) >= 3 and link[0:6].lower() == "ftp://") ): 
            if(link[:link.find("/",7)+1] != siteurl):
                sitematch = False
        return sitematch

    def getpagelinks(self,siteurl,url):
        links = []
        success,linktype = self.typelink(url)
        if success == False:
            self.status['badlinks'].append(url)
            return success,links
        sucess = True
        if linktype != "text/html":
            return False,links
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
        self.status['linkcount'] += len(links)
        return success,links

    def followlinks(self,links,level=0,debug=False):
        retlinks = []
        #_level = level
        if( level >= self.status['urldata']['maxlinklevel'] ):
            # made it to bottom link level, no need to continue
            pass
        else:
            level += 1
            print "Working on {0} links ...".format(len(links))
            for _link in links:
                link,linktext = _link

                print "{0}".format(link)

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
                #print "succes = {0}".format(success)
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

                print "Number of Page Links: {0}".format(len(pagelinks))

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
                       print "{0} : {1} : {2}".format(success,linktype,thelink)
                       #print retlinks

                print "Starting to follow links to next level ..."

                # Follow all of the link within the 'thelink' array
                gotlinks = self.followlinks(#orgid=orgid,
                                           #urlid=urlid,
                                           #maxlevel=maxlevel,
                                           #siteurl=siteurl,
                                           links=pagelinks,
                                           level=level,
                                           #filesize=filesize,
                                          )

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


