import datetime
import time
import resource
import urllib2
import urlparse
from bs4 import BeautifulSoup
import tldextract
import requests
import magic
import json

class Scraper(object):

    def __init__(self, file_header_size=1024, max_bandwidth=-1,
            max_memory=-1, DEBUG=False):
        self._reset_scraper()

        self.__file_header_size = file_header_size
        self.__max_bandwidth = max_bandwidth
        self.__max_memory = max_memory

        self._start_callback = None
        self._finished_callback = None
        self._found_doc_callback = None
        self._new_url_callback = None
        self._bandwidth_limit_callback = None
        self._memory_limit_callback = None
        self._error_callback = None

    def set_url_data(self, url_data):
        if (not 'target_url' in url_data) or  \
                (not 'doc_types' in url_data) or \
                (not 'title' in url_data) or \
                (not 'description' in url_data) or \
                (not 'max_link_level' in url_data) or \
                (not 'creation_datetime' in url_data) or \
                (not 'allowed_domains' in url_data):
            raise Exception("Invalid url_data payload.")
        self._data['url_data'] = url_data
        self._data_loaded = True

    def set_call_backs(self, start_callback=None, finished_callback=None, \
            found_doc_callback=None, new_url_callback=None, \
            bandwidth_limit_callback=None, memory_limit_callback=None, \
            error_callback=None):

        self._start_callback = start_callback
        self._finished_callback = finished_callback
        self._found_doc_callback = found_doc_callback
        self._new_url_callback = new_url_callback
        self._bandwidth_limit = bandwidth_limit_callback
        self._memory_limit = memory_limit_callback
        self._error_callback = error_callback

    def _start(self):
        if not self._start_callback == None:
            try:
                self._start_callback(self._data)
            except:
                pass

    def _stop(self):
        if not self._finished_callback == None:
            try:
                self._finished_callback(self._data)
            except:
                pass
    def _found_doc(self, document_url):
        if not self._found_doc_callback == None:
            try:
                self._found_doc_callback(self._data, document_url)
            except:
                pass

    def _new_url(self, url):
        if not self._new_url_callback == None:
            try:
                self._new_url_callback(self._data, url)
            except:
                pass

    def _bandwidth_limit(self):
        if not self._bandwidth_limit_callback == None:
            try:
                self._bandwidth_limit_callback(self._data)
            except:
                pass

    def _memory_limit(self):
        if not self._memory_limit_callback == None:
            try:
                self._memory_limit_callback(self._data)
            except:
                pass

    def _error(self, error_text):
        if( not self._error_callback == None ):
            try:
                self._error_callback(self._data, error_text)
            except:
                pass

    def _reset_scraper(self):
        self._data = {}
        self._data['seen_urls']  = []
        self._data['bandwidth'] = 0
        self._data['ignored_count'] = 0
        self._data['link_level'] = 1
        self._data['bad_urls'] = []
        self._data['documents'] = []
        self._data['file_header_size'] = 1024 
        self._data['url_data'] = {}
        self._data['working'] = False
        self._data['elapsed_time'] = None
        self._data['start_datetime'] = None
        self._data['stop_datetime'] = None 

        self._stopping = False
        self._data_loaded = False

    def stop(self):
        self._stopping = True
        
    def start(self):
        if self._data_loaded == False:
            raise Exception("URL Data not set.")
        self._stopping = False
        self._data['working'] = True
        self._start()
        start_datetime = datetime.datetime.now() 
        success = self._find_documents()
        stop_datetime = datetime.datetime.now()
        self._data['working'] = False
        self._stop()
        self._data['elapsed_time'] = str(stop_datetime - start_datetime)
        self._data['start_datetime'] = str(start_datetime)
        self._data['stop_datetime'] = str(stop_datetime)

        print json.dumps(self._data, sort_keys=True,
            indent=4, separators=(',', ': '))

    def _find_documents(self):
        root_url = {
            'url': self._data['url_data']['target_url'],
            'tag_text': '<root>',
            'page_title': '<root>',
            'page_url': self._data['url_data']['target_url'],
        }
        success = self._process_urls([root_url])
        return success

    def _process_urls(self, urls):

        if self._stopping == True:
            return False

        if self._data['link_level'] > self._data['url_data']['max_link_level']:
            return False
        else:
            self._data['link_level'] += 1
            
        for url in urls:

            page_urls = self._get_page_urls(url)

            for page_url in page_urls:
                if self._stopping == True:
                    return False
                if not page_url in self._data['bad_urls'] and \
                        not any(seen_url['url'] == page_url['url'] \
                        for seen_url in self._data['seen_urls']):
                    doc_type = self._type_document(page_url)
                    if doc_type != '' and \
                            doc_type in self._data['url_data']['doc_types']:
                        if not any(doc['url'] == page_url \
                                for doc in self._data['documents']):
                            new_doc = {
                                'url': page_url,
                                'found_datetime': str(datetime.datetime.now()),
                            }
                            self._data['documents'].append(new_doc)
                            self._found_doc(new_doc)

                else:
                    self._data['ignored_count'] += 1
            success = self._process_urls(page_urls)

        self._data['link_level'] -= 1

        return True
        
    def _get_page_urls(self, target_url):

        page_urls = []

        #try:
        if True:

            response = ''
            try:
                response = requests.get(target_url['url'])
                self._data['bandwidth'] += len(response.headers)
                self._data['bandwidth'] += len(response.text)
            except:
                pass

            soup = BeautifulSoup(response.text)
            
            page_title = ''
            page_title_tag = soup.find_all('title')
            if len(page_title_tag) != 0 and page_title_tag[0].string != None:
                page_title = page_title_tag[0].string.strip()

            tag_types = [
                ('a','href'),
                ('img','src'),
                ('link','href'),
                ('object','data'),
                ('source','src'),
                ('script','src'),
                ('embed','src'),
                ('iframe','src'),
            ]
            
            for tag_type,verb in tag_types:
                tags = soup.find_all(tag_type)
                for tag in tags:
                    tag_text = ''
                    if len(tag.contents) >= 1:
                        if tag.string != None:
                            tag_text = tag.string.strip()
                        
                    raw_url = ''
                    try:
                        raw_url = tag[verb].encode('utf-8')
                    except:
                        continue
                    
                    full_url = urlparse.urljoin(target_url['url'], raw_url)

                    match = self._check_match(full_url)

                    if match == False:
                        continue

                    url = {
                        'url': full_url,
                        'tag_text': tag_text,
                        'page_title': page_title,
                        'page_url': target_url['url'],
                    }
                    page_urls.append(url)

        #except:
        #    pass

        return page_urls

    def _check_match(self, url):
        match = False
        url_domain = tldextract.extract(url).domain.lower()
        target_url_domain = tldextract.extract(
            self._data['url_data']['target_url']
        ).domain.lower()
        if url_domain == target_url_domain or \
                url_domain in self._data['url_data']['allowed_domains']:
            match = True
        return match

    def _type_document(self, url):
        document_type = None
        for seen_url in self._data['seen_urls']:
            if url['url'] == seen_url:
                document_type = seen_url['type']
        if document_type == None:
            req = urllib2.Request(url['url'], headers={'Range':"byte=0-{0}".format(
                self._data['file_header_size'])}
            )
            try:
                open_url = urllib2.urlopen(req,timeout=5)
                headers = open_url.info()
                payload = open_url.read(
                    self._data['file_header_size']
                )
                self._data['bandwidth'] += ( len(headers) + len(payload) )
                document_type = magic.from_buffer(payload, mime=True) 
            except:
                self._data['bad_urls'].append(url['url'])
        self._data['seen_urls'].append({
            'url': url['url'],
            'type': document_type,
        })
        self._new_url(url)
        print "Memory: {0} KB, Bandwidth: {1} Bytes, URL Count: {2}, Document Count: {3}, Ignored Count: {4}.".format(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            self._data['bandwidth'],
            len(self._data['seen_urls']),
            len(self._data['documents']),
            self._data['ignored_count'],
        )
        return document_type
