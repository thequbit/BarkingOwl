from scraper import (
    type_link,
    check_match,
    sanity_check_url,
    get_page_links,
)

from scraper import Scraper

import uuid
import time
import pprint
import json
import datetime

def declare_test_start(test_name):
    print "\n------------------"
    print "--"
    print "-- Testing {0} ...".format(test_name)
    print "--"
    print "------------------\n"

def declare_test_end(passed):
    print "\n-- TEST COMPLETE --\n"
    print "Result:"
    if passed:
        print "\tPassed!\n"
    else:
        print "\tFailed!\n"


#############################################
#
# Tests 
#

def test_type_link():

    declare_test_start( 'type_link()' )

    success, file_type = type_link('http://timduffy.me/')
   
    print "[ TEST ] file type = '{0}'".format(file_type)

    passed = False
    if success and file_type == 'text/html':
        passed = True

    declare_test_end( passed )   
 

def test_check_match():

    declare_test_start( 'check_match()' )

    passed = True

    site_url = "http://timduffy.me/"
    
    link_a = "http://timduffy.me/blog.html"
    link_b = "http://google.com/index.html"
    
    allowed_domains = []

    site_match_a = check_match(site_url, link_a, allowed_domains)
    site_match_b = check_match(site_url, link_b, allowed_domains)
    
    print "[ TEST ] LinkA = '{0}'(expeect True), LinkB = '{1}'(expect False)".format(site_match_a,site_match_b)
    
    if not (site_match_a and not site_match_b):
        passed = False

    allowed_domains = ['http://google.com/']

    site_match_a = check_match(site_url, link_a, allowed_domains)
    site_match_b = check_match(site_url, link_b, allowed_domains)

    print "[ TEST ] LinkA = '{0}'(expeect True), LinkB = '{1}'(expect True)".format(site_match_a,site_match_b)

    if not (site_match_a and site_match_b):
        passed = False

    declare_test_end( passed )

def test_get_page_links():

    declare_test_start( 'get_page_links()' )

    site_url = "http://timduffy.me/"
    url = "http://timduffy.me/"
    allowed_domains = []

    success, page_links, document_length, bad_link = get_page_links(site_url, url, allowed_domains)

    print "[ TEST ] Success: {0}".format(success)
    print "[ TEST ] Link Count: {0}".format(len(page_links))
    print "[ TEST ] Document Length: {0}".format(document_length)
    print "[ TEST ] Bad Link: {0}".format(bad_link) 
    
    passed = False
    if success and len(page_links) > 0 and document_length > 0 and not bad_link:
        passed = True

    declare_test_end( passed )

def test_find_docs():

    declare_test_start( 'follow_link' ) 

    url_data = {
        'url_id': 1,
        'target_url': 'http://timduffy.me/',
        'max_link_level': 6,
        'creation_date_time': str(datetime.datetime.now()),
        'doc_type': 'application/pdf',
        'dispatch_datetime': str(datetime.datetime.now()),
        'allowed_domains': [],
    }

    uid = str(uuid.uuid4())
    scraper = Scraper(uid)
    scraper.set_url_data(url_data)
    docs = scraper.find_docs( )

    print '[ TEST ] {0}'.format(json.dumps(scraper.status))
    print '[ TEST ] {0}'.format(json.dumps(docs))

    passed = False
    if len(docs) > 0:
        passed = True

    declare_test_end( passed )

def test_find_docs_external():

    declare_test_start( 'follow_link' )

    url_data = {
        'url_id': 1,
        'target_url': 'http://www.scottsvilleny.org/',
        'max_link_level': 5,
        'creation_date_time': str(datetime.datetime.now()),
        'doc_type': 'application/pdf',
        'dispatch_datetime': str(datetime.datetime.now()),
        'allowed_domains': [],
    }

    uid = str(uuid.uuid4())
    scraper = Scraper(uid)
    scraper.set_url_data(url_data)
    docs = scraper.find_docs( )

    #print '[ TEST ] {0}'.format(json.dumps(scraper.status))
    #print '[ TEST ] {0}'.format(json.dumps(docs))

    with open('find_docs_external_results.json','w') as f:
        f.write(json.dumps(scraper.status))

    with open('find_docs_external_all_docs.json', 'w') as f:
        f.write(json.dumps(docs))

    passed = False
    if len(docs) > 0:
        passed = True

    declare_test_end( passed )

if __name__ == '__main__':

    print "Running tests ..."

    # typelink()
    test_type_link()    

    # checkmatch()
    test_check_match()

    # ge_tpage_links()
    test_get_page_links()

    # find_docs()
    docs = test_find_docs()

    # find_docs()
    #start_time = time.time()
    #docs = test_find_docs_external()
    #total_time = time.time() - start_time
    #print "External scraping took {0} seconds.".format(total_time)

    # get scraper status
    #text_get_status()

