import os
import json
import datetime
from optparse import OptionParser
from scraper import Scraper
import requests
from urlparse import urlsplit
from random import randint

_DEBUG = False

_DOWNLOAD_DOCUMENTS = False
_DOCUMENT_DIRECTORY = '.'
_DOCUMENT_COUNT = 0

def handle_doc(_data, document_url):
    if _DEBUG == True:
        print "    New Document: {0} : {1}\n".format(document_url['tag_text'], document_url['url'])

    if _DOWNLOAD_DOCUMENTS == True:
        
        rawname = urlsplit(document_url['url']).path.split('/')[-1]
        filename = '{0}/{1}_{2}'.format(_DOCUMENT_DIRECTORY, _DOCUMENT_COUNT, rawname)

        if not os.path.isdir(_DOCUMENT_DIRECTORY):
            os.makedirs(_DOCUMENT_DIRECTORY)

        print "    Downloading document to '{0}' ...".format(filename)

        with open(filename, 'w') as f:
            response = requests.get(document_url['url'], stream=True)
            if not response.ok:
                print "    ERROR! File could not be downloaded.\n"
                return
            for block in response.iter_content(1024):
                if not block:
                    break
                f.write(block)
        global _DOCUMENT_COUNT
        _DOCUMENT_COUNT += 1

        print "    Done.\n"

if __name__ == '__main__':

    parser = OptionParser()
        
    parser.add_option("-u", "--target-url", dest="target_url",
        help="Target URL to scrape", metavar="URL")

    parser.add_option("-t", "--doc-type", dest="doc_type",
        help="Document type to look for.", metavar="DOCTYPE")

    parser.add_option("-l", "--max-link-level", dest="max_link_level",
        help="Maximum links to follow.", metavar="MAXLEVEL")

    parser.add_option("-m", "--tracking-method", dest="tracking_method",
        help="URL Tracking Method (dict, sql, mongo)", metavar="METHOD")

    parser.add_option("-i", "--uri", dest="uri",
        help="URI for Tracking Method (optional)", metavar="URI")

    parser.add_option("-j", "--json-output", action="store_true", 
        dest="json_output", help="Produce Pretty JSON output.", 
        default=False)

    parser.add_option("-d", "--download-documents", action="store_true",
        dest="download_documents", help="Download a document once found.",
        default=False)

    parser.add_option("-x", "--download-directory", dest="download_directory",
        help="Document download directory.", metavar="DIR")

    (options, args) = parser.parse_args()

    if not options.target_url == '' and not options.target_url == None and \
            not options.doc_type == '' and not options.doc_type == None and \
            not options.max_link_level == '' and \
            not options.max_link_level == None and \
            not options.tracking_method == '' and \
            not options.tracking_method == None and \
            not options.json_output == '' and not options.json_output == None and \
            not options.download_documents == '' and not options.download_documents == None and \
            not options.download_directory == '' and not options.download_directory == None:

        if options.json_output == False:
            _DEBUG = True

        if _DEBUG == True:
            print " -- CLI BarkingOwl Scraper -- "

        if options.download_documents == True:
            _DOWNLOAD_DOCUMENTS = True

        _DOCUMENT_DIRECTORY = options.download_directory

        url = {
            'target_url': options.target_url,
            'doc_types': [
                options.doc_type,
            ],
            'title': options.target_url,
            'description': options.target_url,
            'max_link_level': int(options.max_link_level),
            'creation_datetime': str(datetime.datetime.now()),
            'allowed_domains': [
            ],
            'sleep_time': 0, # do not sleep between URL fetches
        }

        #try:
        if True:
            scraper = Scraper(
                check_type=options.tracking_method,
                check_type_uri=options.uri,
                DEBUG=_DEBUG,
            )
            scraper.set_callbacks(
                found_doc_callback = handle_doc,
            )
            scraper.set_url_data(url)

            if _DEBUG == True:
                print "\nStarting Scraper on {0} ...\n\n".format(options.target_url)
            data = scraper.start()
            if _DEBUG == True:
                print "\n\nScraper complete.\n"

            if _DEBUG == True:
                print "BarkingOwl Scraper found {0} documents on {1}.\n\n".format(
                    len(data['documents']),
                    options.target_url,
                )

            if options.json_output == True:
                data = scraper._data
                for key in data:
                    if isinstance(data[key], datetime.datetime) or \
                            isinstance(data[key], datetime.timedelta):
                        data[key] = str(data[key])
                print json.dumps(scraper._data, sort_keys=True,
                    indent=4, separators=(',', ': '))

        #except:
        #    if DEBUG == True:
        #        print "Yikes!  An error occured while the scraper was running.  Exiting."
        #    else:
        #        print '{"error_text": " An error occured while the scraper was running."}'
    else:
        print "Error: missing CLI arguments.  Try -h for help."
