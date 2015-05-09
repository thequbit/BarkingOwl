import json
import datetime
from optparse import OptionParser
from scraper import Scraper

_DEBUG = False

def print_doc_info(_data, document_url):
    if _DEBUG == True:
        print "    New Document: {0} : {1}\n".format(document_url['tag_text'], document_url['url'])

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

    (options, args) = parser.parse_args()

    if not options.target_url == '' and not options.target_url == None and \
            not options.doc_type == '' and not options.doc_type == None and \
            not options.max_link_level == '' and \
            not options.max_link_level == None and \
            not options.tracking_method == '' and \
            not options.tracking_method == None and \
            not options.json_output == '' and not options.json_output == None:

        if options.json_output == False:
            _DEBUG = True

        if _DEBUG == True:
            print " -- CLI BarkingOwl Scraper -- "

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
                found_doc_callback = print_doc_info,
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
