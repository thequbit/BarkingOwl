import datetime
from optparse import OptionParser
from scraper import Scraper

def print_doc_info(_data, document_url):
    print "    {0} : {1}".format(document_url['tag_text'], document_url['url'])

if __name__ == '__main__':

    print " -- CLI BarkingOwl Scraper -- "

    parser = OptionParser()
        
    parser.add_option("-u", "--target-url", dest="target_url",
        help="Target URL to scrape", metavar="URL")

    parser.add_option("-t", "--doc-type", dest="doc_type",
        help="Document type to look for.", metavar="DOCTYPE")

    parser.add_option("-l", "--max-link-level", dest="max_link_level",
        help="Maximum links to follow.", metavar="MAXLEVEL")

    (options, args) = parser.parse_args()

    if not options.target_url == '' and not options.target_url == None and \
            not options.doc_type == '' and not options.doc_type == None and \
            not options.max_link_level == '' and not options.max_link_level == None:

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
        }

        try:
            scraper = Scraper()
            scraper.set_callbacks(
                found_doc_callback = print_doc_info,
            )
            scraper.set_url_data(url)

            print "\nStarting Scraper on {0} ...\n\n".format(options.target_url)
            data = scraper.start()
            print "\n\nScraper complete.\n"

            print "BarkingOwl Scraper found {0} documents on {1}.\n\n".format(
                len(data['documents']),
                options.target_url,
            )
        except:
            print "Yikes!  An error occured while the scraper was running.  Exiting."
            pass
    else:
        print "Error: missing CLI arguments.  Try -h for help."
