from scraper import Scraper
import uuid
from time import strftime
import pprint
import json

def test_typelink(scraper):
    print "\nTesting typelink() ..."
    success,filetype = scraper.typelink('http://timduffy.me/')
    print "\tFiletype = '{0}'".format(filetype)
    if success:
        print "Passed!"
    else:
        print "Failed!"
    print "--------------------"

def test_checkmatch(scraper):
    siteurl = "http://timduffy.me/"
    linkA = "http://timduffy.me/blog.html"
    linkB = "http://google.com/index.html"
    print "\nTesting checkmatch() ..."
    sitematchA = scraper.checkmatch(siteurl,linkA)
    sitematchB = scraper.checkmatch(siteurl,linkB)
    print "\tLinkA = '{0}'(expeect True), LinkB = '{1}'(expect False)".format(sitematchA,sitematchB)
    if sitematchA and not sitematchB:
        print "Passed!"
    else:
        print "Failed!"
    print "--------------------"

def test_getpagelinks(scraper):
    siteurl = "http://timduffy.me/"
    url = "http://timduffy.me/"
    print "\nTesting getpagelinks() ..."
    success,pagelinks = scraper.getpagelinks(siteurl,url)
    
    if success:
        print "\tLink Count: {0}".format(len(pagelinks))
        print "Passed!"
    else:
        print "Failed!"

    print "--------------------"

def test_followlinks(scraper):
    links = []
    #links.append(('http://timduffy.me/blog.html','TimDuffy.me Blog'))
    links.append(('http://timduffy.me/','TimDuffy.me'))
    isodatetime = strftime("%Y-%m-%d %H:%M:%S")
    urldata = {
        'urlid': 1,
        'targeturl': 'http://timduffy.me/',
        'maxlinklevel': 5,
        'creationdatetime': '2013-11-18 21:09:30',
        #'doctypetitle': 'HTML Document',
        #'docdescription': 'HTML Document',
        #'doctype': 'text/html',
        'doctype': 'application/pdf',
                    
        'disparchdatetime': isodatetime,
        'allowdomains': [],
    }
    print "\nTesting followlinks() ..."

    scraper.status['urldata'] = urldata
    docs = scraper.followlinks(links=links,level=0)

    print "Number of Docs found: {0}".format(len(docs))

    return docs

def text_getstatus(scraper):
    print scraper.status

def main():
    uid = str(uuid.uuid4())

    print "Creating Scraper() instance ..."

    scraper = Scraper(uid, DEBUG=True)
    scraper.run()

    print "Running tests ..."

    # typelink()
    #test_typelink(scraper)    

    # checkmatch()
    #test_checkmatch(scraper)

    # getpagelinks
    #test_getpagelinks(scraper)

    # folowlinks()
    docs = test_followlinks(scraper)

    print "Documents found: {0}".format(json.dumps(docs))

    # get scraper status
    #text_getstatus(scraper)

    try:
        scraper.stop();
    except:
        pass

    print "Done."

main()
