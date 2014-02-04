from scraperwrapper import ScraperWrapper

sw = None

def StartScraper(exchange="barkingowl"):
   global sw
   sw = ScraperWrapper(exchange="barkingowl",DEBUG=True)
   sw.start()

if __name__ == '__main__':
    #print "Starting BarkingOwl-Scraper."

    #try:
        StartScraper()
    #except:
    #    print "Exiting BarkingOwl-Scraper."
