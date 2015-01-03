from scraperwrapper import ScraperWrapper

def StartScraper(address='localhost',exchange='barkingowl'):
    
    sw = ScraperWrapper(
        address = address,
        exchange = exchange,
        DEBUG=True,
    )
    
    sw.start()

if __name__ == '__main__':

    print "Starting BarkingOwl-Scraper."

    #try:
    if True:
        StartScraper()
    #except:
    #    print "Exiting BarkingOwl-Scraper."
