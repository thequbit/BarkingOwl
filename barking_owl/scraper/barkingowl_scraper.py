from scraperwrapper import ScraperWrapper

def StartScraper(address='localhost',exchange='barkingowl'):
    
    sw = ScraperWrapper(
        address = address,
        exchange = exchange,
        DEBUG = True,
    )
    
    data = sw.start()

    print data['documents']

if __name__ == '__main__':

    print "Launching BarkingOwl Scraper."

    try:
        StartScraper()
    except:
        pass
