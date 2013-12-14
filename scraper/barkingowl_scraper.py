from scraperwrapper import ScraperWrapper

def main():
    print "Starting BarkingOwl-Scraper."

    sw = ScraperWrapper()
    try:
        sw.start()
    except:
        print "Exiting BarkingOwl-Scraper."

main()
