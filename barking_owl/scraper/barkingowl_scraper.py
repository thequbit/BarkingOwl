import daemon

from scraperwrapper import ScraperWrapper

def StartScraper(address='localhost',exchange='barkingowl'):
    
    #with daemon.DaemonContext():
    if True:
        try: 
            sw = ScraperWrapper(
                address = address,
                exchange = exchange,
                DEBUG = True,
            )
    
            data = sw.start()

            if not data == None:
                print data['documents']
        except Exception, e:
            print "ERROR: {0}".format(e)

if __name__ == '__main__':

    print ("Launching BarkingOwl Scraper as daemon process.\n"
           "To shutdown scraper, issue (global) shutdown command.\n")

    StartScraper()
