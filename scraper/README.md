###Barking Owl Scraper###
-------------------------

The Scraper is the heart of the BarkingOwl system.  The scraper is the part of the system that actually goes onto
all of the websites that it is pointed to and looks for documents within them.  The scraper takes in a small number
of parameters, but is a powerful tool for scraping websites quickly and efficiently.

There are a few files in this folder:

  - scraper.py
    - Takes in urls and spits out document locations
    - Runs in its own thread.
     
  - lanucher.py
    - Wraps the scraper and allows for communications with the rabbitMQ bus.
    - Runs in its own thread.
     
  - barkingowl_scraper.py
    - Calls and starts the launcher.
    
  - testscraper.py
    - Contains some *very* simple tests for the scraper. 

The simplest way to use the scraper is using the following:

    # import BarkingOwl scraper
    from BarkingOwl.scraper.scraper import Scraper
    
    # import the uuid lib so we can give our scraper a unique ID
    import uuid

    def finished():
        print "done."
        
    def started():
        print "starting."
        
    def docfound(payload):
    
        """
        {
            'sourceid': 'a0351229-3ebd-42b2-add7-4dea1012f96b', 
            'destinationid': 'broadcast', 
            'command': 'found_doc', 
            'message': {
                'scrapedatetime': '2014-02-03 22:47:25', 
                'docurl': u'http://targeturl.com/somedoc.pdf', 
                'urldata': {
                    'maxlinklevel': 1, 
                    'doctype': 'application/pdf', 
                    'targeturl': 'http://targetwebsite.com',
                    'myfield': 'anything I want!',
                }, 
                'linktext': u'A PDF Document'
            }
        }
        """
        
        print payload;

    if __name__ == '__main__':

        # create a unique id for our scraper
        uid = str(uuid.uuid4())
    
        # create an instance of our scraper
        scraper = Scraper(uid=uid)

        # set our scraper callbacks
        scraper.setFinishedCallback(
            finishedCallback=finished,         # called when the scraper has finished scraping the entire site
            startedCallback=started,           # called when the scraper successfully starts
            broadcastDocCallback=docfound, # called every time a document is found
        )
    
        # create the URL payload that will be sent to the 
        urldata = {
            'targeturl': "http://targetwebsite.com", # the url you are trying to scrape
            'maxlinklevel': 1,                       # the number of link levels to follow 
                                                     # note: anything over 3 can get really dangerious ...
            'doctype': 'application/pdf',            # the type of document to search for based on the magic number lib 
            'myfield': 'anything I want!',           # note: the pyaload dictionary can have any additional fields
                                                     #       within it you would like.
        }

        # set the payload within the scraper
        self.scraper.seturldata(urldata)
    
        # start the scraper thread
        self.scraper.start()
    
        # begin scraping!
        try:
            scraper.begin()
        except:
            # an exception will be thrown when the scraper is exited.
            pass
            
        print "all done."
        
    
This code will call the broadcastDocCallback function everytime a document is found on the website by the scraper.

