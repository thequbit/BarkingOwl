BarkingOwl
==========

Scalable web crawler.

BarkingOwl is a scalable web crawler intended to be used to find specific document types such as PDFs.

####Background and Description####

Barking Owl came out of the need presented at a Hacks and Hackers Rochester (#hhroc) meet-up in Syracuse, NY.
A journalist expressed his need for a tool that would assist him in looking for key words within PDFs posted
to town websites, such as meeting minutes.

####Objective####

I wanted to make the code for this project as reusable as possible as I knew it had several parallels to other
work I had been doing and wanted to do in the future.  The solution was a architecture that would allow for 
significant scalability and extensibility.

####How to get started####

BarkingOwl is on teh pypi network, thus it can be installed using pip:

    > pip install barking_owl

####Stand Alone Scraper####

Once installed, BarkingOwl can be used in a number of ways.  First, the scraper can stand alone and be invoked with no interaction with the message bus.  This method is convient for use with a single website that needs to be scraped for specific document types, or if a site outline needs to be established.

####Dispatched Scrapers####

The other method of using the BarkingOwl infastruction is to leverage the massive scalability of the message bus.  BarkingOwl leverages the fexability, speed, and scalability of the RabitMQ (0mq) system.  A single Dispatcher instance can be launched and dispatch URLs to be scraped to any number of scrapers (hundreds or thousands if so need).

The system has been tested by it's author, [Timothy Duffy](https://github.com/thequbit) with 256 scapers successfully.

####Documentation####

Dispatcher documentation can be found here: [README.md](https://github.com/thequbit/BarkingOwl/tree/master/barking_owl/dispatcher/README.md)

Scraper documentation can be found here: [README.md](https://github.com/thequbit/BarkingOwl/blob/master/barking_owl/scraper/README.md)

