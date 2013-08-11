BarkingOwl
==========

scraper + pyramid web app that pulls docs off websites and produces stats and notifications about them

####Background and Description####

Barking Owl came out of the need presented at a Hacks and Hackers Rochester (#hhroc) meet-up in Syracuse, NY.
A journalist expressed his need for a tool that would assist him in looking for key words within PDFs posted
to town websites, such as meeting minutes.

Barking Owl has two primary parts:

    - Website scrapper
        This tool is pointed to a website, and returns information about the PDF documents it finds there
        
    - Web portal to scrapped data
        This presents a simple web app to interface to the data found by the website scrapper

The web portal runs on pyramid, a python web framework.  The website scraper is built using three stand-alone tools:

    - pdfimp ( http://github.com/thequbit/pdfimp )
    - dler ( http://github.com/thequbit/dler )
    - unpdfer ( http://github.com/thequbit/unpdfer )

These tools go out onto a site and:

    1. Find all of the PDFs
    2. Download all of the PDFs
    3. Convert the PDFs to readable text (if possible, note: no OCR is used within this step)
    4. Produce a histogram of words used, the processed PDF text, the date of download, and a unique hash 
    of the PDF binary.

The resulting data is then pushed to a database, where the web front-end can interface with the collected data.
Barking Owl allows for a user to add a URL to scrape, as well as a phrase to look for when scrapping that URL.
An example of this would be to look at the town of Brighton, NY's website and find all of the PDFs.  Then each
time a PDF is found, notify the user if the phrase 'fracking' is found within it.

    Name: The town of Brighton, NY
    URL: http://www.townofbrighton.org/
    Phrase: fracking

####Installing and Running Barking Owl####

TODO: this.
