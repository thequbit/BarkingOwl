####Scraper####

This tool pulls a list of orgs, and then urls within those orgs from the database, and spits out processed 
pdf text, hash, and meta data.


#####Files#####


    pdfimp.py - this scraper will locate all of the PDF's on a website and return a list of their URLs

    dler.py - this file downloads all of the links it gets from pdfimp

    unpdfer.py - this will pre-process and then pdf->text a downloaded pdf

    runscrapper.py - this is the entry point, and interfaces to the database and scrapper.py


#####Dependancies#####

    > pip install pdfminer
    
    > pip install BeautifulSoup4

    > pip install nltk

    > pip install python-mysql

    > pip install python-magic

#####Running the scrapper#####

    > python runscrapper.py
    
This will connect to the database through the sql2api db access layer and pull down all of the URLS that
need to be scrapped.  It will then scrape all of the URLS, find the PDFS, process them, and then push the
results into the database.
    
