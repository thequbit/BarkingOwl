from db.urls import urls
from db.phrases import phrases
from db.docs import docs
from db.urlphrases import urlphrases
from db.finds import finds
from db.scraps import scraps
from db.runs import runs

from scrapper import scrapper

from time import strftime

def geturls():
    u = urls()
    _urls = u.getall()
    return _urls

def addrun(start,end,success):
    r = runs()
    r.add(start,end,success)

def addscrape(start,end,success,urlid):
    s = scraps()
    s.add(start,success,urlid)

def adddoc(filename,linktext,dldt,text,hash,urlid):
    d = docs()
    d.add(filename,linktext,dldt,text,hash,urlid)

def runscraper(destinationdirectory,linklevel):
    starttime = strftime("%Y-%m-%d %H:%M:%S")
    print "Started at: {0}".format(starttime)
    retsuccess = True
    _urls = geturls()
    for _url in _urls:
        urlid,url,name,desc,dt,userid = _url
        print "Running on: ".format(url)
        scrapestarttime = strftime("%Y-%m-%d %H:%M:%S")
        docs,success = scrapper(url,destinationdirectory,linklevel)
        scrapeendtime = strftime("%Y-%m-%d %H:%M:%S") 
        addscrape(scrapestarttime,scrapeendtime,success,urlid)
        if success:
            for doc in docs:
                filename,linktext,dldt,text,hash,tokens = doc
                print "Adding doc:".format(filename)
                adddoc(filename,linktext,dldt,text,hash,urlid)
                # TODO: pull all phrases for this url and check for them
    endtime = strftime("%Y-%m-%d %H:%M:%S")
    addrun(starttime,endtime,retsuccess)
    return retsuccess

def main():
    runscraper('./downloads',1)

main()
