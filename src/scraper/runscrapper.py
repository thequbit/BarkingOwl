from sqlalchemy import *
from sqlalchemy.orm import (
    sessionmaker,
    )
from sqlalchemy.ext.declarative import declarative_base

from scrapper import scrapper

from time import strftime
from time import clock
import os
import threading

from models import *

engine = create_engine('mysql://bouser:password123%%%@lisa.duffnet.local', echo=True)
engine.execute("USE barkingowl")
Session = sessionmaker(bind=engine)
DBSession = Session()
Base = declarative_base()
Base.metadata.create_all(engine)


verbose = True

def _report(text):
    if verbose:
        print "[Scrapper] {0}".format(text)

def geturls():
    urls = DBSession.query(UrlModel)
    return urls

def gethashs():
    hashs = DBSession.query(DocModel.dochash).all()
    return hashs

def addrun(start,end,success,urlid):
    run = RunModel(start,end,success,urlid)
    DBSession.add(run)

def adddoc(docurl,filename,linktext,downloaded,created,doctext,dochash,urlid):
    doc = DocModel(docurl,filename,linktext,downloaded,created,doctext,dochash,urlid)
    DBSession.add(doc)
    return doc.id

def scrapurls(urls,destinationdirectory,linklevel):
    retsuccess = True
    #_report("Working on {0} URLs ...".format(len(urls)))
    for url in urls:
        starttime = strftime("%Y-%m-%d %H:%M:%S")
        _report("Started at: {0}".format(starttime))
        _report("Running on: {0}".format(url.url))
        docs,success = scrapper(url.url,destinationdirectory,linklevel)
        if success:
            _report("Found {0} pdf documents.".format(len(docs)))
            for doc in docs:
                hashs = gethashs()
                docurl,filename,linktext,downloaded,created,doctext,dochash,tokens = doc
                if not dochash in hashs:
                    _report("Adding doc: {0} [{1}]".format(filename,dochash))
                    docid = adddoc(docurl,filename,linktext,downloaded,created,doctext,dochash,url.id)
                    #
                    #   NOTE: Removing this section for first rev of site
                    #
                    #phrases = getphrases(urlid)
                    #for p in phrases:
                    #    phrase,userid,urlphraseid = p
                    #    if phrase in text:
                    #        now = strftime("%Y-%m-%d %H:%M:%S")
                    #        savefind(urlphraseid,now,docid)
                    #    # TODO: send notification to user
                else:
                    _report("Skipping doc, already processed.")
        endtime = strftime("%Y-%m-%d %H:%M:%S")
        addrun(starttime,endtime,retsuccess,url.id)
    return retsuccess

def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts]
             for i in range(wanted_parts) ]

def runscraper(destinationdirectory,linklevel):
    allurls = []
    for url in geturls():
        allurls.append(url)
    urllist = split_list(allurls)
    for urls in urllist:
        print type(urllist)
        t = threading.Thread(target=scrapurls, args=(urls,destinationdirectory,linklevel))
        t.start()

def deletefiles(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except:
            pass

# Taken from http://stackoverflow.com/users/165216/paul-mcguire
# http://stackoverflow.com/questions/1557571/how-to-get-time-of-a-python-program-execution
def secondsToStr(t):
    return "%d:%02d:%02d.%03d" % \
        reduce(lambda ll,b : divmod(ll[0],b) + ll[1:],
            [(t*1000,),1000,60,60])

def main():
    linklevel = 5
    _report("Running scraper with a link depth of {0} ...".format(linklevel))
    starttime = clock()
    runscraper('./downloads',linklevel)
    _report("Cleaning up downloaded files.")
    deletefiles('./downloads')
    endtime = clock()
    _report("Scrapper completed in {0}.".format(secondsToStr(endtime-starttime)))
    
main()
