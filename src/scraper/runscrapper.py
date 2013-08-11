from db.urls import urls
from db.phrases import phrases
from db.docs import docs
from db.urlphrases import urlphrases
from db.finds import finds
from db.scraps import scraps
from db.runs import runs

from scrapper import scrapper

from time import strftime
from time import clock

import os

import pprint

verbose = True

def _report(text):
    if verbose:
        print "[Scrapper] {0}".format(text)

def getcreds():
    return ("lisa.duffnet.local","bouser","password123%%%","barkingowl")

def geturls():
    host,user,passwd,db = getcreds()
    u = urls(host,user,passwd,db)
    _urls = u.getall()
    return _urls

def gethashs():
    host,user,passwd,db = getcreds()
    d = docs(host,user,passwd,db)
    _hashs = d.gethashs()
    return _hashs

def addrun(start,end,success):
    host,user,passwd,db = getcreds()
    r = runs(host,user,passwd,db)
    r.add(start,end,success)

def addscrape(start,end,success,urlid):
    host,user,passwd,db = getcreds()
    s = scraps(host,user,passwd,db)
    s.add(start,success,urlid)

def adddoc(docurl,filename,linktext,dldt,text,dochash,urlid):
    host,user,passwd,db = getcreds()
    d = docs(host,user,passwd,db)
    docid = d.add(docurl,filename,linktext,dldt,text,dochash,urlid)
    return docid

def getphrases(urlid):
    host,user,passwd,db = getcreds()
    p = phrases(host,user,passwd,db)
    _phrases = p.getbyurlid(urlid)
    return _phrases

def savefind(urlphraseid,dt,docid):
    host,user,passwd,db = getcreds()
    f = finds(host,user,passwd,db)
    findid = f.add(urlphraseid,dt,docid)
    return findid

def runscraper(destinationdirectory,linklevel):
    starttime = strftime("%Y-%m-%d %H:%M:%S")
    _report("Started at: {0}".format(starttime))
    retsuccess = True
    _urls = geturls()
    _report("Working on {0} URLs ...".format(len(_urls)))
    for _url in _urls:
        urlid,url,name,desc,dt,cuserid = _url
        _report("Running on: {0}".format(url))
        scrapestarttime = strftime("%Y-%m-%d %H:%M:%S")
        docs,success = scrapper(url,destinationdirectory,linklevel)
        scrapeendtime = strftime("%Y-%m-%d %H:%M:%S") 
        addscrape(scrapestarttime,scrapeendtime,success,urlid)
        if success:
            _report("Found {0} pdf documents.".format(len(docs)))
            for doc in docs:
                hashs = gethashs()
                url,filename,linktext,dldt,text,dochash,tokens = doc
                if not dochash in hashs:
                    _report("Adding doc: {0} [{1}]".format(filename,dochash))
                    docid = adddoc(url,filename,linktext,dldt,text,dochash,urlid)
                    phrases = getphrases(urlid)
                    for p in phrases:
                        phrase,userid,urlphraseid = p
                        if phrase in text:
                            now = strftime("%Y-%m-%d %H:%M:%S")
                            savefind(urlphraseid,now,docid)
                        # TODO: send notification to user
                else:
                    _report("Skipping doc, already processed.")
    endtime = strftime("%Y-%m-%d %H:%M:%S")
    addrun(starttime,endtime,retsuccess)
    return retsuccess

def deletefiles(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except:
            pass

# Taken from http://stackoverflow.com/users/165216/paul-mcguire
#
# http://stackoverflow.com/questions/1557571/how-to-get-time-of-a-python-program-execution
#
def secondsToStr(t):
    return "%d:%02d:%02d.%03d" % \
        reduce(lambda ll,b : divmod(ll[0],b) + ll[1:],
            [(t*1000,),1000,60,60])

def main():
    starttime = clock()
    runscraper('./downloads',3)
    _report("Cleaning up downloaded files.")
    deletefiles('./downloads')
    endtime = clock()
    _report("Scrapper completed in {0}.".format(secondsToStr(endtime-starttime)))
    
main()
