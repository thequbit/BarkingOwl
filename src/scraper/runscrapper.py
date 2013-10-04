#from sqlalchemy import *
#from sqlalchemy.orm import (
#    sessionmaker,
#    )
#from sqlalchemy.ext.declarative import declarative_base

#from scrapper import scrapper

from time import strftime
from time import clock
import os
import threading

from pdfimp import pdfimp
from dler import DLer
from unpdfer import UnPDFer


# sql2api db access 
from models import *

#engine = create_engine('mysql://bouser:password123%%%@lisa.duffnet.local', echo=True)
#engine.execute("USE barkingowl")
#Session = sessionmaker(bind=engine)
#DBSession = Session()
#Base = declarative_base()
#Base.metadata.create_all(engine)

verbose = True

def report(text):
    if verbose:
        print "[Scrapper] {0}".format(text)

def unpdf(filename,scrub):
    unpdfer = UnPDFer(verbose)
    created,pdftext,pdfhash,tokens,success = unpdfer.unpdf(filename=filename,SCRUB=scrub)
    return created,pdftext,pdfhash,tokens,success

def downloadfiles(links,destinationfolder):
    dler = DLer(verbose)
    files,success = dler.dl(links,destinationfolder)
    return files,success

def getpdfs(url,maxlevels):
    imp = pdfimp(verbose)
    links = imp.getpdfs(maxlevel=maxlevels,siteurl=url,links=[(url,url)])
    return links

def getorgs():
    orgs = Orgs()
    allorgs = orgs.getall()
    return allorgs

def geturls(orgid):
    urls = Urls()
    orgurls = urls.byorgid(orgid)
    return orgurls

def gethashs(urlid):
    docs = Docs()
    hashs = docs.gethashs(urlid) 
    return hashs

def addscrape(orgid,start,end,success,urlid):
    scrapes = Scrapes()
    scrapeid = scrapes.add(orgid,start,end,success,urlid)
    return scrapeid

def adddoc(orgid,docurl,filename,linktext,downloaddatetime,creationdatetime,doctext,dochash,urlid):
    docs = Docs()
    docid = docs.add(orgid,docurl,filename,linktext,downloaddatetime,creationdatetime,doctext,dochash,urlid)
    return docid

def scrapper(url,downloaddirectory,linklevel):
    docs = []
    retsuccess = True
    links = getpdfs(url,linklevel)
    if not os.path.exists(downloaddirectory):
        os.makedirs(downloaddirectory)
    files,success = downloadfiles(links=links,destinationfolder=downloaddirectory)
    #print "Downloaded {0} files with a status of {1}".format(len(files),success)
    if not success:
        #print "unable to download files."
        retsuccess = False
    else:
        for f in files:
            url,filename,linktext,downloaded = f
            created,pdftext,pdfhash,tokens,success = unpdf(filename,True)
            #print "done with pdf, with sucess of '{0}'".format(success)
            if success:
                docs.append((url,filename,linktext,downloaded,created,pdftext,pdfhash,tokens))
            else:
                #print "pdf->txt unsuccessful"
                retsuccess = False
                break
    return docs,retsuccess

def scrapurls(orgname,urls,destinationdirectory,linklevel):
    retsuccess = True
    report("Working on {0} URLs for '{1}'...".format(len(urls),orgname))
    for _url in urls:
        urlid,orgid,url,urlname,description,createdatettime,creationuserid = _url
        starttime = strftime("%Y-%m-%d %H:%M:%S")
        report("Started at: {0}".format(starttime))
        report("Running on: '{0}'".format(urlname))
        report("    {0}".format(url))
        docs,success = scrapper(url,destinationdirectory,linklevel)
        if success:
            report("Found {0} pdf documents.".format(len(docs)))
            for doc in docs:
                hashs = gethashs(urlid)
                docurl,filename,linktext,downloaddatetime,creationdatetime,doctext,dochash,tokens = doc
                if not dochash in hashs:
                    report("Pushing to database doc: {0} [{1}]".format(filename,dochash))
                    #docid = adddoc(docurl,filename,linktext,downloaded,created,doctext,dochash,url.id)
                    
                    # cleanup the text a bit.  get rid of repeating spaces and \n to save DB space
                    doctext = re.sub(' +',' ',doctext)
                    doctext = re.sub('\n+','\n',doctext)

                    docid = adddoc(orgid,docurl,filename,linktext,downloaddatetime,creationdatetime,doctext,dochash,urlid)

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
                    report("Skipping doc, already processed.")
        else:
            # TODO: fail more eligantly than this ...
            retsuccess = False
            break
        endtime = strftime("%Y-%m-%d %H:%M:%S")
        addscrape(orgid,starttime,endtime,retsuccess,urlid)
    return retsuccess

#def split_list(alist, wanted_parts=1):
#    length = len(alist)
#    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts]
#             for i in range(wanted_parts) ]

def runscraper(destinationdirectory,linklevel):
    #allurls = []
    #for url in geturls():
    #    allurls.append(url)
    #urllist = split_list(allurls)
    #for urls in urllist:
    #    print type(urllist)
    #    t = threading.Thread(target=scrapurls, args=(urls,destinationdirectory,linklevel))
    #    t.start()

    # get all of the organizations from the database and pull the urls for each of them
    orgs = getorgs()
    for org in orgs:
        orgid,orgname,description,creationdatetime,ownerid = org
        print "[{0}] {1}:".format(orgid,orgname)
        urls = geturls(orgid)
        #print "Found {0} urls".format(len(urls))
        scrapurls(orgname,urls,destinationdirectory,linklevel)

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
    linklevel = 1
    report("Running scraper with a link depth of {0} ...".format(linklevel))
    starttime = clock()
    runscraper('./downloads',linklevel)
    report("Cleaning up downloaded files.")
    deletefiles('./downloads')
    endtime = clock()
    report("Scrapper completed in {0}.".format(secondsToStr(endtime-starttime)))
    
main()
