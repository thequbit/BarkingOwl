import time
from time import strftime
from time import clock
import re
import os
import threading

from pdfimp import pdfimp
from dler import DLer
from unpdfer import UnPDFer


# sql2api db access 
from models import *

verbose = True

class Scrapper(threading.Thread):

    verbose = True

    def __init__(self,threadnumber,orgname,orgurls,destdir,linklevel):

        threading.Thread.__init__(self)

        self.threadnumber = threadnumber
        self.orgname = orgname
        self.orgurls = orgurls
        self.destdir = destdir
        self.linklevel = linklevel

        self.imp = pdfimp(self.verbose)

        self.urls= Urls()
        self.orgs = Orgs()
        self.scrapes = Scrapes()
        self.docs = Docs()

    def run(self):
        self.r("Thread #{0} Loaded".format(self.threadnumber))
        self.scrapeurls(self.orgname,
                        self.orgurls,
                        self.destdir,
                        self.linklevel
        )
        self.r("Thread #{0} Exiting".format(self.threadnumber))

    def r(self,text):
        if self.verbose:
            print "[Scrapper {0}] {1}".format(self.threadnumber,text)

    def unpdf(self,filename,scrub):
        unpdfer = UnPDFer(verbose)
        created,pdftext,pdfhash,tokens,success,exceptiontext = unpdfer.unpdf(filename=filename,SCRUB=scrub)
        return created,pdftext,pdfhash,tokens,success,exceptiontext

    def downloadfiles(self,links,destinationfolder):
        dler = DLer(verbose)
        files,success = dler.dl(links,destinationfolder)
        return files,success

    def getpdfs(self,url,maxlevels):
        #imp = pdfimp(verbose)
        links,linkcount = self.imp.getpdfs(maxlevel=maxlevels,siteurl=url,links=[(url,url)])
        return links,linkcount

    #def getorgs(self):
    #orgs = Orgs()
    #    allorgs = self.orgs.getall()
    #return allorgs

    #def geturls(self,orgid):
    #    #urls = Urls()
    #    #orgurls = urls.byorgid(orgid)
    #    orgurls = []
    #    orgurls.append(self.urls.get(25))
    #    orgurls.append(self.urls.get(10))
    #    return orgurls

    def gethashs(self,urlid):
        #docs = Docs()
        hashs = self.docs.gethashs(urlid) 
        return hashs

    def addscrape(self,orgid,start,end,success,urlid,linkcount):
        #scrapes = Scrapes()
        scrapeid = self.scrapes.add(orgid,start,end,success,urlid,linkcount)
        return scrapeid

    def adddoc(self,orgid,docurl,filename,linktext,downloaddatetime,
               creationdatetime,doctext,dochash,urlid,processed):
        #docs = Docs()
        docid = self.docs.add(orgid,docurl,filename,linktext,downloaddatetime,
                              creationdatetime,doctext,dochash,urlid,processed)
        return docid



    def scrapper(self,url,downloaddirectory,linklevel):
        docs = []
        links,linkcount = self.getpdfs(url,linklevel)
        self.r("Processed {0} links, found {1} PDF documents from {2}".format(linkcount,len(links),url))
        for link in links:
            u,n = link
            self.r("    {0}".format(u))
        if not os.path.exists(downloaddirectory):
            os.makedirs(downloaddirectory)
        files,success = self.downloadfiles(links=links,destinationfolder=downloaddirectory)
        #eport("Downloaded {0} files with a status of {1}".format(len(files),success)
        if not success:
            self.r("ERROR: Unable to download all files.")
            retsuccess = False
        else:
            self.r("Processing PDF documents for URL:")
            self.r("    {0}".format(url))
            for f in files:
                url,filename,linktext,downloaded = f
                created,pdftext,pdfhash,tokens,success,exceptiontext = self.unpdf(filename,True)
                if success:
                    #docs.append((url,filename,linktext,downloaded,created,pdftext,pdfhash,tokens))
                    processed = True
                else:
                    #retsuccess = False
                    self.r("ERROR: unpdf returned the following error: {0}".format(exceptiontext))
                    processed = False
                docs.append((url,filename,linktext,downloaded,created,pdftext,pdfhash,tokens,processed))
                    #
                    # TODO: Log exceptiontext
                    #
                    #break
        return docs,linkcount

    def scrapeurls(self,orgname,orgurls,destinationdirectory,linklevel):
        
        time.sleep(1)

        #self.r("orgurls = {0}".format(orgurls))

        retsuccess = True
        self.r("Working on {0} URLs for '{1}'...".format(len(orgurls),orgname))
        for _url in orgurls:
            urlid,orgid,url,urlname,description,createdatettime,creationuserid = _url
            starttime = strftime("%Y-%m-%d %H:%M:%S")
            self.r("Scrapper Started at: {0}".format(starttime))
            self.r("Running on: '{0}'".format(urlname))
            self.r("    {0}".format(url))
            self.r("")
            docs,linkcount = self.scrapper(url,destinationdirectory,linklevel)
            #if success:
            #self.r("Found {0} PDF documents".format(len(docs)))
            for doc in docs:
                hashs = self.gethashs(urlid)
                docurl,filename,linktext,downloaddatetime,creationdatetime,doctext,dochash,tokens,processed = doc
                #self.r("this hash: {0}".format(dochash))
                #self.r("doc hashs: {0}".format(hashs))
                if not dochash in hashs or dochash == "":
                    self.r("Pushing PDF to database:")
                    self.r("\t{0}".format(docurl))
                    #self.r("\t{0}".format(filename))
                    #docid = adddoc(docurl,filename,linktext,downloaded,created,doctext,dochash,url.id)
                   
                    # cleanup the text a bit.  get rid of repeating spaces and \n to save DB space
                    doctext = re.sub(' +',' ',doctext)
                    doctext = re.sub('\n+','\n',doctext)

                    # prevents 'None' from being inserted into the database
                    if linktext == None:
                        linktext = ""

                    docid = self.adddoc(orgid,docurl,filename,linktext,downloaddatetime,
                                        creationdatetime,doctext,dochash,urlid,processed)

                else:
                    self.r("Skipping PDF, already processed.")
            endtime = strftime("%Y-%m-%d %H:%M:%S")
            self.addscrape(orgid,starttime,endtime,retsuccess,urlid,linkcount)
        return retsuccess

    #def split_list(alist, wanted_parts=1):
    #    length = len(alist)
    #    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts]
    #             for i in range(wanted_parts) ]

    def deletefiles(self,folder):
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except:
                pass

    def deletefile(self,filename):
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except:
            pass


lol = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]

def getorgs():
    orgs = Orgs()
    allorgs = orgs.getall()
    return allorgs

def geturls(orgid):
    urls = Urls()
    orgurls = urls.byorgid(orgid)
    #orgurls = []
    #orgurls.append(urls.get(25))
    #orgurls.append(urls.get(10))
    return orgurls


def runscrapper(destdir,linklevel):
    # get all of the organizations from the database and pull the urls for each of them
    orgs = getorgs()
    thrds = []
    report("Working on {0} Organizations.".format(len(orgs)))
    for org in orgs:
        orgid,orgname,description,creationdatetime,ownerid = org
        report("Dispatching threads for '{0}'".format(orgname))
        orgurls = geturls(orgid)
        threadcount = 8
        parts = lol(orgurls,threadcount-2)
        #parts = []
        #parts.append(urls[0])
        #parts.append(urls[1])
        #print parts
        i = 1
        for part in parts:
            #print "part #{0} = {1}".format(i,part)
            report("Launching Thread ...")
            thrd = Scrapper(i,orgname,part,destdir,linklevel)
            thrd.start()
            thrds.append(thrd)
            report("Successfully Launched Thread #{0}".format(i)) 
            i += 1
    for thrd in thrds:
        thrd.join()

        #print "Found {0} urls".format(len(urls))
        #srapurls(orgname,urls,destinationdirectory,linklevel)

# Taken from http://stackoverflow.com/users/165216/paul-mcguire
# http://stackoverflow.com/questions/1557571/how-to-get-time-of-a-python-program-execution
def secondsToStr(t):
    return "%d:%02d:%02d.%03d" % \
        reduce(lambda ll,b : divmod(ll[0],b) + ll[1:],
            [(t*1000,),1000,60,60])

def report(text):
    print "[Scrapper] {0}".format(text)

def main():
    linklevel = 1
    report("Running scraper with a link depth of {0} ...".format(linklevel))
    starttime = clock()
    runscrapper('./downloads',linklevel)
    #report("Cleaning up downloaded files.")
    #deletefiles('./downloads')
    endtime = clock()
    report("Scrapper completed in {0}.".format(secondsToStr(endtime-starttime)))
    
main()
