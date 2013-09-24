import os
from pdfimp import pdfimp
from dler import DLer
from unpdfer import UnPDFer
from pprint import pprint

verbose = True

def _report(text):
    if verbose:
        print "[        ] {0}".format(text)

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

def scrapper(url,downloaddirectory,linklevel):
    docs = []
    retsuccess = True
    links = getpdfs(url,linklevel)
    if not os.path.exists(downloaddirectory):
        os.makedirs(downloaddirectory)
    files,success = downloadfiles(links=links,destinationfolder=downloaddirectory)
    if not success:
        #print "unable to download files."
        retsuccess = False
    else:
        for f in files:
            url,filename,linktext,downloaded = f
            created,pdftext,pdfhash,tokens,success = unpdf(filename,True)
            if success:
                docs.append((url,filename,linktext,downloaded,created,pdftext,pdfhash,tokens))
            else:
                #print "pdf->txt unsuccessful"
                retsuccess = False
                break
    return docs,retsuccess

