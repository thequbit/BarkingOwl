import os
from pdfimp import pdfimp
from dler import DLer
from unpdfer import UnPDFer
from pprint import pprint

def unpdf(filename,scrub):
    unpdfer = UnPDFer()
    pdftext,pdfhash,tokens,success = unpdfer.unpdf(filename=filename,SCRUB=scrub)
    return pdftext,pdfhash,tokens,success

def downloadfiles(links,destinationfolder):
    dler = DLer()
    files,success = dler.dl(links,destinationfolder)
    return files,success

def getpdfs(url,maxlevels):
    imp = pdfimp()
    links = imp.getpdfs(maxlevel=maxlevels,siteurl=url,links=[(url,url)])
    return links

def scrapper(url,downloaddirectory):
    docs = []
    retsuccess = True
    links = getpdfs(url,1)
    if not os.path.exists(downloaddirectory):
        os.makedirs(downloaddirectory)
    files,success = downloadfiles(links=links,destinationfolder=downloaddirectory)
    if not success:
        #print "unable to download files."
        retsuccess = False
    else:
        for f in files:
            filename,linktext,datetime = f
            pdftext,pdfhash,tokens,success = unpdf(filename,True)
            if success:
                docs.append((filename,linktext,datetime,pdftext,pdfhash,tokens))
            else:
                #print "pdf->txt unsuccessful"
                retsuccess = False
                break
    return docs,retsuccess

scrapper('http://www.scottsvilleny.org/','./downloads')
