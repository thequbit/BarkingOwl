import urllib
import urllib2
from time import strftime
import time
from random import randint

class DLer:

    _verbose = False

    def __init__(self,verbose):
        self._verbose = verbose

    def report(self,text):
        if self._verbose:
            print "[DLer    ] {0}".format(text)

    def _fileexists(self,filename):
        exists = False
        if filename == None:
            exists = False
        else:
            try:
                with open(filename):
                    exists = True
                    pass
            except:
                exists = False
        return exists

    def _getisotime(self):
        isotime = strftime("%Y-%m-%d %H:%M:%S")
        return isotime

    def _getdt(self):
        dt = strftime("%Y%m%d%H%M%S")
        return dt

    def _download(self,url,dest):
            success = True
        try: 
            urlfile = url[url.rfind("/")+1:]
            t = time.time() 
            _filename = "{0}/{1}_{2}.download".format(dest,urlfile,t)
            while self._fileexists(_filename):
                _filename = "{0}/{1}_{2}.download".format(dest,urlfile,t)
            #print _filename
            filename,_headers = urllib.urlretrieve(url,_filename)
        except:
            filename = ""
            success = False
        isodatetime = self._getisotime()
        return (filename,isodatetime,success)

    def dl(self,links,destdir):
        retsuccess = True
        files = []
        for _link in links:
            link,text = _link
            self.report("Download '{0}'...".format(link))
            filename,datetime,success = self._download(link,destdir)
            #print "success = {0}".format(success)
            if not success:
                retsuccess = False
                break
            else:
                files.append((link,filename,text,datetime))
        return (files,retsuccess)

