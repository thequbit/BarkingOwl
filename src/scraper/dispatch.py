from models import *

import fedmsg

verbose = True

lol = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]

def report(text):
    if verbose:
        print "[DISPATCH] {0}".format(text)

def getorgs():
    orgs = Orgs()
    allorgs = orgs.getall()
    return allorgs

def geturls(orgid):
    urls = Urls()
    orgurls = urls.byorgid(orgid)
    return orgurls

#def split(orgs,scrappercount):
#    sz = len(orgs)/scrappercount;
#    lol = lambda orgs, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]
#    return lol

def main():
    report("Launching BarkingOwl Dispatcher (powered by fedmsg!).")

    #
    # TODO: make this configurable, or auto detect or something ...
    # 
    scrappercount = 4

    orgs = getorgs()

    report("Seeing {0} Organizations within the database.".format(len(orgs)))

    for org in orgs:
        orgid,orgname,description,creationdatetime,ownerid = org
        urls = geturls(orgid)
        report("Seeing {0} URLs within '{1}'".format(len(urls),orgname))

        parts = lol(urls,len(urls)/scrappercount)

        index = 0
        for part in parts:
            report("Dispatching {0} URLs to waiting scrappers ...".format(len(part)))
            fedmsg.publish(topic='barkingowl.dispatch', msg = {
                'orgid': orgid,
                'orgname': orgname,
                'desc': description,
                'urls': part,
                'index': index,
                'payloadsize': len(parts),
            })
            index += 1

        report("All URLs dispatched for '{0}'".format(orgname))

    report("All Organizations and URLs dispatched successfully.  Exiting.")

main()
