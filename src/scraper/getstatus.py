import pika
import simplejson

from models import *

#def sanitizedate(url):
#        urlid,orgid,url,urlname,description,createdatetime,creationuserid,linklevel = url
#        createdatetimeiso = createdatetime.strftime("%Y-%m-%d %H:%M:%S")
#        returl = (urlid,orgid,url,urlname,description,createdatetimeiso,creationuserid,linklevel)
#        return returl

respcon = pika.BlockingConnection(pika.ConnectionParameters(
                                     host='localhost'))

respchan = respcon.channel()
respchan.exchange_declare(exchange='barkingowl',
                              type='fanout')

#orgname = "Monroe County, NY"

#urls = Urls()
#url = urls.get(31)

# create the url payload
#payload = {'command': 'url_payload',
#           'scrapperid': '1183764364',#response['scrapperid'],
#           'orgname': orgname,
#           'url_json': simplejson.dumps(sanitizedate(url))}


body = {
    'command': 'request_status',
    'scraperid': '0'
}

jbody = simplejson.dumps(body)



 # send the message
respchan.basic_publish(exchange='barkingowl',
                       routing_key='', #self._queuename,
                       body=jbody,
                      )

