from flask import Flask
from flask import render_template
from flask import request
import simplejson
import subprocess
from time import strftime

from models import *

from status import Status

app = Flask(__name__)
app.template_folder = "templates"
app.debug = True

status = None
dispatcherlaunched = False

@app.route('/addurl', methods=['POST'])
def add():
    print "Entering add()"
    success = False
    urlid = 0
    try:
        targeturl = request.form['targeturl']
        maxlinklevel = request.form['maxlinklevel']
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        # add url to database
        urls = Urls()
        urlid = urls.add(targeturl=targeturl,
            maxlinklevel=maxlinklevel,
            creationdatetime=isodatetime,
            doctypeid=1, # application/pdf
        )
    except:
        success = False
    print "Exiting add()"
    ret = {}
    ret['success'] = success
    ret['urlid'] = urlid
    return simplejson.dumps(ret)

@app.route('/deleteurl', methods=['POST'])
def delete():
    print "Entering delete()"
    success = True
    try:
        urlid = request.args['urlid']
        urls = Urls()
        urls.delete(urlid)
    except:
        success = False
    print "Exiting delete()"
    ret = {}
    ret['success'] = success 
    return simplejson.dumps(ret)

@app.route('/launchdispatcher')
def launchdispatcher():
    print "Entering launchdispatcher()"
    success = True
    try:
        if dispatcherlaunched == False:
            subprocess.Popen(["python","../dispatcher/barkingowl-dispatcher.py"])
            dispatcherlaunched == True
        else:
            success = False
    except:
        success = False
    print "Exiting launchdispatcher()"
    ret = {}
    ret['success'] = success
    ret['launched'] = dispatcherlaunched
    return simplejson.dumps(ret)

@app.route('/launchscraper', methods=['GET','POST'])
def launchscraper():
    #print "Entering launchscraper()"
        success = True
    #try:
        count = int(request.args['count'])
        for i in range(0,count):
            subprocess.Popen(['python','../scraper/barkingowl-scraper.py'])
    #except:
    #    success = False
    #print "Exiting launchscraper()"
        ret = {}
        ret['success'] = success
        return simplejson.dumps(ret)

@app.route('/status.json')
def status():
    print "Entering status()"
    success = True
    json = "{}"
    try:
        json = simplejson.dumps(status.status)
        print "STATUS:\n{0}".format(json)
    except:
        success = False
    print "Exiting status()"
    ret = {}
    ret['success'] = success
    ret['status'] = status.status
    return simplejson.dumps(ret)

@app.route('/')
def index():
    print "Entering index()"
    print "Exiting index()"
    return render_template('index.html')

if __name__ == "__main__":
    print "Application Starting ..."
    
    status = Status()
    status.start()

    host = '0.0.0.0'
    port = 8080
    app.run(host=host, port=port)

