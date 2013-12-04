from flask import Flask
from flask import render_template
from flask import request
import simplejson
import subprocess
from time import strftime
import sys
import signal
from multiprocessing import Process

from models import *

from status import Status

app = Flask(__name__)
app.template_folder = "templates"
app.static_folder = "static"
app.debug = True

status = None
server = None
dispatcherlaunched = False

@app.route('/addurl', methods=['GET'])
def add():
        print "Entering add()"
        success = True
        urlid = 0
    #try:
        # read values from URL
        targeturl = request.args['targeturl']
        maxlinklevel = request.args['maxlinklevel']
        
        isodatetime = strftime("%Y-%m-%d %H:%M:%S")
        # add url to database
        urls = Urls()
        urlid = urls.add(targeturl=targeturl,
            maxlinklevel=maxlinklevel,
            creationdatetime=isodatetime,
            doctypeid=1, # application/pdf
        )
    #except:
    #    success = False
        print "Exiting add()"
        ret = {}
        ret['success'] = success
        ret['urlid'] = urlid
        return simplejson.dumps(ret)

@app.route('/deleteurl', methods=['GET'])
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

@app.route('/launchscraper', methods=['GET'])
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

@app.route('/systemshutdown', methods=['GET'])
def systemshutdown():
    success = True
    subprocess.Popen(['python','../tools/globalshutdown.py'])
    ret = {}
    ret['success'] = success
    return simplejson.dumps(ret)

@app.route('/scraperstatus')
def status():
    print "Entering status()"
    success = True
    json = []
    #try:
    stats = status.getstatus()
    #json = simplejson.dumps(stats)
    #print "STATUS:\n{0}".format(json)
    #except:
    #    success = False
    #    status = []
    print "Exiting status()"
    ret = {}
    ret['success'] = success
    ret['status'] = stats
    return simplejson.dumps(ret)

@app.route('/jquery-1.10.2.min.js')
def jquery():
    return render_template('jquery-1.10.2.min.js')

@app.route('/datetime.format.js')
def datetimeformat():
    return render_template('datetime.format.js')

@app.route('/')
def index():
    print "Entering index()"
    print "Exiting index()"
    return render_template('index.html')

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

def signal_handler(signal, frame):
    print "hi."
    status.stop()
    sys.exit()

if __name__ == "__main__":
    print "Application Starting ..."
    
    status = Status()
    
    signal.signal(signal.SIGINT, signal_handler)

    status.start()

    host = '0.0.0.0'
    port = 8080
    app.run(host=host, port=port)

