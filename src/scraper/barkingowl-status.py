from scraperstatus import ScraperStatus
import simplejson
from flask import Flask
app = Flask(__name__)

@app.route("/status.json")
def getstatus():
    ss = ScraperStatus()
    ss.run()
    status = ss.getstatus()
    #print status 
    return simplejson.dumps(status)

if __name__ == "__main__":
    app.run(host="0.0.0.0")
    hello()
