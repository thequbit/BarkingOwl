from flask import Flask
from flask import render_template
from flask import request

from time import strftime

from models import *

app = Flask(__name__)
app.template_folder = "templates"
app.debug = True

def addurl(targeturl,maxlinklevel=1):
    isodatetime = strftime("%Y-%m-%d %H:%M:%S")
    urls = Urls()
    urls.add(targeturl=targeturl,
             maxlinklevel=maxlinklevel,
             creationdatetime=isodatetime,
             doctypeid=1, # application/pdf
            )

@app.route('/add', methods=['POST'])
def add():
    if request.method == 'POST':
        targeturl = request.form['targeturl']
        maxlinklevel = request.form['maxlinklevel']
        addurl(targeturl,maxlinklevel)
    return "URL Added Successfully."

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == "__main__":
    host = '0.0.0.0'
    port = 8080
    app.run(host=host, port=port)
