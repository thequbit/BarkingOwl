import ConfigParser

config = ConfigParser.ConfigParser()
config.read('config.ini')

__sql_username__ = config.get('sql2api','username')
__sql_password__ = config.get('sql2api','password')
__sql_database__  = config.get('sql2api','database')
__sql_server__ = config.get('sql2api','server')

import MySQLdb as mdb
import _mysql as mysql
try:
    con = mdb.connect(host = __sql_server__,
                      user = __sql_username__,
                      passwd = __sql_password__,
                      db = __sql_database__,
                     )
except Exception,e:
    raise Exception("sql2api error - could not connect to the database:\n\n\t{0}".format(e))


from Users import Users
from Userinfo import Userinfo
from Orgs import Orgs
from Urls import Urls
from Userurls import Userurls
from Phrases import Phrases
from Urlphrases import Urlphrases
from Runs import Runs
from Scraps import Scraps
from Docs import Docs
from Finds import Finds

