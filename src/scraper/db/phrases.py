import MySQLdb as mdb
import _mysql as mysql
import re

class phrases:

    __settings = {}
    __con = False

    def __init__(self):
        configfile = "./db/sqlcreds.txt"
        f = open(configfile)
        for line in f:
            # skip comment lines
            m = re.search('^\s*#', line)
            if m:
                continue

            # parse key=value lines
            m = re.search('^(\w+)\s*=\s*(\S.*)$', line)
            if m is None:
                continue

            self.__settings[m.group(1)] = m.group(2)
        f.close()

    def __connect(self):
        con = mdb.connect(host=self.__settings['host'], user=self.__settings['username'], passwd=self.__settings['password'], db=self.__settings['database'])
        return con

    def __sanitize(self,valuein):
        if type(valuein) == 'str':
            valueout = mysql.escape_string(valuein)
        else:
            valueout = valuein
        return valuein

    def add(self,phrase,userid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO phrases(phrase,userid) VALUES(%s,%s)",(self.__sanitize(phrase),self.__sanitize(userid)))
            cur.close()
            newid = cur.lastrowid
        con.close()
        return newid

    def get(self,phraseid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM phrases WHERE phraseid = %s",(phraseid))
            row = cur.fetchone()
            cur.close()
        con.close()
        return row

    def getall(self):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM phrases")
            rows = cur.fetchall()
            cur.close()
        _phrases = []
        for row in rows:
            _phrases.append(row)
        con.close()
        return _phrases

    def delete(self,phraseid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM phrases WHERE phraseid = %s",(phraseid))
            cur.close()
        con.close()

    def update(self,phraseid,phrase,userid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE phrases SET phrase = %s,userid = %s WHERE phraseid = %s",(self.__sanitize(phrase),self.__sanitize(userid),self.__sanitize(phraseid)))
            cur.close()
        con.close()

##### Application Specific Functions #####

#    def myfunc():
#        con = self.__connect()
#        with con:
#            cur = son.cursor()
#            cur.execute("")
#            row = cur.fetchone()
#            cur.close()
#        con.close()
#        return row

