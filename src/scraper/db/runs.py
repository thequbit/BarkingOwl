import MySQLdb as mdb
import _mysql as mysql
import re

class runs:

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

    def add(self,startdatetime,enddatetime,success):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO runs(startdatetime,enddatetime,success) VALUES(%s,%s,%s)",(self.__sanitize(startdatetime),self.__sanitize(enddatetime),self.__sanitize(success)))
            cur.close()
            newid = cur.lastrowid
        con.close()
        return newid

    def get(self,runid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM runs WHERE runid = %s",(runid))
            row = cur.fetchone()
            cur.close()
        con.close()
        return row

    def getall(self):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM runs")
            rows = cur.fetchall()
            cur.close()
        _runs = []
        for row in rows:
            _runs.append(row)
        con.close()
        return _runs

    def delete(self,runid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM runs WHERE runid = %s",(runid))
            cur.close()
        con.close()

    def update(self,runid,startdatetime,enddatetime,success):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE runs SET startdatetime = %s,enddatetime = %s,success = %s WHERE runid = %s",(self.__sanitize(startdatetime),self.__sanitize(enddatetime),self.__sanitize(success),self.__sanitize(runid)))
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

