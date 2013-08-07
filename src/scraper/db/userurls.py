import MySQLdb as mdb
import _mysql as mysql
import re

class userurls:

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

    def add(self,urlid,userid,name,notes,adddatetime):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO userurls(urlid,userid,name,notes,adddatetime) VALUES(%s,%s,%s,%s,%s)",(self.__sanitize(urlid),self.__sanitize(userid),self.__sanitize(name),self.__sanitize(notes),self.__sanitize(adddatetime)))
            cur.close()
            newid = cur.lastrowid
        con.close()
        return newid

    def get(self,userurlid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM userurls WHERE userurlid = %s",(userurlid))
            row = cur.fetchone()
            cur.close()
        con.close()
        return row

    def getall(self):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM userurls")
            rows = cur.fetchall()
            cur.close()
        _userurls = []
        for row in rows:
            _userurls.append(row)
        con.close()
        return _userurls

    def delete(self,userurlid):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM userurls WHERE userurlid = %s",(userurlid))
            cur.close()
        con.close()

    def update(self,userurlid,urlid,userid,name,notes,adddatetime):
        con = self.__connect()
        with con:
            cur = con.cursor()
            cur.execute("UPDATE userurls SET urlid = %s,userid = %s,name = %s,notes = %s,adddatetime = %s WHERE userurlid = %s",(self.__sanitize(urlid),self.__sanitize(userid),self.__sanitize(name),self.__sanitize(notes),self.__sanitize(adddatetime),self.__sanitize(userurlid)))
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

