CREATE DATABASE IF NOT EXISTS bodispatcherdb;

USE bodispatcherdb;

grant usage on bodispatcherdb.* to bouser identified by 'password123%%%';
grant all privileges on bodispatcherdb.* to bouser;

CREATE TABLE IF NOT EXISTS urls(
    urlid INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    targeturl TEXT NOT NULL,
    maxlinklevel INT NOT NULL,
    creationdatetime DATETIME NOT NULL,
    doctypeid INT NOT NULL,
    FOREIGN KEY (doctypeid) REFERENCES doctypes(doctypeid)
);

CREATE INDEX urls_urlid ON urls(urlid);

CREATE TABLE IF NOT EXISTS doctypes(
    doctypeid INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    title CHAR(256) NOT NULL,
    description TEXT NOT NULL,
    doctype CHAR(256) NOT NULL
);

CREATE INDEX doctypes_doctypeid ON doctypes(doctypeid);
CREATE INDEX doctypes_doctype ON doctypes(doctype);
