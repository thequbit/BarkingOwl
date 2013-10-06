create database if not exists barkingowl;

use barkingowl;

create table if not exists users(
userid int not null auto_increment primary key,
name varchar(256) not null,
login varchar(256) not null,
passhash varchar(256) not null,
lastlogin datetime
);

create index users_userid on users(userid);
create index users_name on users(name);
create index users_login on users(login);
create index users_passhash on users(passhash);

create table if not exists userinfo(
userinfoid int not null,
userid int not null,
foreign key (userid) references users(userid),
enabled bool not null,
email varchar(256),
twitter varchar(256),
phone varchar(256),
pushinterval int not null
);

create index userinfo_userinfoid on userinfo(userinfoid);
create index userinfo_userid on userinfo(userid);
create index userinfo_email on userinfo(email);
create index userinfo_twitter on userinfo(twitter);
create index userinfo_phone on userinfo(phone);

create table if not exists orgs(
orgid int not null auto_increment primary key,
name varchar(256) not null,
description text not null,
creationdatetime datetime not null,
ownerid int not null,
foreign key (ownerid) references users(userid)
);

create index orgs_orgid on orgs(orgid);
create index orgs_name on orgs(name);

create table if not exists urls(
urlid int not null auto_increment primary key,
orgid int not null,
foreign key (orgid) references orgs(orgid),
url text not null,
name varchar(256) not null,
description text,
createdatetime datetime not null,
creationuserid int not null,
foreign key (creationuserid) references users(userid)
);

create index urls_urlid on urls(urlid);
create index urls_name on urls(name);
create index urls_creationuserid on urls(creationuserid);

create table if not exists userurls(
userurlid int not null auto_increment primary key,
urlid int not null,
foreign key (urlid) references urls(urlid),
userid int not null,
foreign key (userid) references users(userid),
name varchar(256) not null,
notes text,
adddatetime datetime not null
);

create index userurls_userurlid on userurls(userurlid);
create index userurls_urlid on userurls(urlid);
create index userurls_userid on userurls(userid);

create table if not exists phrases(
phraseid int not null auto_increment primary key,
phrase varchar(256) not null,
userid int not null,
foreign key (userid) references users(userid)
);

create index phrases_phraseid on phrases(phraseid);
create index phrases_phrase on phrases(phrase);
create index phrases_userid on phrases(userid);

create table if not exists urlphrases(
urlphraseid int not null auto_increment primary key,
userid int not null,
foreign key (userid) references users(userid),
urlid int not null,
foreign key (urlid) references urls(urlid),
phraseid int not null,
foreign key (phraseid) references phrases(phraseid)
);

create index urlphrases_urlphraseid on urlphrases(urlphraseid);
create index urlphrases_userid on urlphrases(userid);
create index urlphrases_urlid on urlphrases(urlid);
create index urlphrases_phraseid on urlphrases(phraseid);

create table if not exists runs(
runid int not null auto_increment primary key,
startdatetime datetime not null,
enddatetime datetime not null,
success bool not null
);

create table if not exists scrapes(
scrapeid int not null auto_increment primary key,
orgid int not null,
foreign key (orgid) references orgs(orgid), 
startdatetime datetime not null,
enddatetime datetime not null,
success bool not null,
urlid int not null,
foreign key (urlid) references urls(urlid),
linkcount int not null
);

create index scrapes_scrapid on scrapes(scrapeid);
create index scrapes_startdatetime on scrapes(startdatetime);
create index scrapes_enddatetime on scrapes(enddatetime);
create index scrapes_urlid on scrapes(urlid);

create table if not exists docs(
docid int not null auto_increment primary key,
orgid int not null,
foreign key (orgid) references orgs(orgid),
docurl text not null,
filename text not null,
linktext text not null,
downloaddatetime datetime not null,
creationdatetime datetime,
doctext longtext not null,
dochash text not null,
urlid int not null,
foreign key (urlid) references urls(urlid),
processed bool not null
);

create index docs_docid on docs(docid);
create index docs_urlid on docs(urlid);
create index docs_creationdatetime on docs(creationdatetime);
create index docs_downloaddatetime on docs(downloaddatetime);

create table if not exists finds(
findid int not null auto_increment primary key,
urlphraseid int not null,
foreign key (urlphraseid) references urlphrases(urlphraseid),
finddatetime datetime not null,
docid int not null,
foreign key (docid) references docs(docid)
);

create index finds_findid on finds(findid);
create index finds_urlphraseid on finds(urlphraseid);
create index finds_docid on finds(docid);


