drop database barkingowl;
create database barkingowl;

grant usage on barkingowl.* to bouser identified by 'password123%%%';

grant all privileges on barkingowl.* to bouser;

use barkingowl;

create table users(
userid int not null auto_increment primary key,
name varchar(256) not null,
login varchar(256) not null,
passhash varchar(256) not null,
lastlogin datetime
);

create index users_name on users(name);
create index users_login on users(login);

create table userinfo(
userinfoid int not null,
userid int not null,
foreign key (userid) references users(userid),
enabled bool not null,
email varchar(256),
twitter varchar(256),
phone varchar(256),
pushinterval int not null
);

create index userinfo_userid on userinfo(userid);
create index userinfo_email on userinfo(email);
create index userinfo_twitter on userinfo(twitter);
create index userinfo_phone on userinfo(phone);

create table urls(
urlid int not null auto_increment primary key,
url text not null,
name varchar(256) not null,
description text,
createdatetime datetime not null,
creationuserid int not null,
foreign key (creationuserid) references users(userid)
);

create index urls_name on urls(name);
create index urls_creationuserid on urls(creationuserid);

create table userurls(
userurlid int not null auto_increment primary key,
urlid int not null,
foreign key (urlid) references urls(urlid),
userid int not null,
foreign key (userid) references users(userid),
name varchar(256) not null,
notes text,
adddatetime datetime not null
);

create index userurls_urlid on userurls(urlid);
create index userurls_userid on userurls(userid);

create table phrases(
phraseid int not null auto_increment primary key,
phrase varchar(256) not null,
userid int not null,
foreign key (userid) references users(userid)
);

create index phrases_phrase on phrases(phrase);
create index phrases_userid on phrases(userid);

create table urlphrases(
urlphraseid int not null auto_increment primary key,
userid int not null,
foreign key (userid) references users(userid),
urlid int not null,
foreign key (urlid) references urls(urlid),
phraseid int not null,
foreign key (phraseid) references phrases(phraseid)
);

create index urlphrases_userid on urlphrases(userid);
create index urlphrases_urlid on urlphrases(urlid);
create index urlphrases_phraseid on urlphrases(phraseid);

create table runs(
runid int not null auto_increment primary key,
startdatetime datetime not null,
enddatetime datetime not null,
success bool not null
);

create table scraps(
scrapid int not null auto_increment primary key,
scrapdatetime datetime not null,
success bool not null,
urlid int not null,
foreign key (urlid) references urls(urlid)
);

create index scraps_urlid on scraps(urlid);

create table docs(
docid int not null auto_increment primary key,
docurl text not null,
filename text not null,
linktext text not null,
downloaddatetime datetime not null,
doctext text not null,
dochash text not null,
urlid int not null,
foreign key (urlid) references urls(urlid)
);

create index docs_urlid on docs(urlid);

create table finds(
findid int not null auto_increment primary key,
urlphraseid int not null,
foreign key (urlphraseid) references urlphrases(urlphraseid),
finddatetime datetime not null,
docid int not null,
foreign key (docid) references docs(docid)
);

create index finds_urlphraseid on finds(urlphraseid);
create index finds_docid on finds(docid);


