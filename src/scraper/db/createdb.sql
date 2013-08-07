drop database barkingowl;
create database barkingowl;

grant usage on barkingowl.* to bouser identified by 'password123%%%';

grant all privileges on barkingowl.* to bouser;

use barkingowl;

create table users(
userid int not null auto_increment primary key,
name varchar(256) not null,
login varchar(256) not null,
passhash varchar(256) not null
);

create table urls(
urlid int not null auto_increment primary key,
url text not null,
name varchar(256) not null,
description text,
createdatetime datetime not null,
creationuserid int not null,
foreign key (creationuserid) references users(userid)
);

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

create table phrases(
phraseid int not null auto_increment primary key,
phrase varchar(256) not null,
userid int not null,
foreign key (userid) references users(userid)
);

create table urlphrases(
urlphraseid int not null auto_increment primary key,
userid int not null,
foreign key (userid) references users(userid),
urlid int not null,
foreign key (urlid) references urls(urlid),
phraseid int not null,
foreign key (phraseid) references phrases(phraseid)
);

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

create table docs(
docid int not null auto_increment primary key,
filename text not null,
linktext text not null,
downloaddatetime datetime not null,
doctext text not null,
hash text not null,
urlid int not null,
foreign key (urlid) references urls(urlid)
);

create table finds(
findid int not null auto_increment primary key,
urlphraseid int not null,
foreign key (urlphraseid) references urlphrases(urlphraseid),
finddatetime datetime not null,
docid int not null,
foreign key (docid) references docs(docid)
);
