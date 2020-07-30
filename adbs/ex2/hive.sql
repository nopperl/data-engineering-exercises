-- create db

drop database if exists e_test;
create database e_test;
use e_test;

-- a)

drop table badges;
create table badges (
id bigint,
class int,
date_ date,
name string,
tagbased boolean,
userid bigint
)
row format delimited 
fields terminated by ','
lines terminated by '\n'
tblproperties ("skip.header.line.count"="1");
load data local inpath "/home/adbs20/shared/stackexchange/badges.csv" into table badges;

drop table comments;
create table comments (
id bigint,
creationdate string,
postid bigint,
score int,
text string,
userdisplayname string,
userid bigint
)
row format delimited 
fields terminated by ','
lines terminated by '\n'
tblproperties ("skip.header.line.count"="1");
load data local inpath "/home/adbs20/shared/stackexchange/comments.csv" into table comments;

drop table posts;
create table posts(
id bigint,
acceptedanswerid int,
answercount int,
body string,
closeddate date,
commentcount int,
communityowneddate string,
creationdate string,
favoritecount int,
lastactivitydate string,
lasteditdate string,
lasteditordisplayname string,
lasteditoruserid bigint,
ownerdisplayname string,
owneruserid bigint,
parentid bigint,
posttypeid bigint,
score int,
tags string,
title string,
viewcount int
)
row format delimited 
fields terminated by ','
lines terminated by '\n'
tblproperties ("skip.header.line.count"="1");
load data local inpath "/home/adbs20/shared/stackexchange/posts.csv" into table posts;

drop table postlinks;
create table postlinks(
id bigint,
creationdate date,
linktypeid bigint,
postid bigint,
relatedpostid bigint
)
row format delimited 
fields terminated by ','
lines terminated by '\n'
tblproperties ("skip.header.line.count"="1",
'timestamp.formats'='yyyy-MM-dd\'T\'HH:mm:ss.SSS');
load data local inpath "/home/adbs20/shared/stackexchange/postlinks.csv" into table postlinks;

drop table users;
create table users(
id bigint,
aboutme string,
accountid bigint,
creationdate string,
displayname string,
downvotes int,
lastaccessdate string,
location_ string,
profileimageurl string,
reputation int,
upvotes int,
views_ int,
websiteurl string
)
row format delimited 
fields terminated by ','
lines terminated by '\n'
tblproperties ("skip.header.line.count"="1");
load data local inpath "/home/adbs20/shared/stackexchange/users.csv" into table users;

drop table votes;
create table votes(
id bigint,
bountyamount int,
creationdate string,
postid bigint,
userid bigint,
votetypeid bigint
)
row format delimited 
fields terminated by ','
lines terminated by '\n'
tblproperties ("skip.header.line.count"="1");
load data local inpath "/home/adbs20/shared/stackexchange/votes.csv" into table votes;

-- b)

set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 1000;
set hive.exec.max.dynamic.partitions = 1000;
set hive.optimize.bucketmapjoin = true;
set hive.exec.dynamic.partition.mode = nonstrict;

drop table users_partitioned;
create table users_partitioned (id bigint) partitioned by (reputation int);
insert into table users_partitioned partition (reputation = 101) select id from users where reputation > 100;

create table users_partitioned ( 
id bigint, 
aboutme string, 
accountid bigint, 
creationdate string, 
displayname string, 
downvotes int, 
lastaccessdate string, 
location_ string, 
profileimageurl string, 
upvotes int, 
views_ int, 
websiteurl string 
) partitioned by (reputation int);
insert into table users_partitioned partition (reputation = 101) select id, aboutme, accountid, creationdate, displayname, downvotes, lastaccessdate, location_, profileimageurl, upvotes, views_, websiteurl from users where reputation > 100;

EXPLAIN SELECT p.posttypeid, COUNT(p.id), COUNT(c.id) FROM posts p, comments c, users_partitioned u
WHERE c.postid=p.id AND u.id=p.owneruserid AND u.reputation > 100
AND NOT EXISTS (SELECT * FROM postlinks l WHERE l.relatedpostid = p.id)
GROUP BY p.posttypeid;

drop table posts_bucketed;
create table posts_bucketed (id bigint, posttypeid bigint, owneruserid bigint)
clustered by (owneruserid) sorted by (owneruserid asc) into 524 buckets;
insert into posts_bucketed select id, posttypeid, owneruserid from posts;

EXPLAIN SELECT p.posttypeid, COUNT(p.id), COUNT(c.id) FROM posts_bucketed p, comments c, users_partitioned u
WHERE c.postid=p.id AND u.id=p.owneruserid AND u.reputation > 100
AND NOT EXISTS (SELECT * FROM postlinks l WHERE l.relatedpostid = p.id)
GROUP BY p.posttypeid;

drop table posts_bucketed_2;
create table posts_bucketed_2 (id bigint, posttypeid bigint, owneruserid bigint)
clustered by (posttypeid) sorted by (posttypeid asc) into 280 buckets;
insert into posts_bucketed_2 select id, posttypeid, owneruserid from posts;

EXPLAIN SELECT p.posttypeid, COUNT(p.id), COUNT(c.id) FROM posts_bucketed_2 p, comments c, users_partitioned u
WHERE c.postid=p.id AND u.id=p.owneruserid AND u.reputation > 100
AND NOT EXISTS (SELECT * FROM postlinks l WHERE l.relatedpostid = p.id)
GROUP BY p.posttypeid;

drop table posts_partitioned_bucketed;
create table posts_partitioned_bucketed (id bigint, owneruserid bigint)
partitioned by (posttypeid bigint)
clustered by (owneruserid) sorted by (owneruserid asc) into 524 buckets;
insert into table posts_partitioned_bucketed partition(posttypeid) select id, owneruserid, posttypeid from posts where posttypeid regexp '^[0-9]+$';

EXPLAIN SELECT p.posttypeid, COUNT(p.id), COUNT(c.id) FROM posts_partitioned_bucketed p, comments c, users_partitioned u
WHERE c.postid=p.id AND u.id=p.owneruserid AND u.reputation > 100
AND NOT EXISTS (SELECT * FROM postlinks l WHERE l.relatedpostid = p.id)
GROUP BY p.posttypeid;

-- c)
SELECT p.id FROM posts p, comments c, users u, users u2, badges b, postlinks l
WHERE c.postid=p.id AND u.id=p.owneruserid
AND u.id = b.userid
AND l.relatedpostid > p.id
AND u2.creationdate > p.creationdate
AND b.name LIKE 'Research Assistant'
group by p.id, u.id, u.upvotes HAVING u.upvotes = MAX(u2.upvotes);
