create table UserSessions (
   username  email,
   loggedIn  timestamp,
   loggedOut timestamp
   -- etc. etc.
);

insert into UserSessions(username,loggedIn,loggedOut) values
('jas@cse.unsw.edu.au','2012-07-01 15:45:55','2012-07-01 15:51:20'),
('jas@cse.unsw.EDU.AU','2012-07-01 15:50:30','2012-07-01 15:53:15'),
('z9987654@unsw.edu.au','2012-07-01 15:51:10','2012-07-01 16:01:05'),
('m.mouse@disney.com','2012-07-01 15:51:11','2012-07-01 16:01:06'),
('a-user@fast-money.com','2012-07-01 15:52:25','2012-07-01 16:10:15')
;
--(...XXX..)

-- create the support function too
CREATE FUNCTION pjw(email) RETURNS int4
   AS '/usr/local/postgresql-9.3.4/src/tutorial/email' LANGUAGE C IMMUTABLE STRICT;

-- now we can make the operator class
-------------------------Only 5 operators can be generated, careful!!!!------------
CREATE OPERATOR CLASS email_abs_ops
    DEFAULT FOR TYPE email USING hash AS
        OPERATOR        1       = ,
        FUNCTION        1       pjw(email);


create index on UserSessions using hash (username);

select a.username, a.loggedIn, b.loggedIn
from   UserSessions a, UserSessions b
where  a.username = b.username and a.loggedIn <> b.loggedIn;

select count(*)
from   UserSessions;
--group  by path;
drop table UserSessions;
