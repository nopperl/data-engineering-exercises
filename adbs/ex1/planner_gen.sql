drop table if exists r,s,t,u;
create table r as select (random()*50)::int as a, (random()*50)::int as b, (random()*50)::int as c
       from generate_series(1, 500000);
create table s as select (random()*100)::int as d, (random()*100)::int as b, (random()*100)::int as c
       from generate_series(1, 200000);
create table t as select (random()*50)::int as a, (random()*50)::int as d, (random()*50)::int as c
       from generate_series(1, 20000);
create table u as select (random()*20)::int as a, (random()*20)::int as b, (random()*20)::int as d
       from generate_series(1, 20000);
analyze r;
analyze s;
analyze t;
analyze u;
