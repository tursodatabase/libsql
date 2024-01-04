.echo on
pragma cipher='sqlcipher';
pragma key='test2';
create table t1 (c1 int, c2 char);
insert into t1 values (1,'Alf');
insert into t1 values (2,'Bert');
insert into t1 values (3,'Cecil');
insert into t1 values (4,'Donald');
insert into t1 values (5,'Ernie');
select * from t1;
.q
