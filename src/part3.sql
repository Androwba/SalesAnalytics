create role administrator with login password '1234';
grant create on schema public to administrator;
grant select, insert, update, delete on all tables in schema public to administrator;

create role user1 with login password '84218421';
grant select on all tables in schema public to user1;