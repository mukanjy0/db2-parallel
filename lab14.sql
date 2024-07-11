create schema lab14;
set search_path to lab14;
show search_path;

/*
 ==================
 |       P0       |
 ==================
 */

drop table if exists Estudiante;
create table Estudiante (
    DNI char(4) not null,
    Nombre varchar not null,
    Ciudad varchar(20) not null,
    Grupo char(1),
    Promedio float,
    Edad smallint,
    Sexo char(1)
)
partition by list (Ciudad);

drop table if exists Aux;
create table Aux (
    DNI char(4) not null,
    Nombre varchar not null,
    Ciudad varchar(20) not null,
    Grupo char(1),
    Promedio float,
    Edad smallint,
    Sexo char(1)
);

create table Estudiante_Lima partition of Estudiante for values in ('Lima');
create table Estudiante_Callao partition of Estudiante for values in ('Callao');

insert into Estudiante (DNI, Nombre, Ciudad, Grupo, Promedio, Edad, Sexo) values
    ('0001', 'Selene Aguirre', 'Lima', 'A', 8.5, 17, 'F'),
    ('0002', 'Martin Porres', 'Lima', 'C', 9, 23, 'M'),
    ('0003', 'Miriam Gutierrez', 'Callao', 'A', 7, 21, 'F'),
    ('0004', 'Benito Lopez', 'Callao', 'B', 10, 19, 'M');

/*
==================
|       P1       |
==================
 */

CREATE OR REPLACE FUNCTION update_estudiante()
RETURNS TRIGGER AS $$
DECLARE
    nombre varchar;
BEGIN
    IF NOT EXISTS (SELECT Ciudad FROM Estudiante WHERE Ciudad = quote_literal(NEW.Ciudad))
    THEN
        nombre := 'Estudiante_' || NEW.Ciudad;
        RAISE NOTICE '%', nombre;
        EXECUTE 'create table ' || nombre || ' partition of Estudiante for values in (' || quote_literal(NEW.Ciudad) || ');';
    END IF;
    RAISE NOTICE '%', quote_literal(NEW);
    EXECUTE 'insert into Estudiante select ($1).*' using NEW;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_update_estudiante
BEFORE INSERT ON Aux
FOR EACH ROW
EXECUTE PROCEDURE update_estudiante();

insert into Aux (DNI, Nombre, Ciudad, Grupo, Promedio, Edad, Sexo) values
    ('0005', 'Selene Aguirre', 'Ayacucho', 'A', 8.5, 17, 'F'),
    ('0006', 'Martin Porres', 'Junin', 'C', 9, 23, 'M'),
    ('0007', 'Miriam Gutierrez', 'Arequipa', 'A', 7, 21, 'F'),
    ('0008', 'Benito Lopez', 'Tacna', 'B', 10, 19, 'M');

delete from Aux;
select * from Estudiante;

/*
==================
|       P2       |
==================
 */

-- execute for each remote server
create database remote_db; -- connect to remote_db afterwards
create schema remote_schema;

-- set up schema locally
create schema local_schema;
set search_path to local_schema;
show search_path ;

-- execute for remote servers and local server
create extension postgres_fdw;

-- configure connection with remote1
drop server remote1 cascade;
create server remote1 foreign data wrapper postgres_fdw options
(host 'host.docker.internal', dbname 'remote_db', port '5433'); -- using docker for local_db too

create user mapping for current_user
server remote1
options (user 'postgres', password 'ihavethepower');

import foreign schema remote_schema
    from server remote1
    into local_schema;

-- configure connection with remote2
create server remote2 foreign data wrapper postgres_fdw options
    (host 'host.docker.internal', dbname 'remote_db', port '5434'); -- using docker for local_db too

create user mapping for current_user
    server remote2
    options (user 'postgres', password 'ihavethepower');

import foreign schema remote_schema
    from server remote2
    into local_schema;

-- checking
select * from pg_foreign_server;

-- local_schema
drop table if exists Estudiante;
create table Estudiante (
    DNI char(4) not null,
    Nombre varchar not null,
    Ciudad varchar(20) not null,
    Grupo char(1),
    Promedio float,
    Edad smallint,
    Sexo char(1)
)
partition by list (Ciudad);

drop table if exists Aux;
create table Aux (
     DNI char(4) not null,
     Nombre varchar not null,
     Ciudad varchar(20) not null,
     Grupo char(1),
     Promedio float,
     Edad smallint,
     Sexo char(1)
);

create table it (
    n_servers smallint not null,
    cur_server smallint not null
);
insert into it (n_servers, cur_server) values (3,0);

CREATE OR REPLACE FUNCTION update_estudiante()
    RETURNS TRIGGER AS $$
DECLARE
    nombre varchar;
    n smallint := (select n_servers from it);
    i smallint := (select cur_server from it);
BEGIN
    IF NOT EXISTS (SELECT Ciudad FROM Estudiante WHERE Ciudad = quote_literal(NEW.Ciudad))
    THEN
        nombre := 'Estudiante_' || NEW.Ciudad;
        IF i = 0::smallint
        THEN
            EXECUTE 'create table local_schema.' || nombre || ' partition of Estudiante for values in (' || quote_literal(NEW.Ciudad) || ');';
        ELSE
            EXECUTE 'create foreign table local_schema.' || nombre
                        || ' partition of local_schema.Estudiante for values in (' || quote_literal(NEW.Ciudad) || ')'
                        || ' server remote' || i::text
                        || ' options (schema_name ''remote_schema'', table_name ''' || nombre || ''');';
        END IF;
        i := mod(i + 1, n);
        EXECUTE 'update it set cur_server = ' || i::text || ' where n_servers = ' || n::text || ';';
    END IF;
    -- RAISE NOTICE '%', quote_literal(NEW);
    EXECUTE 'insert into local_schema.Estudiante select ($1).*' using NEW;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_update_estudiante
    BEFORE INSERT ON Aux
    FOR EACH ROW
EXECUTE PROCEDURE update_estudiante();

/*
==================
|     Testing    |
==================
 */

insert into Aux (DNI, Nombre, Ciudad, Grupo, Promedio, Edad, Sexo) values
    ('0001', 'Selene Aguirre', 'Lima', 'A', 8.5, 17, 'F'),
    ('0002', 'Martin Porres', 'Lima', 'C', 9, 23, 'M'),
    ('0003', 'Miriam Gutierrez', 'Callao', 'A', 7, 21, 'F'),
    ('0004', 'Benito Lopez', 'Callao', 'B', 10, 19, 'M'),
    ('0005', 'Willy Aguirre', 'Ayacucho', 'C', 8, 17, 'F'),
    ('0006', 'Lisa Porres', 'Junin', 'B', 9, 24, 'M'),
    ('0007', 'Lucia Gutierrez', 'Arequipa', 'A', 7, 21, 'F'),
    ('0008', 'Gabriel Lopez', 'Tacna', 'A', 12, 19, 'M');


select * from it;
update it set cur_server = 1 where n_servers = 3;
explain analyse
select * from Estudiante;