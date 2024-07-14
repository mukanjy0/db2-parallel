create schema lab14;
set search_path to lab14;
show search_path;

/*
 ==================
 |       P0       |
 ==================
 */

drop table if exists estudiante;
create table estudiante (
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

create table estudiante_Lima partition of estudiante for values in ('Lima');
create table estudiante_Callao partition of estudiante for values in ('Callao');

insert into estudiante (DNI, Nombre, Ciudad, Grupo, Promedio, Edad, Sexo) values
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
    IF NOT EXISTS (SELECT Ciudad FROM estudiante WHERE Ciudad = quote_literal(NEW.Ciudad))
    THEN
        nombre := 'estudiante_' || NEW.Ciudad;
        RAISE NOTICE '%', nombre;
        EXECUTE 'create table ' || nombre || ' partition of estudiante for values in (' || quote_literal(NEW.Ciudad) || ');';
    END IF;
    RAISE NOTICE '%', quote_literal(NEW);
    EXECUTE 'insert into estudiante select ($1).*' using NEW;
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
select * from estudiante;

/*
==================
|       P2       |
==================
 */
 -- POSTGRES_FDW IS CASE SENSITIVE
drop table if exists estudiante;
create table estudiante (
    DNI char(4) not null,
    Nombre varchar not null,
    Ciudad varchar(20) not null,
    Grupo char(1),
    Promedio float,
    Edad smallint,
    Sexo char(1)
    )
partition by list (Ciudad);


-- execute for each remote server
create database remote_db; -- connect to remote_db afterwards
create schema remote_schema;

SELECT * FROM pg_catalog.pg_tables where tablename like '%estudiante%';

-- local
create table estudiante_callao partition of estudiante for values in ('Callao');
create table estudiante_junin partition of estudiante for values in ('Junin');

-- set up schema locally
create schema local_schema;
set search_path to local_schema;
show search_path ;

-- execute for remote servers and local server
create extension postgres_fdw;

/*
==================
|     Remote1     |
==================
 */

drop server remote1 cascade;
create server remote1 foreign data wrapper postgres_fdw options
    (host 'host.docker.internal', dbname 'remote_db', port '5433'); -- using docker for local_db too

create user mapping for current_user
    server remote1
    options (user 'postgres', password 'ihavethepower');

import foreign schema remote_schema
    from server remote1
    into local_schema;

-- estudiante_Lima
create foreign table local_schema.estudiante_lima
    partition of local_schema.estudiante for values in ('Lima')
    server remote1
    options (schema_name 'remote_schema', table_name 'estudiante_lima');

-- connect to remote1
drop table if exists estudiante_lima;
create table estudiante_lima (
    DNI char(4) not null,
    Nombre varchar not null,
    Ciudad varchar(20) not null,
    Grupo char(1),
    Promedio float,
    Edad smallint,
    sexo char(1)
);

-- estudiante_Tacna
create foreign table local_schema.estudiante_tacna
    partition of local_schema.estudiante for values in ('Tacna')
    server remote1
    options (schema_name 'remote_schema', table_name 'estudiante_tacna');

-- connect to remote1
drop table if exists estudiante_tacna;
create table estudiante_tacna (
    DNI char(4) not null,
    Nombre varchar not null,
    Ciudad varchar(20) not null,
    Grupo char(1),
    Promedio float,
    Edad smallint,
    Sexo char(1)
);

/*
==================
|     Remote2     |
==================
 */

-- configure connection with remote2
drop server remote2 cascade;
create server remote2 foreign data wrapper postgres_fdw options
    (host 'host.docker.internal', dbname 'remote_db', port '5434'); -- using docker for local_db too

create user mapping for current_user
    server remote2
    options (user 'postgres', password 'ihavethepower');

import foreign schema remote_schema
    from server remote2
    into local_schema;

-- estudiante_Arequipa
create foreign table local_schema.estudiante_arequipa
    partition of local_schema.estudiante for values in ('Arequipa')
    server remote1
    options (schema_name 'remote_schema', table_name 'estudiante_arequipa');

-- connect to remote2
drop table if exists estudiante_arequipa;
create table estudiante_arequipa (
    DNI char(4) not null,
    Nombre varchar not null,
    Ciudad varchar(20) not null,
    Grupo char(1),
    Promedio float,
    Edad smallint,
    sexo char(1)
);

-- estudiante_Trujillo
create foreign table local_schema.estudiante_trujillo
    partition of local_schema.estudiante for values in ('Trujillo')
    server remote1
    options (schema_name 'remote_schema', table_name 'estudiante_trujillo');

-- connect to remote2
drop table if exists estudiante_trujillo;
create table estudiante_trujillo (
    DNI char(4) not null,
    Nombre varchar not null,
    Ciudad varchar(20) not null,
    Grupo char(1),
    Promedio float,
    Edad smallint,
    Sexo char(1)
);

/*
==================
|     Testing    |
==================
 */

-- checking
select * from pg_foreign_server;

insert into local_schema.estudiante (DNI, Nombre, Ciudad, Grupo, Promedio, Edad, Sexo) values
    ('0001', 'Selene Aguirre', 'Lima', 'A', 8.5, 17, 'F'),
    ('0002', 'Martin Porres', 'Lima', 'C', 9, 23, 'M'),
    ('0003', 'Miriam Gutierrez', 'Callao', 'A', 7, 21, 'F'),
    ('0004', 'Benito Lopez', 'Callao', 'B', 10, 19, 'M'),
    ('0006', 'Lisa Porres', 'Junin', 'B', 9, 24, 'M'),
    ('0008', 'Gabriel Lopez', 'Tacna', 'A', 12, 19, 'M');

explain analyse
select * from estudiante;