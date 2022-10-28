SQL_TO_GET_DATABASES = "SELECT datname FROM pg_catalog.pg_database"

SQL_TO_GET_SCHEMAS = "SELECT schema_name FROM information_schema.schemata order by 1"

#SQL_TO_GET_TABLES = "SELECT schemaname,tablename FROM pg_catalog.pg_tables where schemaname in ({}) order by 1,2"
SQL_TO_GET_TABLES = """SELECT table_schema, table_name, pg_relation_size('"'||table_schema||'"."'||table_name||'"') size
FROM information_schema.tables 
where table_type='BASE TABLE' and table_schema in ({}) order by 1,2"""

SQL_TO_GET_FUNCTIONS = """select n.nspname as function_schema,
       p.proname as function_name,
       case when l.lanname = 'internal' then p.prosrc
            else pg_get_functiondef(p.oid)
            end as definition
from pg_proc p
left join pg_namespace n on p.pronamespace = n.oid
left join pg_language l on p.prolang = l.oid
where n.nspname in ({})
order by function_schema,
         function_name;"""

SQL_TO_GET_VIEWS = """select table_schema as schema_name,
       table_name as view_name,
       view_definition
from information_schema.views
where table_schema in ({})
order by schema_name,
         view_name;"""