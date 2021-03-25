import os
import environ
import psycopg2
import datetime
from functions import *

env = environ.Env()
environ.Env.read_env()


SOURCE_DATABASE = env.db('SOURCE_DATABASE')
TARGET_DATABASE = env.db('TARGET_DATABASE')

TABLES = read_csv('table_list.csv')


VAR = """

table = '%(schema)s.%(table)s'
select = "%(select)s"
fields = "id, %(fields)s"
insert = "%(insert)s"
update = "%(update)s"

execute(table, select, fields, insert, update)

print()
"""

dados = {}
dados['source_database'] = SOURCE_DATABASE
dados['target_database'] = TARGET_DATABASE

content = """from functions import execute

SOURCE_DATABASE = %(source_database)s
TARGET_DATABASE = %(target_database)s

""" % dados


for t in TABLES:

    if '.' in t[0]:
        a = t[0].split('.')
        SOURCE_SCHEMA = a[0]
        SOURCE_TABLE = a[1]
    else:
        SOURCE_SCHEMA = 'public'
        SOURCE_TABLE = t[0]

    if '.' in t[1]:
        a = t[1].split('.')
        TARGET_SCHEMA = a[0]
        TARGET_TABLE = a[1]
    else:
        TARGET_SCHEMA = 'public'
        TARGET_TABLE = t[1]

    DATA_TYPE, TARGET_COLUMNS_NAMES = execute_sql("""
        SELECT CASE
               WHEN data_type IN (
                    'boolean',
                    'integer')
               THEN False
               ELSE True END AS is_text
          FROM information_schema.columns
         WHERE table_schema = '%s'
           AND table_name = '%s'
           AND column_name NOT IN (
               'id', 'created_by_id', 'updated_by_id',
               'created_at', 'updated_at')
         ORDER BY table_name, column_name;""" % (
               TARGET_SCHEMA, TARGET_TABLE), 
            True, TARGET_DATABASE)

    DATA_TYPE = process_columns(DATA_TYPE)

    SOURCE_COLUMNS_TABLE, SOURCE_COLUMNS_NAMES = execute_sql("""
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = '%s'
           AND table_name ILIKE '%s'
           AND column_name NOT IN (
               'id', 'foto', 'created_by_id', 'updated_by_id',
               'created_at', 'updated_at')
         ORDER BY table_name, column_name;""" % (
             SOURCE_SCHEMA, SOURCE_TABLE), 
            True, SOURCE_DATABASE)  

    TARGET_COLUMNS_TABLE, TARGET_COLUMNS_NAMES = execute_sql("""
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = '%s'
           AND table_name ILIKE '%s'
           AND column_name NOT IN (
               'id', 'created_by_id', 'updated_by_id',
               'created_at', 'updated_at')
         ORDER BY table_name, column_name;""" % (
             TARGET_SCHEMA, TARGET_TABLE), 
            True, TARGET_DATABASE)
    
    SOURCE_COLUMNS = process_columns(SOURCE_COLUMNS_TABLE)
    TARGET_COLUMNS = process_columns(TARGET_COLUMNS_TABLE)

    SELECT = create_select(
        SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMNS)

    INSERT = create_insert(
        TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMNS, DATA_TYPE)

    UPDATE = create_update(
        TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMNS, DATA_TYPE)

    dados['schema'] = TARGET_SCHEMA
    dados['table'] = TARGET_TABLE
    dados['select'] = SELECT
    dados['fields'] = ', '.join(TARGET_COLUMNS)
    dados['insert'] = INSERT
    dados['update'] = UPDATE

    content += VAR % dados
    print("Processing table %s" % TARGET_TABLE)

save_file('etl.py', content)
