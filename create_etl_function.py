import os
import environ
import psycopg2
import datetime
from functions import *

env = environ.Env()
environ.Env.read_env()

DATABASE_SOURCE = env.db('DATABASE_SOURCE')
DATABASE = env.db('DATABASE')
# TABLE_SOURCE
# TABLE_TARGET

table_list = read_csv('table_list.csv')

for t in table_list:
    print(t)

# for t in TABLES:

#     print(t)

#     DATA_TYPE, column_names = execute_sql("""
#         SELECT CASE
#                WHEN data_type IN (
#                     'character varying',
#                     'text')
#                THEN True
#                ELSE False END AS is_text
#           FROM information_schema.columns
#          WHERE table_schema = 'public'
#            AND table_name = '%s'
#            AND column_name NOT IN (
#                 'id',
#                 'created_by_id',
#                 'updated_by_id',
#                 'created_at',
#                 'updated_at')
#            ORDER BY table_name, ordinal_position;""" % t, True, DATABASE)
           
#     DATA_TYPE = process_columns(DATA_TYPE)

#     COLUMNS_TABLE, column_names = execute_sql("""
#         SELECT column_name
#           FROM information_schema.columns
#          WHERE table_schema = 'public'
#            AND table_name ILIKE '%s'
#            AND column_name NOT IN (
#                 'id',
#                 'created_by_id',
#                 'updated_by_id',
#                 'created_at',
#                 'updated_at')
#          ORDER BY table_name, ordinal_position;""" % t, True, DATABASE)
    
#     COLUMNS = process_columns(COLUMNS_TABLE)
#     SELECT = create_select(t, COLUMNS, COLUMNS)
#     INSERT = create_insert(t, COLUMNS, DATA_TYPE)
#     UPDATE = create_update(t, COLUMNS, DATA_TYPE)

#     print()
#     print()
#     print("Processing table %s" % t)

#     ID_EXISTS, column_names = execute_sql("""
#         SELECT id
#           FROM public.%s;""" % t, True, DATABASE)
    
#     ID_EXISTS = process_columns(ID_EXISTS)

#     lista, column_names = execute_sql(SELECT, True, DATABASE_SOURCE)

#     print('Count lines %s: %s' % (t, len(lista)))

#     TEXT = ''
#     last = 'TABLE %s IS EMPTY' % t

#     for l in lista:

#         dic = {}

#         for a in range(len(column_names)):
#             dic[column_names[a]] = str(l[a]).replace("'", "''")

#         dic['table'] = t

#         if int(dic['id']) in ID_EXISTS:

#             TEXT += UPDATE % dic
#             last = 'UPDATE %(table)s %(id)s' % dic

#         else:

#             TEXT += INSERT % dic
#             last = 'INSERT %(table)s %(id)s' % dic

#     execute_sql(
#         TEXT,
#         False, DATABASE)

#     print(last)
