import os
import psycopg2
import datetime
import environ

env = environ.Env()
environ.Env.read_env()

SOURCE_DATABASE = env.db('SOURCE_DATABASE')
TARGET_DATABASE = env.db('TARGET_DATABASE')


def read_csv(filename):
    import csv
    with open(filename) as csv_file:
        csv_reader = csv.reader(
            csv_file, delimiter=',')
        line_count = 0
        lista = []
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                lista.append([row[0], row[1]])
                line_count += 1
        print(f'Processed {line_count} lines.')
    return lista


def execute_sql(select, array, database):
    try:
        conn = psycopg2.connect("user='%(USER)s' host='%(HOST)s' port='%(PORT)s' password='%(PASSWORD)s' dbname='%(NAME)s'" % database)
        conn.autocommit = True
    except:
        print("I am unable to connect to the database")
    if select:
        cur = conn.cursor()
        select = select.replace("None", 'Null').replace("'Null'", 'Null')
        cur.execute(select)
        column_names = []
        if array:
            column_names = [desc[0] for desc in cur.description]
            lista = cur.fetchall()
        else:
            lista = None
        cur.close()
        return lista, column_names
    else:
        return None, None


def execute(table, select, fields, insert, update):
    print()
    print('ETL %s' % table)
    fields = fields.replace(" ", "").split(',')

    
    exists_command, _ = execute_sql('SELECT id FROM %s;' % table, True, TARGET_DATABASE)
    exists = []
    for e in exists_command: 
        exists.append(e[0])
    data, _ = execute_sql(select, True, SOURCE_DATABASE)
    command = ''
    last = ''
    for a in data:
        dicion = {}
        for n in range(len(fields)):
            dicion[fields[n]] = str(a[n]).replace("'", "''")
        if int(dicion['id']) in exists:
            command += str(update % dicion).replace("='',", "=NULL,")
            #print('U')
            #execute_sql(command, False, TARGET_DATABASE)
            last = 'UPDATE %(id)s' % dicion
        else:
            command += str(insert % dicion).replace(", '',", ", NULL,")
            #print('I')
            #execute_sql(command, False, TARGET_DATABASE)
            last = 'INSERT %(id)s' % dicion
    print(last)
    execute_sql(command, False, TARGET_DATABASE)


def save_file(file, content):
    import codecs
    file = codecs.open(file, "w", "utf-8")
    file.write(content)
    file.close()


def read_file(file):
    import codecs
    file = codecs.open(file, "r", "utf-8")
    content = file.read()
    file.close()
    return content.encode('utf-8')
    

def reset_sequences(name, DATABASE):
    lista, column_names = execute_sql(
        """select table_name from information_schema.tables 
            where table_schema='public';""",
        True, DATABASE)
    for a in lista:
        try:
            b, column_names = execute_sql("""SELECT max(id)+1 FROM %s;""" % (a[0]), True, DATABASE)
            print("""ALTER SEQUENCE %s_id_seq RESTART WITH %s;""" % (a[0], b[0][0] or 1))
            execute_sql("""ALTER SEQUENCE %s_id_seq RESTART WITH %s;""" % (a[0], b[0][0] or 1), False, DATABASE)
        except:
            print('ERROR %s ...' % a[0])


def process_columns(list_old):
    list_new = []
    for lo in list_old:
        list_new.append(lo[0])
    return list_new


def create_select(schema_name, table_name, columns_new):
    data_types = []
    n = range(len(columns_new))
    for a in n:
        data_types.append(columns_new[a])
    return """SELECT id, %s FROM %s.%s ORDER BY id; """ % (
               ', '.join(data_types), 
               schema_name, 
               table_name)


def create_insert(schema_name, table_name, columns, data_type):
    data_types = []
    n = range(len(columns))
    for a in n:
        if data_type[a]:
            data_types.append("'#(%s)s'" % columns[a])
        else:
            data_types.append("#(%s)s" % columns[a])
    INSERT = """INSERT INTO %s.%s (id, %s, created_at, created_by_id) VALUES ( #(id)s, %s, NOW(), NULL);""" % (schema_name, table_name, ', '.join(columns), ', '.join(data_types))
    return INSERT.replace('#', '%')


def create_update(schema_name, table_name, columns, data_type):
    data_types = []
    n = range(len(columns))
    for a in n:
        if data_type[a]:
            data_types.append(""+columns[a]+"='%("+columns[a]+")s'")
        else:
            data_types.append(""+columns[a]+"=%("+columns[a]+")s")
    UPDATE = """UPDATE %s.%s SET %s WHERE id=#(id)s;""" % (
        schema_name, table_name, ', '.join(data_types))
    return UPDATE.replace('#', '%')
