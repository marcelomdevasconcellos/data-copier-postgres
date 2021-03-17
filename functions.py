import os
import psycopg2
import datetime
import pandas as pd


def read_csv(filename):
    import pandas as pd
    return pd.read_csv(filename)


def execute_sql(select, array, database):
    try:
        conn = psycopg2.connect("user='%(USER)s' host='%(HOST)s' port='%(PORT)s' password='%(PASSWORD)s' dbname='%(NAME)s'" % database)
        conn.autocommit = True
    except:
        print("I am unable to connect to the database")
    if select:
        cur = conn.cursor()
        select = select.replace("None", 'Null') #.replace("'Null'", 'Null')
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


def create_select(table_name, columns_new, columns_old):
    data_types = []
    n = range(len(columns_new))
    for a in n:
        data_types.append(""+columns_old[a]+" AS "+columns_new[a]+"")
    return """
    SELECT id, 
           %s
     FROM public.%s
    ORDER BY id;
        """ % (', '.join(data_types), table_name)


def create_insert(table_name, columns, data_type):
    data_types = []
    n = range(len(columns))
    for a in n:
        if data_type[a]:
            data_types.append("'#(%s)s'" % columns[a])
        else:
            data_types.append("#(%s)s" % columns[a])
    INSERT = """
        INSERT INTO public.%s (id, 
               %s, 
               created_at, created_by_id) 
        VALUES ( #(id)s, 
               %s, 
               NOW(), NULL);
    """ % (table_name, ', '.join(columns), ', '.join(data_types))
    return INSERT.replace('#', '%')


def create_update(table_name, columns, data_type):
    data_types = []
    n = range(len(columns))
    for a in n:
        if data_type[a]:
            data_types.append(""+columns[a]+"='%("+columns[a]+")s'")
        else:
            data_types.append(""+columns[a]+"=%("+columns[a]+")s")
    UPDATE = """
        UPDATE public.%s 
           SET %s
         WHERE id=#(id)s;
    """ % (table_name, ', '.join(data_types))
    return UPDATE.replace('#', '%')
