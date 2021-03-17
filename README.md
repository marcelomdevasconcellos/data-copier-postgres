# data-copier-postgres (development)

Insert and update data from postgres tables between different databases on python

1) Create env_example based .env;

```
DATABASE_SOURCE=psql://user:pass@host:port/name
DATABASE=psql://user:pass@host:port/name
```

* Configure source database in DATABASE_SOURCE variable;
* Configure destiny database in DATABASE variable;
* Configure ordened tablenames in TABLES variable;

2) Install requirements.txt: 

```
pip install -r requirements.txt
```

3) Edit `plan.ods` with tables_old and tables_new list


4) Execute `create_commands.py` for read `plan.ods` and create all commands: 

```
python copy.py
```

5) Execute `copy.py` for transfer data between two database: 

```
python copy.py
```

## Data structure example:

```

LIST = [ 
    {
        'origem': {
            'table': nome_tabela, 
            'campos': [lista_campos, ]},
        'destino': {
            'table': nome_tabela, 
            'campos': [lista_campos, ]},
    },
    {
        'origem': {
            'table': nome_tabela, 
            'campos': [lista_campos, ]},
        'destino': {
            'table': nome_tabela, 
            'campos': [lista_campos, ]},
    },
    (...)
    {
        'origem': {
            'table': nome_tabela, 
            'campos': [lista_campos, ]},
        'destino': {
            'table': nome_tabela, 
            'campos': [lista_campos, ]},
    },
]

``` 

## Get database list

```

SELECT DISTINCT table_name
  FROM information_schema.columns
 WHERE table_schema = 'public'
 ORDER BY table_name

```

