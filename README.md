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

3) Edit `table_list.csv` with tables_old and tables_new list


4) Execute `create_etl.py` for read `table_list.csv` and create all commands: 

```
python create_etl.py
```

5) Execute `etl.py` for transfer data between two database: 

```
python etl.py
```

## Table list example:

```
source_tables,target_tables
xxxx.vinculo_tipos,xxxx_vinculotipos
xxxx.processo_tipos,xxxx_processotipos
xxxx.desligamento_motivos,xxxx_desligamentomotivos
xxxx.escopos,xxxx_escopos
xxxx.instituicao_tipos,xxxx_instituicaotipos
xxxx.instituicao_definicoes,xxxx_instituicaodefinicoes
xxxx.vinculos,xxxx_vinculos
xxxx.processos,xxxx_processos
``` 

## Get database list

```
SELECT DISTINCT table_name
  FROM information_schema.columns
 WHERE table_schema = 'public'
 ORDER BY table_name;
```

