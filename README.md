## Reading from BQ, write to PostgreSQL

Quick and dirty Beam python code to read from BQ and write output to PostgreSQL.

Based on [this](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/bigquery_tornadoes.py) code example from the Beam documentation.

Setup python3 environment using virtualenv, and make sure gcloud is authenticated.

###Dependencies
I got some error messages from sql alchemy:
```
TypeError: expected bytes, str found [while running 'Writing to DB/ParDo(_WriteToRelationalDBFn)']
```

Fixed by installing beam-nuggets from source, changing pg8000 driver away from the newest version. Also changing SQLAlchemy to get PG13 support.

```
git clone git@github.com:mohaseeb/beam-nuggets.git
cd beam-nuggets
```

change versions in setup.py:
```
'SQLAlchemy>=1.3.0,<2.0.0',
'pg8000==1.16.5',
```

`pip install .`



### Setup tarball of beam-nuggets

The command below should create the tarball under the dist directory
```
cd beam-nuggets
python setup.py sdist
```


### Script versions
**local_postgresql.py** is used to insert static data into local database. Using localrunner
```
python local_postgresql.py
```
**bq-postgres.py** will extract data from BigQuery, load into local postgresql database. Using localrunner
```
python bq-postgres.py --temp_location gs://magnusfagertun-test --project magnusfagertun
```

**bq-postgres-dataflow.py** will run on dataflow, using private ip connection to Cloud SQL. Include full path to the setup.py file, this will upload the tarball of our modified beam-nuggets.
```
python bq-postgres-dataflow.py --temp_location gs://magnusfagertun-test --project magnusfagertun --region europe-west1 --runner DataflowRunner --setup_file /Users/magnusfagertun/src/dataflow-python/beam-nuggets/setup.py --max_num_workers 20

```
