## Reading from BQ, write to PostgreSQL

Quick and dirty Beam python code to read from BQ and write output to PostgreSQL.

Based on [this](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/bigquery_tornadoes.py) code example from the Beam documentation.

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

pip install .



### Script versions
local_postgresql.py is used to 
