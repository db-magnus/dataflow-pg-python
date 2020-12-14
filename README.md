notes:

error message from sql alchemy:
TypeError: expected bytes, str found [while running 'Writing to DB/ParDo(_WriteToRelationalDBFn)']

Fix by installing from source, changing pg8000 driver. Also changing SQLAlchemy to get PG13 support.


git clone git@github.com:mohaseeb/beam-nuggets.git
cd beam-nuggets

change versions in setup.py:
'SQLAlchemy>=1.3.0,<2.0.0',
'pg8000==1.16.5',


pip install .
