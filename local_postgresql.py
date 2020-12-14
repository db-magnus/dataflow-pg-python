#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""

Writing table to local postgresql

"""

from __future__ import absolute_import
from beam_nuggets.io import relational_db
from sqlalchemy import Table, Integer, String, Column


import argparse
import logging

import apache_beam as beam


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      default='magnusfagertun:demos.small_teams',
      help=(
          'Input BigQuery table to process specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
  parser.add_argument(
      '--output',
#      required=True,
      required=False,
      help=(
          'Output BigQuery table for results specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))

  parser.add_argument(
      '--gcs_location',
      required=False,
      help=('GCS Location to store files to load '
            'data into Bigquery'))

  known_args, pipeline_args = parser.parse_known_args(argv)

  source_config = relational_db.SourceConfiguration(
     drivername='postgresql+pg8000',
     host='localhost',
     port=5432,
     username='postgres',
     password='somePassword',
     database='postgres'

  )

  schema='id:INT64, score:INT64',

  records = [
        {'id': '1', 'num': 1},
        {'id': '2', 'num': 2},
        {'id': '3', 'num': 3}
    ]


  table_config = relational_db.TableConfiguration(
    name='teams',
    create_if_missing=True,  # automatically create the table if not there
    primary_key_columns=['num']  # and use 'num' column as primary key
)

  with beam.Pipeline(argv=pipeline_args) as p:


    # Write the output using a "Write" transform that has side effects.

    rows = p | "Reading records" >> beam.Create(records, reshuffle=False)

    rows | 'Write' >> relational_db.Write(
        source_config=source_config,
        table_config=table_config    )

    # Run the pipeline (all operations are deferred until run() is called).


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
