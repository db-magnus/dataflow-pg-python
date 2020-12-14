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

Example based on https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/bigquery_tornadoes.py

python bq-postgres-dataflow.py --temp_location gs://magnusfagertun-test --project magnusfagertun --region europe-west1 --runner DataflowRunner --setup_file beam-nuggets/dist/beam-nuggets-0.16.0.tar.gz
"""

from __future__ import absolute_import
from beam_nuggets.io import relational_db
from sqlalchemy import Table, Integer, String, Column

import datetime;

import argparse
import logging

import apache_beam as beam


def count_categories(input_data):
  """
Counting how many rows there are per category.

  """
  ts =  datetime.datetime.now().strftime("%Y%m%d%-%H%M%S")

  return (
    input_data
      | 'count ' >> beam.FlatMap(
          lambda row: [(int(row['category']),1)])
      | 'count inputs' >> beam.CombinePerKey(sum)
      | 'formatoutput' >>
      beam.Map(lambda k_v: {
          'category_ts': str(k_v[0])+ts, 'count': k_v[1]
      }))


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
     host='10.42.64.3',
     port=5432,
     username='postgres',
     password='rsJECqf3M5v9BzpD',
     database='postgres'
  )


  table_config_teams = relational_db.TableConfiguration(
    name='teams',
    create_if_missing=True,  # automatically create the table if not there
    primary_key_columns=['id']  # and use 'num' column as primary key
  )

  table_config_category = relational_db.TableConfiguration(
    name='category',
    create_if_missing=True,  # automatically create the table if not there
    primary_key_columns=['category_ts']  # and use 'num' column as primary key
  )

  with beam.Pipeline(argv=pipeline_args) as p:
    # Read the table rows into a PCollection.
    rows = p | 'read' >> beam.io.ReadFromBigQuery(
            query="""
            SELECT id, category FROM `magnusfagertun.demos.teams_full` """,
            use_standard_sql=True)
    counted= count_categories(rows)


    # Write the output using a "Write" transform that has side effects.

    rows | 'Write Teams' >> relational_db.Write(
        source_config=source_config,
        table_config=table_config_teams    )
    counted | 'Write Counts' >> relational_db.Write(
            source_config=source_config,
            table_config=table_config_category   )

    # Run the pipeline (all operations are deferred until run() is called).


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
