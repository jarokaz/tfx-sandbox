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

"""An example that verifies the counts and includes best practices.
On top of the basic concepts in the wordcount example, this workflow introduces
logging to Cloud Logging, and using assertions in a Dataflow pipeline.
To execute this pipeline locally, specify a local output file or output prefix
on GCS::
  --output [YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]
To execute this pipeline using the Google Cloud Dataflow service, specify
pipeline configuration::
  --project YOUR_PROJECT_ID
  --staging_location gs://YOUR_STAGING_DIRECTORY
  --temp_location gs://YOUR_TEMP_DIRECTORY
  --job_name YOUR_JOB_NAME
  --runner DataflowRunner
and an output prefix on GCS::
  --output gs://YOUR_OUTPUT_PREFIX
"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re

from typing import List, Optional, Text, Union, Dict, Iterable

import apache_beam as beam
import pyarrow as pa
import tensorflow_data_validation as tfdv

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import bigquery
from tensorflow_data_validation import constants
from tensorflow_data_validation import types
from tensorflow_metadata.proto.v0 import statistics_pb2

from jinja2 import Template


# BigQuery to Arrow type mappings
_BQ_TO_ARROW = {
    'STRING': pa.list_(pa.binary()),
    'INTEGER': pa.list_(pa.int64()),
    'FLOAT': pa.list_(pa.float32()),
    'BOOLEAN': pa.list_(pa.int64())
}


class BatchedDictsToTable(beam.DoFn):
    """DoFn to convert a batch of dictionaries to a pyarrow.Table."""

    def process(self, batch: List[Dict], 
                column_specs: Dict) -> Iterable[pa.Table]:
        
        column_names = batch[0].keys()
        # Check that the batch conforms to schema
        if set(column_names) != set(column_specs.keys()):
            raise ValueError("Columns in a batch don't match required column specs")
        
        values_by_column = {column_name: [] for column_name in column_names}
        
        for row in batch:
            for key, value in row.items():
                values_by_column[key].append(value)
        
        arrow_arrays = [
            pa.array([arr], type=_BQ_TO_ARROW[column_specs[column_name]]) 
            for column_name, arr in values_by_column.items()
            ]  
        
        yield pa.Table.from_arrays(arrow_arrays, list(column_names))
  
class DecodeBigQuery(beam.PTransform):
    """Decodes BigQuery records into Arrow RecordBatches."""
    def __init__(
        self,
        column_specs: Dict):
    
        self._column_specs = column_specs
    
    def expand(self, pcoll):
        record_batches = (
            pcoll
            | beam.BatchElements()
            | beam.ParDo(
                BatchedDictsToTable(), self._column_specs)
            )

        return record_batches


def _get_column_specs(query) -> Dict:
    """Gets column specs for data returned by a BQ query."""
    
    client = bigquery.Client()
    
    query_job = client.query('SELECT * FROM ({}) LIMIT 0'.format(query))
    results = query_job.result()
    column_specs = {field.name: field.field_type for field in results.schema}
    
    return column_specs


query = """
       SELECT *
       FROM 
           `mlops-dev-env.covertype_dataset.covertype` 
       """

def run(argv=None, save_main_session=True):
    """Runs the TFDV pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        help='Output file to write results to.',
        default = '/home/jupyter/artifact_store/stats')

    known_args, pipeline_args = parser.parse_known_args(argv)

    #source_table_name = 'mlops-dev-env.covertype_dataset.covertype'
    #num_lots = 100
    #lots = [1]
    #query = _generate_sampling_query(source_table_name, num_lots, lots)
    
    column_specs = _get_column_specs(query)
    
    if not set(column_specs.values()).issubset(_BQ_TO_ARROW.keys()):
        raise ValueError("Unsupported BigQuery data types.")
  
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=pipeline_options) as p:
        pyarrow_records = ( p
            | 'GetData' >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=True))
            | 'DecodeData' >>  DecodeBigQuery(column_specs)
            | 'GenerateStatistics' >> tfdv.GenerateStatistics()
            | 'WriteStatsOutput' >> beam.io.WriteToTFRecord(
                file_path_prefix = known_args.output,
                shard_name_template='',
                coder=beam.coders.ProtoCoder(
                statistics_pb2.DatasetFeatureStatisticsList)))



if __name__ == '__main__':
  # Cloud Logging would contain only logging.INFO and higher level logs logged
  # by the root logger. All log statements emitted by the root logger will be
  # visible in the Cloud Logging UI. Learn more at
  # https://cloud.google.com/logging about the Cloud Logging UI.
  #
  # You can set the default logging level to a different level when running
  # locally.
    logging.getLogger().setLevel(logging.INFO)
    run()