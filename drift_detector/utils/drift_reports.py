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
"""A utility function to generate data drift reports for a time window
in AI Platform Prediction request-response log.
"""

import datetime
import os
import logging
from typing import List, Optional, Text, Union, Dict, Iterable

import apache_beam as beam
import tensorflow_data_validation as tfdv

from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from jinja2 import Template
from tensorflow_data_validation.statistics import stats_options
from tensorflow_data_validation.utils import batch_util

from tensorflow_metadata.proto.v0 import statistics_pb2
from tensorflow_metadata.proto.v0 import schema_pb2

from coders.beam_example_coder import InstanceToBeamExample

_STATS_FILENAME='stats.pb'
_ANOMALIES_FILENAME='anomalies.pbtxt'

GCSPath = Union[bytes, Text]

def _generate_query(table_name, start_time, end_time):
  """Prepares the data sampling query."""

  sampling_query_template = """
       SELECT *
       FROM 
           `{{ source_table }}` AS cover
       WHERE time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
       LIMIT 100
       """
  
  query = Template(sampling_query_template).render(
      source_table=table_name, start_time=start_time, end_time=end_time)

  return query


def generate_drift_reports(
        request_response_log_table: str,
        feature_names: List[str],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        output_path: GCSPath,
        schema: schema_pb2.Schema,
        baseline_stats: statistics_pb2.DatasetFeatureStatisticsList,
        stats_options: stats_options.StatsOptions = stats_options.StatsOptions(),
        pipeline_options: Optional[PipelineOptions] = None,       
):
    """Computes statistics and anomalies for a time window in AI Platform Prediction
    request-response log.
  
    Args:
      request_response_log_table: A full name of a BigQuery table
        with the request_response_log
      feature_names: A list of feature names. It is required when the instances
        in the log are in a simple list format.
      start_time: The beginning of a time window.
      end_time: The end of a time window.
      output_path: The GCS location to output the statistics and anomalies
        proto buffers to. The file names will be `stats.pb` and `anomalies.pbtxt`. 
      schema: A Schema protobuf describing the expected schema.
      stats_options: `tfdv.StatsOptions` for generating data statistics.
      pipeline_options: Optional beam pipeline options. This allows users to
        specify various beam pipeline execution parameters like pipeline runner
        (DirectRunner or DataflowRunner), cloud dataflow service project id, etc.
        See https://cloud.google.com/dataflow/pipelines/specifying-exec-params for
        more details.
    """

    query = _generate_query(request_response_log_table, start_time, end_time)    
    stats_output_path = os.path.join(output_path, _STATS_FILENAME)
    anomalies_output_path = os.path.join(output_path, _ANOMALIES_FILENAME)
    
    with beam.Pipeline(options=pipeline_options) as p:
        stats = ( p
            | 'GetData' >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=True))
            | 'InstancesToBeamExamples' >> beam.ParDo(InstanceToBeamExample(feature_names))
            | 'BeamExamplesToArrow' >> batch_util.BatchExamplesToArrowTables()
            | 'GenerateStatistics' >> tfdv.GenerateStatistics(stats_options))
        
        _ = (stats       
            | 'WriteStatsOutput' >> beam.io.WriteToTFRecord(
                  file_path_prefix=stats_output_path,
                  shard_name_template='',
                  coder=beam.coders.ProtoCoder(
                      statistics_pb2.DatasetFeatureStatisticsList)))
        
        _ = (stats
            | 'ValidateStatistics' >> beam.Map(tfdv.validate_statistics, schema=schema)
            | 'WriteAnomaliesOutput' >> beam.io.textio.WriteToText(
                                            file_path_prefix=anomalies_output_path,
                                            shard_name_template='',
                                            append_trailing_newlines=False))
        

    

