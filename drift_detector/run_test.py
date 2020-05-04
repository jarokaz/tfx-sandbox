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



# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import os

from typing import List, Optional, Text, Union, Dict, Iterable

import apache_beam as beam
import pyarrow as pa
import tensorflow_data_validation as tfdv

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner
from apache_beam.runners import DirectRunner

from google.protobuf import text_format
from tensorflow_data_validation.statistics import stats_options
from tensorflow_data_validation.utils import io_util
from tensorflow_data_validation.utils import batch_util
from tensorflow_metadata.proto.v0 import statistics_pb2

#from utils.gen_stats import generate_statistics_from_bq



from coders.beam_example_coder import InstanceToBeamExample
        
        
if __name__ == '__main__':
    # Cloud Logging would contain only logging.INFO and higher level logs logged
    # by the root logger. All log statements emitted by the root logger will be
    # visible in the Cloud Logging UI. Learn more at
    # https://cloud.google.com/logging about the Cloud Logging UI.
    #
    # You can set the default logging level to a different level when running
    # locally.
    logging.getLogger().setLevel(logging.INFO)
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_path',
        dest='output_path',
        help='Output path',
        #default = 'gs://hostedkfp-default-36un4wco1q/tfdv')
        default = '/home/jupyter/output')
    parser.add_argument(
        '--dataflow_gcs_location',
        dest='dataflow_gcs_location',
        help='A GCS root for Dataflow staging and temp locations.',
        default = 'gs://hostedkfp-default-36un4wco1q/dataflow')
    parser.add_argument(
        '--schema_file',
        dest='schema_file',
        help='A path to a schema file',
        default = 'schema.pbtxt')

    known_args, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(GoogleCloudOptions).staging_location = '%s/staging' % known_args.dataflow_gcs_location
    pipeline_options.view_as(GoogleCloudOptions).temp_location = '%s/temp' % known_args.dataflow_gcs_location
    
    stats_options = stats_options.StatsOptions()
    schema = tfdv.load_schema_text(known_args.schema_file)
    
    anomalies_output_path = os.path.join(
        known_args.output_path, 'test.txt'
    )
    stats_output_path = os.path.join(
        known_args.output_path, 'stats.pb'
    )
    
    query = """
       SELECT *
       FROM 
           `mlops-dev-env.data_validation.sklearn_covertype_classifier_logs` 
       """
    
    query1 = """
       SELECT *
       FROM 
           `mlops-dev-env.covertype_dataset.covertype` 
       LIMIT 2
       """

    feature_names = [
        'Elevation', 
        'Aspect',
        'Slope',
        'Horizontal_Distance_To_Hydrology',
        'Vertical_Distance_To_Hydrology',
        'Horizontal_Distance_To_Roadways',
        'Hillshade_9am',
        'Hillshade_Noon',
        'Hillshade_3pm',
        'Horizontal_Distance_To_Fire_Points',
        'Wilderness_Area',
        'Soil_Type'
    ]
    with beam.Pipeline(options=pipeline_options) as p:
        examples = ( p
            | 'GetData' >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=True))
            | 'InstancesToBeamExamples' >> beam.ParDo(InstanceToBeamExample(feature_names))
            | 'BeamExamplesToArrow' >> batch_util.BatchExamplesToArrowTables())
 #       
        stats = (examples
            | 'GenerateStatistics' >> tfdv.GenerateStatistics())
                    
        _ = (examples
            | 'WriteOutputTest' >> beam.io.textio.WriteToText(
                                           file_path_prefix=anomalies_output_path,
                                           shard_name_template='',
                                           append_trailing_newlines=True))
        
        
        _ = (stats       
            | 'WriteStatsOutput' >> beam.io.WriteToTFRecord(
                  file_path_prefix=stats_output_path,
                  shard_name_template='',
                  coder=beam.coders.ProtoCoder(
                      statistics_pb2.DatasetFeatureStatisticsList)))
   
       # _ = (stats
            #| 'ValidateStatistics' >> beam.Map(tfdv.validate_statistics, schema=schema)
        #    | 'WriteAnomaliesOutput' >> beam.io.textio.WriteToText(
          #                                  file_path_prefix=anomalies_output_path,
         #                                   shard_name_template='',
           #                                 append_trailing_newlines=True))
        