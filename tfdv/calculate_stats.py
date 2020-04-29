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

"""An example that calculates statistics from BQ query.
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
import os

from typing import List, Optional, Text, Union, Dict, Iterable

import apache_beam as beam
import pyarrow 
import tensorflow_data_validation as tfdv

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner
from apache_beam.runners import DirectRunner

from google.cloud import bigquery
from tensorflow_data_validation.statistics import stats_options
from tensorflow_metadata.proto.v0 import statistics_pb2

from utils.gen_stats import generate_statistics_from_bq


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
        '--stats_file_path',
        dest='stats_file_path',
        help='The path to the Stats file',
        default = 'gs://hostedkfp-default-36un4wco1q/tfdv/stats/stats.pb')
    parser.add_argument(
        '--dataflow_gcs_location',
        dest='dataflow_gcs_location',
        help='A GCS root for Dataflow staging and temp locations.',
        default = 'gs://hostedkfp-default-36un4wco1q/dataflow')

    known_args, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(GoogleCloudOptions).staging_location = '%s/staging' % known_args.dataflow_gcs_location
    pipeline_options.view_as(GoogleCloudOptions).temp_location = '%s/temp' % known_args.dataflow_gcs_location
    
    stats_options = stats_options.StatsOptions()
    
    query = """
       SELECT *
       FROM 
           `mlops-dev-env.covertype_dataset.covertype` 
       """
    
    _ = generate_statistics_from_bq(
            query,
            output_path=known_args.stats_file_path,
            stats_options=stats_options,
            pipeline_options=pipeline_options)