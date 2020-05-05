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

"""Runs a data drift job."""

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

from google.protobuf import text_format
from tensorflow_data_validation.statistics import stats_options
from tensorflow_data_validation.utils import io_util
from tensorflow_metadata.proto.v0 import statistics_pb2

from utils.drift_reports import generate_drift_reports


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
        '--request_response_log_table',
        dest='request_response_log_table',
        required=True,
        help='Full name of AI Platform Prediction request-response log table')
    parser.add_argument(
        '--feature_names',
        dest='feature_names',
        required=False,
        help='A list of feature names for instances in the log')
    parser.add_argument(
        '--start_time',
        dest='start_time',
        required=True,
        help='The beginning of a time window')
    parser.add_argument(
        '--end_time',
        dest='end_time',
        required=True,
        help='The end of a time window')
    parser.add_argument(
        '--output_path',
        dest='output_path',
        required=True,
        help='Output path',
        default = 'gs://hostedkfp-default-36un4wco1q/tfdv')
    parser.add_argument(
        '--dataflow_gcs_location',
        dest='dataflow_gcs_location',
        required=True,
        help='A GCS root for Dataflow staging and temp locations')
    parser.add_argument(
        '--schema_file',
        dest='schema_file',
        help='A path to a schema file',
        required=True)
    parser.add_argument(
        '--baseline_stats_file',
        dest='baseline_stats_file',
        help='A path to a baseline statistics file',
        required=False)

    known_args, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(GoogleCloudOptions).staging_location = '%s/staging' % known_args.dataflow_gcs_location
    pipeline_options.view_as(GoogleCloudOptions).temp_location = '%s/temp' % known_args.dataflow_gcs_location
    
    stats_options = stats_options.StatsOptions()
    schema = tfdv.load_schema_text(known_args.schema_file)
    baseline_stats = None
    
    start_time = known_args.start_time
    end_time = known_args.end_time
    feature_names = known_args.feature_names
    if feature_names:
        feature_names = known_args.feature_names.split(',')
        
    _ = generate_drift_reports(
            request_response_log_table=known_args.request_response_log_table,
            feature_names=feature_names,
            start_time=start_time,
            end_time=end_time,
            output_path=known_args.output_path,
            schema=schema, 
            baseline_stats=baseline_stats,
            stats_options=stats_options,
            pipeline_options=pipeline_options)