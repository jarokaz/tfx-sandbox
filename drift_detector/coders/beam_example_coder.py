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

"""A DoFn converting a raw_data field in AI Platform Prediction
request-response log into tfdv.types.BeamExample.
"""

import json
import apache_beam as beam
import tensorflow_data_validation as tfdv

from typing import List, Optional, Text, Union, Dict, Iterable, Mapping
from tensorflow_data_validation import types
from tensorflow_data_validation import constants


_RAW_DATA_COLUMN = 'raw_data'
_LOGGING_TABLE_SCHEMA = {
    'model': lambda x: type(x) is str,
    'model_version': lambda x: type(x) is str,
    'time': lambda x: lambda x: type(x) is str,
    'raw_data': lambda x: type(x) is str, 
    'raw_prediction': lambda x: type(x) is str,
    'groundtruth': lambda x: type(x) is str
}

LOG_RECORD = Dict

#@beam.typehints.with_input_types(LOG_RECORD)
#@beam.typehints.with_output_types(types.BeamExample)
class InstanceToBeamExample(beam.DoFn):
  """A DoFn which converts a JSON string to types.BeamExample."""

  def __init__(self):
    self._example_size = beam.metrics.Metrics.counter(
        constants.METRICS_NAMESPACE, "example_size")

            

  def process(self, log_record: LOG_RECORD): #-> Iterable[types.BeamExample]:
      #if bq_record:
      #  self._example_size.inc(sum(map(len, batch)))
    
      incorrect_columns = set(log_record.keys()) - set(_LOGGING_TABLE_SCHEMA.keys())
      if bool(incorrect_columns):
          raise TypeError("Received log record with incorrect columns %s" %
                          incorrect_columns)
       
      columns_with_wrong_type = [key for key, value in log_record.items() 
                    if not _LOGGING_TABLE_SCHEMA[key](value)]
      if not bool(columns_with_wrong_type):
          raise TypeError("Received log record with incorrect column types %s" %
                          columns_with_wrong_type)
    
      raw_data = json.loads(log_record[_RAW_DATA_COLUMN])
    
    
      for instance in raw_data['instances']:
          yield instance

    

