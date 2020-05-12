# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START functions_pubsub_setup]
import base64
import json
import os

import googleapiclient.discovery
import pandas as pd


def _sample_instances(data_file, num_rows):
    """Samples instance from a CSV file."""

    df = pd.read_csv(data_file).sample(frac=1).drop('Cover_Type', axis=1)
    
    instances = []
    for row in df.head(num_rows).iterrows():
        feature_dict = {key: [value] for key, value in row[1].to_dict().items()}
        instances.append(feature_dict)

    return instances

def _call_caip_predict(service_name, signature_name, model_output_key, instances):
  
  service = googleapiclient.discovery.build('ml', 'v1')
    
  request_body={
      'signature_name': signature_name,
      'instances': instances}

  response = service.projects().predict(
      name=service_name,
      body=request_body

  ).execute()

  if 'error' in response:
    raise RuntimeError(response['error'])

  return [output[model_output_key] for output in response['predictions']]
    
def run_predictions(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    
    json_str = base64.b64decode(event['data']).decode('utf-8')
    params = json.loads(json_str)
    instances = _sample_instances(
        params['data_file'], 
        params['num_examples'])
     
    predictions = _call_caip_predict(
        params['service_name'], 
        params['signature_name'], 
        params['model_output_key'], 
        instances)
        
    return predictions
    
    