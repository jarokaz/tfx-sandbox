# Lint as: python2, python3
# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import absl
import os

import kfp

from tfx.orchestration import metadata
from tfx.orchestration import pipeline
from tfx.orchestration.beam.beam_dag_runner import BeamDagRunner
from tfx.orchestration.kubeflow import kubeflow_dag_runner

from pipeline import create_pipeline


_project_id = os.getenv("PROJECT_ID")
_region = os.getenv("GCP_REGION", "us-central1")
_artifact_store_uri = os.getenv("ARTIFACT_STORE_URI")
_tfx_image = os.getenv("KUBEFLOW_TFX_IMAGE")
_pipeline_name = os.getenv("PIPELINE_NAME")
_taxi_root = 'gs://mlops-dev-workspace/pipelines'
_data_root = os.getenv("DATA_ROOT_URI")


_pipeline_root = '{}/{}/{}'.format(
      _artifact_store_uri, 
      _pipeline_name,
      kfp.dsl.RUN_ID_PLACEHOLDER)

_beam_tmp_folder = '{}/beam/tmp'.format(_artifact_store_uri)
_beam_pipeline_args =  [
      '--runner=DataflowRunner',
      '--experiments=shuffle_mode=auto',
      '--project=' + _project_id,
      '--temp_location=' + _beam_tmp_folder,
      '--region=' + _region,
  ]

# To run this pipeline from the python CLI:
#   $python taxi_pipeline_hello.py
if __name__ == '__main__':
  absl.logging.set_verbosity(absl.logging.INFO)

  metadata_config = kubeflow_dag_runner.get_default_kubeflow_metadata_config()
  pipeline_operator_funcs = kubeflow_dag_runner.get_default_pipeline_operator_funcs()
    
  runner_config = kubeflow_dag_runner.KubeflowDagRunnerConfig(
      kubeflow_metadata_config = metadata_config,
      pipeline_operator_funcs = pipeline_operator_funcs,
      tfx_image=_tfx_image)

  kubeflow_dag_runner.KubeflowDagRunner(config=runner_config).run(
      create_pipeline(
          pipeline_name=_pipeline_name,
          pipeline_root=_pipeline_root,
          data_root=_data_root,
          beam_pipeline_args=_beam_pipeline_args))
