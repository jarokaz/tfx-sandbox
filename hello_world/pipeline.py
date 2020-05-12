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


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function




from typing import Text


from hello_component import component

from tfx.components import CsvExampleGen
from tfx.components import StatisticsGen
from tfx.orchestration import metadata
from tfx.orchestration import pipeline

from tfx.utils.dsl_utils import external_input




def create_pipeline(pipeline_name: Text, pipeline_root: Text, data_root: Text,
                     beam_pipeline_args: Text) -> pipeline.Pipeline:
  """Custom component demo pipeline."""
  examples = external_input(data_root)

  # Brings data into the pipeline or otherwise joins/converts training data.
  example_gen = CsvExampleGen(input=examples)

  hello = component.HelloComponent(
      input_data=example_gen.outputs['examples'], name=u'HelloWorld')

  # Computes statistics over data for visualization and example validation.
  statistics_gen = StatisticsGen(examples=hello.outputs['output_data'])

  return pipeline.Pipeline(
      pipeline_name=pipeline_name,
      pipeline_root=pipeline_root,
      components=[example_gen, hello, statistics_gen],
      enable_cache=True,
      beam_pipeline_args=beam_pipeline_args
      )


