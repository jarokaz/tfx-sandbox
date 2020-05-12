FROM tensorflow/tfx:0.21.4
WORKDIR /pipeline
COPY ./dataflow_runner.py ./
COPY ./pipeline.py ./
COPY ./hello_component /tfx-src/hello_component
ENV PYTHONPATH="/pipeline:${PYTHONPATH}"