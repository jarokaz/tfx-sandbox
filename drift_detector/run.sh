REQUEST_RESPONSE_LOG_TABLE=mlops-dev-env.data_validation.sklearn_covertype_classifier_logs
FEATURE_NAMES=\
Elevation,\
Aspect,\
Slope,\
Horizontal_Distance_To_Hydrology,\
Vertical_Distance_To_Hydrology,\
Horizontal_Distance_To_Roadways,\
Hillshade_9am,\
Hillshade_Noon,\
Hillshade_3pm,\
Horizontal_Distance_To_Fire_Points,\
Wilderness_Area,\
Soil_Type
START_TIME=2020-05-01T20:00:27
END_TIME=2020-05-01T20:01:39
OUTPUT_PATH='gs://hostedkfp-default-36un4wco1q/tfdv/output'
DATAFLOW_GCS_LOCATION='gs://hostedkfp-default-36un4wco1q/tfdv/dataflow'
SCHEMA_FILE='gs://hostedkfp-default-36un4wco1q/tfdv/schema/schema.pbtxt'
BASELINE_STATS_FILE='gs://hostedkfp-default-36un4wco1q/tfdv/baseline_stats/stats.pb'

PROJECT_ID=mlops-dev-env
RUNNER=DataflowRunner
SETUP_FILE=./setup.py

python run.py --project $PROJECT_ID \
--runner $RUNNER \
--setup_file $SETUP_FILE \
--temp_location $DATAFLOW_GCS_LOCATION/temp \
--staging_location $DATAFLOW_GCS_LOCATION/staging \
--request_response_log_table $REQUEST_RESPONSE_LOG_TABLE \
--feature_names $FEATURE_NAMES \
--start_time $START_TIME \
--end_time $END_TIME \
--output_path $OUTPUT_PATH \
--schema_file $SCHEMA_FILE
