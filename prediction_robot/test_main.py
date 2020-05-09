import json
import base64
import mock

import main


mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'


def test_print_table(capsys):
    params = {
        'table': 'covertype_dataset.covertype',
        'num_rows': 10,
        'service_name': 'projects/mlops-dev-env/models/covertype_classifier_tf/versions/v2',
        'signature_name': 'serving_default',
        'model_output_key': 'probabilities'
    }

    message = json.dumps(params)
    data = {'data': base64.b64encode(message.encode())}

    # Call tested function
    main.hello_pubsub(data, mock_context)
    #out, err = capsys.readouterr()
    #assert 'covertype_dataset.covertype\n' in out
    
