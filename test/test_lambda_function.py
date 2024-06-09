from src.lambda_function import lambda_handler

def test_lambda_handler(s3_prep, dynamodb_prep):
    responses = []
    event = {
        "bucket": "my-bucket",
        "key":"archivo_ok.txt"
    }
    responses.append(lambda_handler(event, None))

    event = {
        "bucket": "my-bucket",
        "key":"archivo_fail.txt"
    }
    responses.append(lambda_handler(event, None))
    
    assert responses

