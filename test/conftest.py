import os
from tempfile import NamedTemporaryFile

import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def os_environment_variables():
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "test-credentials"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test-credentials"
    os.environ["AWS_SECURITY_TOKEN"] = "test-credentials"
    os.environ["AWS_SESSION_TOKEN"] = "test-credentials"


@pytest.fixture
def s3(os_environment_variables):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        yield s3


@pytest.fixture
def s3_prep(s3):
    bucket = "my-bucket"
    filename_ok = "archivo_ok.txt"
    filename_fail = "archivo_fail.txt"

    s3.create_bucket(Bucket=bucket)
    with NamedTemporaryFile(delete=False) as file:
        with open(file.name, "w", encoding="UTF-8") as f:
            f.write("totalContactoClientes=250\nmotivoReclamo=25\nmotivoGarantia=10\nmotivoDuda=100\nmotivoCompra=100\nmotivoFelicitaciones=7\nmotivoCambio=8\nhash=2f941516446dce09bc2841da60bf811f")

        s3.upload_file(file.name, bucket, filename_ok)

    with NamedTemporaryFile(delete=False) as file:
        with open(file.name, "w", encoding="UTF-8") as f:
            f.write("totalContactoClientes=251\nmotivoReclamo=25\nmotivoGarantia=10\nmotivoDuda=100\nmotivoCompra=100\nmotivoFelicitaciones=7\nmotivoCambio=8\nhash=2f941516446dce09bc2841da60bf811f")

        s3.upload_file(file.name, bucket, filename_fail)
        
        
@pytest.fixture
def dynamodb(os_environment_variables):
    with mock_aws():
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        yield dynamodb


@pytest.fixture
def dynamodb_prep(dynamodb):
    table_name = "my-table"

    # Create the DynamoDB table
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'timestamp',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'S'  # String
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    yield dynamodb, table_name


