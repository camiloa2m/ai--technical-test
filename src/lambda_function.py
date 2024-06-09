import hashlib
import urllib.parse
from collections import OrderedDict
from datetime import datetime, UTC

import boto3

# Run pytest as follows:
# pytest -p no:cacheprovider --cov-report html --cov=. test/
# open htmlcov/index.html to check coverage

def lambda_handler(event, context):
    # Obtener el nombre del bucket y la clave del objeto
    bucket = event["bucket"]
    key = urllib.parse.unquote_plus(
        event["key"], encoding="utf-8"
    )

    try:
        # Leer el archivo de S3
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        print(e)
        print(
            f"Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function."
        )
        raise e

    # Procesar el contenido del archivo
    file_content = response["Body"].read().decode("utf-8")
    
    data = OrderedDict()
    for line in file_content.split():
        k, v = line.split("=")
        data[k] = v

    # Verificar integridad del archivo
    hash_ok = check_file_integrity(data)

    if hash_ok:
        data.pop("hash")
        # Guardar informacion del archivo en DynamoDB
        dynamodb = boto3.resource("dynamodb")
        
        # Generate the current timestamp
        current_timestamp = datetime.now(UTC).isoformat()
        current_timestamp = "2024-06-08T12:00:00Z"

        # Add the timestamp to the item
        data['timestamp'] = current_timestamp

        # Insert the item into the table
        table = dynamodb.Table("my-table")
        response = table.put_item(Item=data)
        
        # Verificar el resultado de la operación
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            print(f"Error inserting element, object {key}. Item: {data}")
        else:
            # Si no hay errores se elimina el archivo del S3
            response = s3.delete_object(Bucket=bucket, Key=key)
    else:
        print(f"Hash does not match. Object {key} from bucket {bucket}")


def check_file_integrity(data: OrderedDict) -> bool:
    values = [data[k] for k in data.keys()]
    concat_str = "~".join(values[:-1])

    true_hash = values[-1]

    hash_concat_str = hashlib.md5(concat_str.encode("utf-8"))
    hash_concat_str = hash_concat_str.hexdigest()

    hash_ok = hash_concat_str == true_hash

    return hash_ok