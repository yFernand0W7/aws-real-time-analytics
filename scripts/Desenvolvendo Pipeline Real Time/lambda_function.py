import json
import requests
import boto3
import os

# Configurações da API
latitude = -29.6846 
longitude = -51.1419 
TOMORROW_API_KEY = os.getenv('TOMORROW_API_KEY')
url = f"https://api.tomorrow.io/v4/weather/realtime?location={latitude},{longitude}&apikey={TOMORROW_API_KEY}"
headers = {"accept": "application/json"}

# Configurações do Kinesis
STREAM_NAME = "broker"
REGION = "us-east-1"

# Cliente do Kinesis
kinesis_client = boto3.client('kinesis', region_name=REGION)

def lambda_handler(event, context):
    # Faz a requisição à API
    response = requests.get(url, headers=headers)
    weather_data = response.json()

    # Envia os dados para o Kinesis
    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(weather_data),
        PartitionKey="partition_key"
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Dados enviados ao Kinesis com sucesso')
    }
