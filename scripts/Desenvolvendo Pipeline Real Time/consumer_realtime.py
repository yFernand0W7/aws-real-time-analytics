import json
import base64
import os
import boto3

sns_client = boto3.client('sns')

SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:908027383012:snsalerta'

PRECIPITATION_PROBABILITY_THRESHOLD = int(os.environ.get('PRECIPITATION_PROBABILITY', 10))
WIND_SPEED_THRESHOLD = int(os.environ.get('WIND_SPEED', 10))
WIND_GUST_THRESHOLD = int(os.environ.get('WIND_GUST', 10))
RAIN_INTENSITY_THRESHOLD = int(os.environ.get('RAIN_INTENSITY', 10))

def lambda_handler(event, context):
    if 'Records' not in event:
        print("No records found in the event.")
        return {
            'statusCode': 400,
            'body': json.dumps('No records found in the event')
        }

    for record in event['Records']:
        # O dado do Kinesis está codificado em base64, então precisa ser decodificado
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        data = json.loads(payload)

        # Extrair os valores relevantes
        precipitation_probability = data['data']['values'].get('precipitationProbability', 0)
        wind_speed = data['data']['values'].get('windSpeed', 0)
        wind_gust = data['data']['values'].get('windGust', 0)
        rain_intensity = data['data']['values'].get('rainIntensity', 0)

        # Verificar se algum dos valores excede os limites configurados
        if (precipitation_probability >= PRECIPITATION_PROBABILITY_THRESHOLD or
                wind_speed >= WIND_SPEED_THRESHOLD or
                wind_gust >= WIND_GUST_THRESHOLD or
                rain_intensity >= RAIN_INTENSITY_THRESHOLD):
            
            message = (
                f"Probabilidade de Chuva: {precipitation_probability}%\n"
                f"Velocidade do Vento: {wind_speed} m/s\n"
                f"Rajada de Vento: {wind_gust} m/s\n"
                f"Intensidade da Chuva: {rain_intensity} mm/h\n"
            )
            
            response = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message,
                Subject='Alerta Meteorológico'
            )
            
            print(f"SNS response: {response}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processed Kinesis records and sent to SNS if thresholds exceeded')
    }
