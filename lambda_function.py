import json
import boto3
from datetime import datetime
import urllib.request
from decimal import Decimal, Context
ctx = Context(prec=10)


# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('test_bike_share_data_1')  # Replace with your actual table name


def make_request_urllib(url):
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read()
            return json.loads(data.decode('utf-8'))
    except urllib.error.URLError as e:
        print(f"An error occurred: {e}")
        return None

def lambda_handler(event, context):
    # Example data (could also be from an API call)
    #free bike status
    free_bike_status = make_request_urllib("https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/free_bike_status.json")
    #data = {'api_name':'free_bikes',
    #        'timestamp': free_bike_status['last_updated'],
    #        'data':free_bike_status['data']}
    data = {
    'api_name': 'free_bikes',
    'timestamp': str(datetime.utcnow()),  # Current timestamp
    'last_updated': free_bike_status['last_updated'],
    #'ttl': free_bike_status['ttl'],
    #'version': free_bike_status['version'],
    'data': free_bike_status['data'],
    }
    #ensure all data that is in float type is decimal
    for bike in data['data']['bikes']:
        bike['lat'] = ctx.create_decimal_from_float(bike['lat'])#Decimal(str(bike['lat']))
        bike['lon'] = ctx.create_decimal_from_float(bike['lon'])
        bike['is_reserved'] = int(bike['is_reserved'])
        bike['is_disabled'] = int(bike['is_disabled'])
        #bike['vehicle_type_id'] = ctx.create_decimal_from_float(bike['vehicle_type_id'])#str(bike['vehicle_type_id'])
        bike['current_range_meters'] = ctx.create_decimal_from_float(bike['current_range_meters'])
    # Write item to DynamoDB
    try:
        table.put_item(Item=data)
        return {
            'statusCode': 200,
            'body': json.dumps('Item written to DynamoDB successfully')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error writing to DynamoDB: {str(e)}')
        }
