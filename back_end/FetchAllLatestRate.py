import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timezone, timedelta


def get_today_rates():
    dynamodb = boto3.resource('dynamodb')
    rate_table = dynamodb.Table('CurrencyRate')
    # currentDateAndTime = datetime.now() - timedelta(minutes=2)
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%Y-%m-%d")
    print(currentTime)
    response = rate_table.scan(
        FilterExpression='begins_with(update_time, :date)',
        ExpressionAttributeValues={
            ':date': currentTime}
    )
    print(response)
    return response["Items"]

def get_latest_rates():
    dynamodb = boto3.resource('dynamodb')
    rate_table = dynamodb.Table('CurrencyRate')
    currentDateAndTime = datetime.now() - timedelta(minutes=1)
    currentTime = currentDateAndTime.strftime("%Y-%m-%d %H:%M")
    print(currentTime)
    response = rate_table.scan(
        FilterExpression='begins_with(update_time, :date)',
        ExpressionAttributeValues={
            ':date': currentTime}
    )
    print(response)
    if len(response["Items"]) == 0:
        currentDateAndTime = datetime.now() - timedelta(minutes=2)
        currentTime = currentDateAndTime.strftime("%Y-%m-%d %H:%M")
        print(currentTime)
        response = rate_table.scan(
            FilterExpression='begins_with(update_time, :date)',
            ExpressionAttributeValues={
                ':date': currentTime}
            )
    return response["Items"][0]


def find_min_max(rates):
    min_val = {}
    max_val = {}
    for currency in list(rates[0]['results'].keys()):
        min_val[currency] = 1e9
        max_val[currency] = 0
    for rate in rates:
        for k, v in rate['results'].items():
            min_val[k] = min(min_val[k], float(v))
            max_val[k] = max(max_val[k], float(v))

    return min_val, max_val


def generate_response(latest_rates, max_val, min_val):
    response = {}
    for currency, rate in latest_rates['results'].items():
        if 'rates' not in response:
            response['rates'] = {}
        if currency not in response['rates']:
            response['rates'][currency] = {}
        response['rates'][currency]['rate'] = rate
        response['rates'][currency]['highest'] = str(max_val[currency])
        response['rates'][currency]['lowest'] = str(min_val[currency])
    response['update_time'] = latest_rates['update_time']
    response['base'] = latest_rates['base']
    return response


def lambda_handler(event, context):
    # TODO implement
    rates = get_today_rates()
    min_val, max_val = find_min_max(rates)
    latest_rates = get_latest_rates()
    response = generate_response(latest_rates, max_val, min_val)
    print(response)

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*'
        },
        'body': json.dumps(response)
    }
