from boto3 import resource
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime

demo_table = resource('dynamodb').Table('demo-dynamo-python')

def insert():
    print(f'demo_insert')
    response = demo_table.put_item(
        Item = {
            'customer_id': 'cus-01',
            'order_id': 'ord-1',
            'status': 'pending',
            'created_date': datetime.now().isoformat()

        }
    )
    print(f'Insert response: {response}')

def select_scan():
    print(f'demo_select_scan')
    filter_expression = Attr('status').eq('pending')

    item_list = []
    dynamo_response = {'LastEvaluatedKey': False}
    while 'LastEvaluatedKey' in dynamo_response:
        if dynamo_response['LastEvaluatedKey']:
            dynamo_response = demo_table.scan(
                FilterExpression=filter_expression,
                ExclusiveStartKey=dynamo_response['LastEvaluatedKey']

            )
            print(f'response-if: {dynamo_response}')
        else:
            dynamo_response = demo_table.scan(
                FilterExpression = filter_expression
            )
            print(f'response-else: {dynamo_response}')

        for i in dynamo_response['Items']:
            item_list.append(i)
    print(f'Number of input tasks to process: {len(item_list)}')
    for item in item_list:
        print(f'Item: {item}')


def query_by_partition_key(customer_value):
    print(f'demo_select_query')
    response = {}
    filtering_exp = Key('customer_id').eq(customer_value)
    response = demo_table.query(
        KeyConditionExpression=filtering_exp)
    print(f'Query response: {response}')
    print(f'Query response: {response["Items"]}')

query_by_partition_key('cus-01')

#insert()
#select_scan()

def query_by_partiton_key_order(customer_value):
    print(f'\n\t\t\t>>>>>>>>>>>>>> demo_query_by_partition_key_order <<<<<<<<<<<<<<<<<')
    response = {}
    filtering_exp = Key('customer_id').eq(customer_value)
    response = demo_table.query(
        KeyConditionExpression=filtering_exp,ScanIndexForward=True
    )

    item_list = response["Items"]
    for item in item_list:
        print(f'Item:{item}')




query_by_partition_key('cus-01')

#insert()
#select_scan()