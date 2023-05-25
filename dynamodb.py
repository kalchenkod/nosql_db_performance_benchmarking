import boto3


class DynamoDB:
    def __init__(self):
        self.dynamodb_client = boto3.resource('dynamodb',
                                              aws_access_key_id='#####',
                                              aws_secret_access_key='##########',
                                              region_name='eu-west-1'
                                              )
        self.name = 'dynamodb'

    def create(self, items):
        table_name = 'reviews_table'
        table = self.dynamodb_client.Table(table_name)
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

    def read(self):
        table_name = 'reviews_table'
        table = self.dynamodb_client.Table(table_name)
        response = table.scan()
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
        return response

    def update(self, items=None):
        table_name = 'reviews_table'
        table = self.dynamodb_client.Table(table_name)
        for item in items:
            table.update_item(
                Key={'id': item['id']},
                UpdateExpression="set user_id=:i",
                ExpressionAttributeValues={':i': item['user_id'] + '!'},
                ReturnValues="UPDATED_NEW"
            )

    def delete(self):
        table_name = 'reviews_table'
        table = self.dynamodb_client.Table(table_name)

        flag = False
        scan = table.scan()
        while not flag:
            with table.batch_writer() as batch:
                for item in scan['Items']:
                    batch.delete_item(Key={'id': item['id']})
                flag = True
