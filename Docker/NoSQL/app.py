import boto3
import csv

def main(): 

    with open('keys.txt') as f:
        lines = f.readlines()
    print(lines[0])
    print(lines[1])
    aws_key = str.strip(lines[0])
    aws_secret_key = str.strip(lines[1])


    s3 = boto3.resource('s3',
    aws_access_key_id = aws_key,
    aws_secret_access_key=aws_secret_key)

    try:
        s3.create_bucket(Bucket='pob6-bucket-test', CreateBucketConfiguration={
            'LocationConstraint':'us-west-2'
        })
    except Exception as e:
        print(e)

    bucket = s3.Bucket("pob6-bucket-test")
    bucket.Acl().put(ACL='public-read')

    body = open('test.txt', 'rb')
    o = s3.Object('pob6-bucket-test', 'test').put(Body=body)
    s3.Object('pob6-bucket-test', 'test').Acl().put(ACL='public-read')

    dyndb = boto3.resource('dynamodb',
        region_name='us-west-2',
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret_key)

    try:
        table = dyndb.create_table(
            TableName='DataTable',
            KeySchema= [
                {
                    'AttributeName': 'PartitionKey',
                    'KeyType':'HASH'
                },
                {
                    'AttributeName': 'RowKey',
                    'KeyType':'RANGE',
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName':'PartitionKey',
                    'AttributeType':'S'
                },
                {
                    'AttributeName':'RowKey',
                    'AttributeType':'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    except Exception as e:
        print(e)

        table = dyndb.Table("DataTable")

    table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

    print(table.item_count)

    with open('.\spreadsheets\experiments.csv', 'r', encoding='utf-8') as csvfile:
        csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
        next(csvf)
        for item in csvf:
            print(item)
            body = open('.\spreadsheets\datafiles\\'+item[4], 'rb')
            s3.Object("pob6-bucket-test", item[4]).put(Body=body)
            md = s3.Object("pob6-bucket-test", item[4]).Acl().put(ACL='public-read')

            url = " https://s3-us-west-2.amazonaws.com/pob6-example-test/"+item[4]
            metadata_item = { 'PartitionKey': item[0], 'RowKey': item[1], 
                                'temp': item[1], 'conductivity': item[2],
                                'concentration':item[3], 'url':url }
            try:
                table.put_item(Item=metadata_item)
            except:
                    print("item may already be there or another failure")

    query={
        'PartitionKey':'2',
        'RowKey':'-2'
    }

    response = table.get_item(
        Key=query
    )
    print('Query')
    print(str(query))
    item = response['Item']
    print(item)

    response

    return

if __name__ == '__main__':
    main()
