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

    # try:
    #     s3.create_bucket(Bucket='pob6-bucket-test', CreateBucketConfiguration={
    #         'LocationConstraint':'us-west-2'
    #     })
    # except Exception as e:
    #     print(e)

    bucket = s3.Bucket("pob6-bucket-test")
    bucket.Acl().put(ACL='public-read')

    body = open('test.txt', 'rb')
    o = s3.Object('pob6-bucket-test', 'test').put(Body=body)
    s3.Object('pob6-bucket-test', 'test').Acl().put(ACL='public-read')

    dyndb = boto3.resource('dynamodb',
        region_name='us_west_name',
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret_key)

    try:
        table = dyndb.create_table(
            TableName='DataTable',
            KeySchema= [
                {
                    'AttributeName': 'PartitionKey',
                    'KeyTypes':'RANGE'
                },
                {
                    'AttributeName': 'RowKey',
                    'KeyType':'RANGE',
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName':'PartitinoKey',
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

    with open('..\spreadsheets\experiments.csv', 'rb') as csvfile:
        csvf = csv.reader(csvfile, delimeter=',', quotechar='|')
        for item in csvf:
            print(item)
            body = open('..\spreadsheets\datafiles\\'+item[3], 'rb')
            s3.Object('pob6-example-test2', item[3]).put(Body=body)

            url = " https://s3-us-west-2.amazonaws.com/pob6-example-test/"+item[3]
            metadata_item = { 'PartitionKey': item[0], 'RowKey': item[1], 
                                'description': item[4], 'date': item[2], 'url':url }
            try:
                table.put_item(Item=metadata_item)
            except:
                    print("item may already be there or another failure")

    ['experiment1', '1', '3/15/2002', 'exp1', 'this is the comment']
    ['experiment1', '2', '3/15/2002', 'exp2', 'this is the comment2']
    ['experiment1', '3', '3/16/2002', 'exp3', 'this is the comment3']
    ['experiment1', '4', '3/16/2002', 'exp4', 'this is the comment4']

    response = table.get_item(
        Key={
            'PartitionKey':'experiment3',
            'RowKey':'4'
        }
    )
    item = response['Item']
    print(item)

    {u'url': u'https://s3-us-west-2.amazonaws.com/pob6-example-test/exp4', 
        u'date':u'3/16/2002', u'PartitionKey':u'experiment3', u'description':u'this is the comment233',
        u'RowkKey': u'4'}

    response

    return

if __name__ == '__main__':
    main()
