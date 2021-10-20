import boto3
import csv

def main(): 

 


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

    with open('ADD_A_PATH_TO_EXCEL', 'rb') as csvfile:
        csvf = csv.reader(csvfile, delimeter=',', quotechar='|')
        for item in csvf:
            print(item)
            body = open('LOCATION_OF_OTHER_EXP_FILES'+item[3], 'rb')
            s3.Object('pob6-example-test', item[3]).put(Body=body)

    return

if __name__ == '__main__':
    main()
