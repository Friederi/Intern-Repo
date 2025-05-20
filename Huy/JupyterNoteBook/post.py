import pymysql
import json


endpoint = 'huy.cfsyaaw461l4.ap-southeast-2.rds.amazonaws.com'
username = 'admin'
password = 'Thtruemilk123'
database_name = 'customer'

connection = pymysql.connect(
            host=endpoint,
            user=username,
            password=password,
            database=database_name
        )

def lambda_handler(event, context):
    connection = None
    try:
        body = json.loads(event['body'])
        id = body.get('id')
        customer_name = body.get('customer_name')
        address = body.get('address')

        if not (id and customer_name and address):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing id, customer_name, or address"})
            }

        cursor = connection.cursor()
        sql = "INSERT INTO customer_info (id, customer_name, address) VALUES (%s, %s, %s)"
        values = (id, customer_name, address)

        cursor.execute(sql, values)
        connection.commit()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Inserted successfully!"})
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    finally:
        if connection:
            connection.close()
