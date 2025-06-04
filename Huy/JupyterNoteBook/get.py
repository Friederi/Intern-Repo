import pymysql
import json

endpoint = 'huy.cfsyaaw461l4.ap-southeast-2.rds.amazonaws.com'
username = 'admin'
password = 'Thtruemilk123'
database_name = 'customer'

connection = pymysql.connect(host = endpoint, user = username, password = password, database = database_name) 

def lambda_handler(event, context):

    try:
        cursor = connection.cursor()
        cursor.execute("Select * FROM customer_info")

        rows = cursor.fetchall()
        for row in rows:
            print(row)

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in rows]

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(results)
        }

    except Exception as e:
        # Log the full error
        print(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
            
		