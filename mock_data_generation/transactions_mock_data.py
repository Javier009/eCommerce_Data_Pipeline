#s3://your-bucket/transactions/year=2023/month=03/day=15/hour=05/min=04transactions_2023-03-15_06_04_16.csv
import boto3
import time
import random
import pandas as pd
from datetime import datetime
import csv
from io import StringIO

REGION     = "us-east-1"
WORKGROUP  = "ecomm-exercise"
DATABASE   = "dev"
SECRET_NAME = "redshift/dev/admin" 

secrets = boto3.client("secretsmanager", region_name=REGION)
rsd  = boto3.client("redshift-data",   region_name=REGION)

# 1) Resolve Secret ARN (so we don't hardcode it)
SECRET_ARN = secrets.describe_secret(SecretId=SECRET_NAME)["ARN"]

def df_sql(sql:str):

    r = rsd.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        SecretArn=SECRET_ARN,
        Sql=sql
    )
    sid = r["Id"]

    try:
        while True:
            d = rsd.describe_statement(Id=sid)
            s = d["Status"]
            if s in ("FINISHED", "FAILED", "ABORTED"):
                break
            time.sleep(0.2)
        if s != "FINISHED":
            raise RuntimeError(d)
        out  = rsd.get_statement_result(Id=sid)
        cols = [c["name"] for c in out["ColumnMetadata"]]
        rows = [[list(v.values())[0] if v else None for v in rec] for rec in out["Records"]]
        return pd.DataFrame(rows, columns=cols)
       
    except Exception as e:
        print(f"Error executing SQL: {str(e)}")


def fake_transactions_data(number_of_records:int=100):

    data = []

    customer_data = df_sql("SELECT  * FROM public.dim_customers ORDER BY RANDOM() LIMIT 50;")
    product_data  = df_sql("SELECT product_id, price FROM public.dim_products ORDER BY RANDOM() LIMIT 50;")

    for i in range(number_of_records):
        customer = customer_data.sample(n=1).iloc[0]
        product  = product_data.sample(n=1).iloc[0]

        transaction = {
            "transaction_id": f"TXN{datetime.now().strftime('%Y%m%d%H%M%S')}{i:04}",
            "customer_id": customer['customer_id'],
            "product_id": product['product_id'],
            "quantity": random.randint(1, 5),
            "price": product['price'],
            "transaction_date": (datetime.now().strftime("%Y-%m-%d-%H:%M:%S")),
            "payment_type": random.choice(["Credit Card", "Debit Card", "PayPal", "Gift Card"]),
            "status": random.choice(["Completed", "Pending", "Failed", "Refunded"])
        }
    
        data.append(transaction)
    return data

def data_to_s3(bucket_name:str, data:list):

    s3_client = boto3.client('s3')
    object_key = (
    f"transactions/year={datetime.now().year}/"
    f"month={datetime.now().month:02}/"
    f"day={datetime.now().day:02}/"
    f"hour={datetime.now().hour:02}/"
    f"min={datetime.now().minute:02}/"
    f"transactions_{datetime.now().strftime('%Y-%m-%d_%H_%M_%S')}.csv")

    try:
        csv_buffer = StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(data)
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue())
        print(f"Mock transactions data uploaded to s3://{bucket_name}/{object_key} in CSV format.")
    except Exception as e:
        print(f"Error uploading CSV to S3: {str(e)}")


def transactions_data_mock_lambda_handler(event, context):
    try:
        print("Generating mock transactions data...")
        transactions = fake_transactions_data(number_of_records=100)
        print(f"Generated {len(transactions)} mock transactions records.")
        data_to_s3(bucket_name="ecomm-transactions-bucket", data=transactions)
        print("Data upload complete.")
        return {
            'statusCode': 200,
            'body': 'Mock transactions data generated and uploaded to S3 successfully.'
        }
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error generating or uploading mock transactions data: {str(e)}'
        }

# if __name__ == "__main__":
#     print("Generating mock transactions data...")
#     transactions = fake_transactions_data(number_of_records=100)
#     print(f"Generated {len(transactions)} mock transactions records.")
#     data_to_s3(bucket_name="ecomm-transactions-bucket", data=transactions)
#     print("Data upload complete.")
   


   