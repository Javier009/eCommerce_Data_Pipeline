import random, time, boto3, pandas as pd
from faker import Faker

REGION     = "us-east-1"
WORKGROUP  = "ecomm-exercise"
DATABASE   = "dev"
SECRET_NAME = "redshift/dev/admin" 

secrets = boto3.client("secretsmanager", region_name=REGION)
rsd  = boto3.client("redshift-data",   region_name=REGION)

# 1) Resolve Secret ARN (so we don't hardcode it)
SECRET_ARN = secrets.describe_secret(SecretId=SECRET_NAME)["ARN"]

def df_sql(sql: str, is_insert:bool):
    # kick off query
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
        if not is_insert:
            # fetch results -> DataFrame
            out  = rsd.get_statement_result(Id=sid)
            cols = [c["name"] for c in out["ColumnMetadata"]]
            rows = [[list(v.values())[0] if v else None for v in rec] for rec in out["Records"]]
            return pd.DataFrame(rows, columns=cols)
        else:
            print("Insert SQL statement executed successfully.")
    except Exception as e:
        print(f"Error executing SQL: {str(e)}")

def fake_dims_product_table(number_of_potential_newrecords:int=10):

    data = []
    for i in range(number_of_potential_newrecords):
        unique_id_pattern = ''.join([str(random.randint(0,9)) for _ in range(6)])
        record = {
            "product_id": f"prod-{unique_id_pattern}",
            "product_name": f"Product {unique_id_pattern}",
            "category": random.choice(["Electronics", "Clothing", "Home", "Books", "Toys"]),
            "price": round(random.uniform(10.0, 500.0), 2),
            "supplier_id": 'S' + ''.join([str(random.randint(0,9)) for _ in range(4)])
        }
        tuple_record = (record['product_id'], record['product_name'], record['category'], record['price'], record['supplier_id'])

        data.append(tuple_record)

    # Remove product ids that are already in the table so we don't insert duplicates in dimmension table
    existing_products = df_sql(sql="SELECT product_id FROM public.dim_products;", is_insert=False)['product_id'].tolist()
    clean_data = [record for record in data if record[0] not in existing_products]

    # Insert filtered records into Redshift
    if not clean_data:
        print('No new records to insert.')
        return
    values_str = ",\n".join(f"('{p}', '{n}', '{c}', {pr}, '{s}')" for p, n, c, pr, s in clean_data)
    insert_query = f"""
        INSERT INTO public.dim_products (product_id, product_name, category, price, supplier_id) 
        VALUES {values_str}; """
    df_sql(sql=insert_query, is_insert=True)
    print(f"✅ Inserted {len(data)} new records successfully in public.dim_products.")



def fake_dim_customers_table(number_of_potential_newrecords: int = 10):
    fake = Faker()
    membership_levels = ["Bronze", "Silver", "Gold", "Platinum"]
    data = []

    for _ in range(number_of_potential_newrecords):
        # Generate unique customer ID
        unique_id_pattern = ''.join([str(random.randint(0, 9)) for _ in range(6)])
        customer_id = f"cust-{unique_id_pattern}"

        # Realistic customer fields
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()  # real-looking email

        membership_level = random.choices(
            membership_levels,
            weights=[0.60, 0.25, 0.10, 0.05],  # realistic distribution
            k=1
        )[0]

        record = (
            customer_id,
            first_name,
            last_name,
            email,
            membership_level
        )
        data.append(record)

    # Remove duplicates already in the table
    existing_customers = df_sql(
        sql="SELECT customer_id FROM public.dim_customers;",
        is_insert=False
    )['customer_id'].tolist()

    clean_data = [record for record in data if record[0] not in existing_customers]

    if not clean_data:
        print("No new customer records to insert.")
        return

    values_str = ",\n".join(
        f"('{cid}', '{fn}', '{ln}', '{em}', '{ml}')"
        for cid, fn, ln, em, ml in clean_data
    )

    insert_query = f"""
        INSERT INTO public.dim_customers (customer_id, first_name, last_name, email, membership_level)
        VALUES {values_str};
    """

    df_sql(sql=insert_query, is_insert=True)
    print(f"✅ Inserted {len(clean_data)} new Faker-based customers.")


def dimmension_tables_mock_lambda_handler(event, context):
    try:
        fake_dims_product_table(number_of_potential_newrecords=10)
        fake_dim_customers_table(number_of_potential_newrecords=10)
        return {
            'statusCode': 200,
            'body': 'Mock dimension tables data generation completed.'
        }
    except Exception as e:
        print(f"Error in dimension tables mock data generation: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }   
 
# if __name__ == "__main__":
#     print("Generating mock data for dimension tables...")
#     fake_dims_product_table(number_of_potential_newrecords=10)
#     fake_dim_customers_table(number_of_potential_newrecords=10)
#     print("Mock data generation completed.")
