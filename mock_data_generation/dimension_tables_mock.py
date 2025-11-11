import random, time, boto3, pandas as pd


REGION     = "us-east-1"
WORKGROUP  = "ecomm-exercise"
DATABASE   = "dev"
SECRET_NAME = "redshift/dev/admin" 

secrets = boto3.client("secretsmanager", region_name=REGION)
rsd  = boto3.client("redshift-data",   region_name=REGION)

# 1) Resolve Secret ARN (so we don't hardcode it)
SECRET_ARN = secrets.describe_secret(SecretId=SECRET_NAME)["ARN"]

def df_sql(sql: str) -> pd.DataFrame:
    # kick off query
    r = rsd.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        SecretArn=SECRET_ARN,
        Sql=sql
    )
    sid = r["Id"]

    # wait for completion (simple poll)
    while True:
        d = rsd.describe_statement(Id=sid)
        s = d["Status"]
        if s in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(0.2)
    if s != "FINISHED":
        raise RuntimeError(d)

    # fetch results -> DataFrame
    out  = rsd.get_statement_result(Id=sid)
    cols = [c["name"] for c in out["ColumnMetadata"]]
    rows = [[list(v.values())[0] if v else None for v in rec] for rec in out["Records"]]
    return pd.DataFrame(rows, columns=cols)

def fake_dimension_tables_mock(number_of_potential_newrecords, table_name):

    if table_name == 'dim_products':
        data = []
        for i in range(number_of_potential_newrecords):
            record = {
                "product_id": f"prod-{i+1}",
                "product_name": f"Product {i+1}",
                "category": random.choice(["Electronics", "Clothing", "Home", "Books", "Toys"]),
                "price": round(random.uniform(10.0, 500.0), 2),
                "supplier_id": 'S' + ''.join([str(random.randint(0,9)) for _ in range(4)])
            }
            tuple_record = (record['product_id'], record['product_name'], record['category'], record['price'], record['supplier_id'])

            data.append(tuple_record)
        # Remove product ids that are already in the table  
        existing_products = df_sql("SELECT product_id FROM public.dim_products;")['product_id'].tolist()
        clean_data = [record for record in data if record[0] not in existing_products]

        # Insert filtered records into Redshift
        if not clean_data:
            print('No new records to insert.')
            return
        values_str = ",\n".join(f"('{p}', '{n}', '{c}', {pr}, '{s}')" for p, n, c, pr, s in clean_data)
        insert_sql = f"""
            INSERT INTO public.dim_products (product_id, product_name, category, price, supplier_id) 
            VALUES {values_str}; """
        df_sql(insert_sql)
        print(f"✅ Inserted {len(data)} new records successfully.")
    else:
        print('Nothing happened')
        pass
            
if __name__ == "__main__":
    print("Generating mock data for dimension tables...")
    fake_dimension_tables_mock(number_of_potential_newrecords=10, table_name='dim_products')

# # --- examples: your two tables ---
# dim_products  = df_sql("SELECT * FROM public.dim_products ORDER BY product_id LIMIT 10;")
# # dim_customers = df_sql("SELECT * FROM public.dim_customers ORDER BY customer_id LIMIT 10;")

# print(dim_products)
