import os

from google.cloud import bigquery

try:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "auth.json"
except:
    print("Please check if authentication file is available")

# Construct a BigQuery client object.
client = bigquery.Client()

projectid = 'snake-case-0000'
dataset_id = 'dc-rh-bucketdata-dataset'
table_id = 'dc-rh'

table_full_name = projectid + "." + dataset_id + "." + table_id

print('Creating BigQuery Table : ' + table_id)
schema = [
    bigquery.SchemaField("TIMESTAMP", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("CUSTOMER", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("POINTNAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("VALUE", "FLOAT", mode="NULLABLE"),

]

table = bigquery.Table(table_full_name, schema=schema)
table = client.create_table(table) 
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)
