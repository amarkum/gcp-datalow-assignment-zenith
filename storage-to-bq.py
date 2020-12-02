import os

from google.cloud import bigquery

try:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../auth.json"
except:
    print("Please check if authentication file is available")

client = bigquery.Client()

projectid = 'snake-case-000000'
dataset_id = 'dc-rh-bucketdata-dataset'
table_id = 'dc-rh'

tablefullname = projectid + "." + dataset_id + "." + table_id

print('Connecting to BigQuery Database.')
client = bigquery.Client(project=projectid)

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

job_config = bigquery.LoadJobConfig(
 schema = [
     bigquery.SchemaField("TIMESTAMP", "DATE", mode="NULLABLE"),
     bigquery.SchemaField("CUSTOMER", "STRING", mode="NULLABLE"),
     bigquery.SchemaField("POINTNAME", "STRING", mode="NULLABLE"),
     bigquery.SchemaField("VALUE", "FLOAT", mode="NULLABLE"),

 ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)

job_config.skip_leading_rows = 1
job_config.autodetect = True


uri = "gs://dc-rh-bucketdata/data/2018/10/rh_201810.csv"

load_job = client.load_table_from_uri(
    uri, tablefullname, job_config=job_config
)

load_job.result()

destination_table = client.get_table(tablefullname)

print('Loaded {} rows into {}:{}.'.format(load_job.output_rows, dataset_id, table_id))
print("Total {} rows in the Table.".format(destination_table.num_rows))
