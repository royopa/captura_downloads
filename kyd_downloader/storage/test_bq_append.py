import io

from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.

# job_config = bigquery.LoadJobConfig(
#     schema=[
#         bigquery.SchemaField('refdata', 'STRING'),
#         bigquery.SchemaField('value', 'FLOAT'),
#         bigquery.SchemaField('rate', 'FLOAT'),
#         bigquery.SchemaField('rate_start_date', 'STRING'),
#         bigquery.SchemaField('proj', 'BOOLEAN'),
#         bigquery.SchemaField('index_ref', 'STRING'),
#         bigquery.SchemaField('instrument_ref', 'STRING'),
#     ],
# )

# body = io.BytesIO(b'Washington,WA')
# client.load_table_from_file(body, table_id, job_config=job_config).result()
# previous_rows = client.get_table(table_id).num_rows
# assert previous_rows > 0

project_id = 'kyd-storage'
dataset_id = 'layer1_anbima'
dataset = bigquery.Dataset(dataset_id)
dataset.location = 'US'
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.


table_id = f'{project_id}.{dataset_id}.vnatitpub'

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    source_format=bigquery.SourceFormat.PARQUET,
)

uri = 'gs://ks-layer1/AnbimaVnaTitpub/*.parquet'
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)
print('Loaded {} rows.'.format(destination_table.num_rows))
