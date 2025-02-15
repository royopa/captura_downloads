import logging

from google.cloud import bigquery


class ParquetDataLoader:
    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = f'{project_id}.{dataset_id}'
        self.client = bigquery.Client(project=project_id)
        dataset = bigquery.Dataset(self.dataset_id)
        self.dataset = self.client.create_dataset(dataset, exists_ok=True)

    def load(self, table_id, uri):
        return self._load_table_from_uri(
            table_id, uri, bigquery.WriteDisposition.WRITE_TRUNCATE
        )

    def append(self, table_id, uri):
        return self._load_table_from_uri(
            table_id, uri, bigquery.WriteDisposition.WRITE_APPEND
        )

    def _load_table_from_uri(self, table_id, uri, write_disp):
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disp,
            source_format=bigquery.SourceFormat.PARQUET,
        )

        table_name = f'{self.dataset.dataset_id}.{table_id}'
        load_job = self.client.load_table_from_uri(
            uri, table_name, job_config=job_config
        )

        job = load_job.result()
        eps = job.ended - job.started
        logging.info(
            f'Table {table_name} loaded - elapsed time {eps.seconds:,}s'
        )
        logging.info(f'Elapsed time {eps.seconds:,}s')
        logging.info(f'{job.output_rows:,} rows')

        return job
