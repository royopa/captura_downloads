import datetime

from airflow import models
from airflow.operators import bash
from airflow.providers.google.cloud.operators import bigquery
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

bq_dataset_name = 'kyd-storage:layer1_b3'

# If you are running Airflow in more than one time zone
# see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# for best practices
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Load B3 Data to bigquery',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

with models.DAG(
    'load_b3_data_to_bigquery',
    'catchup=False',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:

    make_bq_dataset = bash.BashOperator(
        task_id='make_bq_dataset',
        # Executing 'bq' command requires Google Cloud SDK which comes
        # preinstalled in Cloud Composer.
        bash_command=f'bq ls {bq_dataset_name} || bq mk {bq_dataset_name}',
    )

    load_raw_indexdata = GCSToBigQueryOperator(
        task_id='load_raw_indexdata',
        bucket='ks-layer1',
        source_objects=['BVBG087/IndxInf/*.parquet'],
        source_format='PARQUET',
        destination_project_dataset_table=f'{bq_dataset_name}.raw_indexdata',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    load_raw_iopvdata = GCSToBigQueryOperator(
        task_id='load_raw_iopvdata',
        bucket='ks-layer1',
        source_objects=['BVBG087/IOPVInf/*.parquet'],
        source_format='PARQUET',
        destination_project_dataset_table=f'{bq_dataset_name}.raw_iopvdata',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    load_raw_bdrdata = GCSToBigQueryOperator(
        task_id='load_raw_bdrdata',
        bucket='ks-layer1',
        source_objects=['BVBG087/BDRInf/*.parquet'],
        source_format='PARQUET',
        destination_project_dataset_table=f'{bq_dataset_name}.raw_bdrdata',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    load_raw_marketdata = GCSToBigQueryOperator(
        task_id='load_raw_marketdata',
        bucket='ks-layer1',
        source_objects=['BVBG086/*.parquet'],
        source_format='PARQUET',
        destination_project_dataset_table=f'{bq_dataset_name}.raw_marketdata',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    load_raw_equitydata = GCSToBigQueryOperator(
        task_id='load_raw_equitydata',
        bucket='ks-layer1',
        source_objects=['BVBG028/EqtyInf/*.parquet'],
        source_format='PARQUET',
        destination_project_dataset_table=f'{bq_dataset_name}.raw_equitydata',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    load_raw_futuredata = GCSToBigQueryOperator(
        task_id='load_raw_futuredata',
        bucket='ks-layer1',
        source_objects=['BVBG028/FutrCtrctsInf/*.parquet'],
        source_format='PARQUET',
        destination_project_dataset_table=f'{bq_dataset_name}.raw_futuredata',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    load_raw_equityoptiondata = GCSToBigQueryOperator(
        task_id='load_raw_equityoptiondata',
        bucket='ks-layer1',
        source_objects=['BVBG028/OptnOnEqtsInf/*.parquet'],
        source_format='PARQUET',
        destination_project_dataset_table=f'{bq_dataset_name}.raw_equityoptiondata',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    load_raw_cotahist = GCSToBigQueryOperator(
        task_id='load_raw_cotahist',
        bucket='ks-layer1',
        source_objects=['COTAHIST/*.parquet'],
        source_format='PARQUET',
        destination_project_dataset_table=f'{bq_dataset_name}.raw_cotahistdata',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    QUERY_TB_EQUITY = """
create or replace table `kyd-storage.layer1_b3.tb_equities` as
with EQ AS (
	SELECT
		eq.trade_date,
		eq.symbol,
		eq.isin,
		eq.security_id,
		eq.distribution_id
	FROM `kyd-storage.layer1_b3.raw_equitydata` eq
	WHERE
		eq.instrument_market = '10'
		and eq.instrument_asset <> 'TAXA'
		and eq.trading_start_date <> '9999-12-31'
		and eq.instrument_segment = '1'
		and eq.security_category in ('11', '13', '3')
), CH AS (
	SELECT
	    data_referencia,
	    cod_negociacao,
	    cod_isin,
	    preco_abertura,
	    preco_min,
	    preco_max,
	    preco_ult,
	    volume_titulos_negociados,
	    LAG(preco_ult) OVER (PARTITION BY cod_negociacao ORDER BY data_referencia ASC) prev_preco_ult
	FROM `kyd-storage.layer1_b3.raw_cotahistdata` er
	WHERE
	    tipo_mercado = '010'
), CHEQ AS (
	SELECT
		SAFE_CAST(eq.trade_date AS DATE) refdate,
		eq.symbol,
	    ch.preco_abertura open,
	    ch.preco_min low,
	    ch.preco_max high,
	    ch.preco_ult close,
	    ch.volume_titulos_negociados volume,
		TRUNC(100*(ch.preco_ult / ch.prev_preco_ult - 1), 2) oscillation_percentage,
		SAFE_CAST(eq.distribution_id AS INT) distribution_id
	FROM EQ eq
	LEFT JOIN CH ch ON
		CAST(eq.trade_date AS DATE) = CAST(ch.data_referencia AS DATE)
		AND eq.isin = ch.cod_isin
    WHERE
        SAFE_CAST(eq.trade_date AS DATE) = '2021-06-10'
), EQMD AS (
    SELECT
        SAFE_CAST(eq.trade_date AS date) refdate,
        eq.symbol,
        SAFE_CAST(md.first_price AS float64) open,
        SAFE_CAST(md.min_price AS float64) low,
        SAFE_CAST(md.max_price AS float64) high,
        SAFE_CAST(md.last_price AS float64) close,
        SAFE_CAST(md.volume AS float64) volume,
        SAFE_CAST(md.oscillation_percentage AS float64) oscillation_percentage,
        SAFE_CAST(eq.distribution_id AS int64) distribution_id
    FROM
        `kyd-storage.layer1_b3.raw_equitydata` eq
    INNER JOIN
        `kyd-storage.layer1_b3.raw_marketdata` md
    ON
        eq.trade_date = md.trade_date
        AND eq.security_id = md.security_id
    WHERE
        eq.instrument_asset <> 'TAXA'
        AND eq.trading_start_date <> '9999-12-31'
        AND eq.instrument_market = '10'
        AND eq.instrument_segment = '1'
        AND eq.security_category IN ('11',
            '3',
            '13')
), EQ_FINAL AS (
    SELECT * FROM EQMD
    UNION ALL
    SELECT * FROM CHEQ
)
select 
    symbol,
    array_agg(struct (
        refdate,
        open,
        low,
        high,
        close,
        volume,
        oscillation_percentage,
        distribution_id
    ) order by refdate) as equity_data
from 
    EQ_FINAL
group by 
    symbol;
"""
    bq_create_tb_equity = bigquery.BigQueryInsertJobOperator(
        task_id='bq_create_tb_equity',
        configuration={
            'query': {
                'query': QUERY_TB_EQUITY,
                'useLegacySql': False,
            }
        },
        location='US',
    )

    make_bq_dataset >> [
        load_raw_indexdata,
        load_raw_marketdata,
        load_raw_equitydata,
        load_raw_equityoptiondata,
        load_raw_futuredata,
        load_raw_bdrdata,
        load_raw_iopvdata,
        load_raw_cotahist,
    ]

    [
        load_raw_marketdata,
        load_raw_equitydata,
        load_raw_cotahist,
    ] >> bq_create_tb_equity
