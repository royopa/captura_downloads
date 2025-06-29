import logging

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

project_id = "kyd-storage"

client = bigquery.Client(project=project_id)

query_job = client.query(
    """
CREATE OR REPLACE TABLE
    `kyd-storage.layer1_b3.tb_futures` AS
WITH
FUT AS (
    SELECT
        dx.symbol,
        dx.instrument_asset,
        dx.expiration_code as maturity_code,
        SAFE_CAST(dx.trading_start_date as DATE) trading_start_date,
        SAFE_CAST(dx.trading_end_date as DATE) trading_end_date,
        SAFE_CAST(dx.expiration_date as DATE) maturity_date,
        SAFE_CAST(dx.trade_date AS date) refdate,
        SAFE_CAST(md.first_price AS float64) open,
        SAFE_CAST(md.min_price AS float64) low,
        SAFE_CAST(md.max_price AS float64) high,
        SAFE_CAST(md.last_price AS float64) close,
        SAFE_CAST(md.adjusted_tax AS float64) adjusted_tax,
        SAFE_CAST(md.volume AS float64) volume,
        SAFE_CAST(md.trade_quantity AS float64) trade_quantity,
        SAFE_CAST(md.traded_contracts AS float64) traded_contracts,
        SAFE_CAST(md.open_interest AS float64) open_interest
    FROM
        `kyd-storage.layer1_b3.raw_futuredata` dx
    INNER JOIN
        `kyd-storage.layer1_b3.raw_marketdata` md
    ON
        dx.trade_date = md.trade_date
        AND dx.security_id = md.security_id
)
SELECT
    symbol,
    instrument_asset,
    maturity_code,
    maturity_date,
    trading_start_date,
    trading_end_date,
    ARRAY_AGG(STRUCT (
        refdate,
        open,
        low,
        high,
        close,
        adjusted_tax,
        volume,
        trade_quantity,
        traded_contracts,
        open_interest
    ) ORDER BY refdate) as contract_data
FROM
    FUT
GROUP BY
    symbol,
    instrument_asset,
    maturity_code,
    maturity_date,
    trading_start_date,
    trading_end_date;
"""
)
results = query_job.result()
