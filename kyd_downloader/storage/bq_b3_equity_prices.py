import logging

import pandas as pd
import pandas_gbq
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

project_id = 'kyd-storage'

client = bigquery.Client(project=project_id)

query_job = client.query(
    """
CREATE OR REPLACE TABLE
`kyd-storage.layer1_b3.tb_equities` AS
WITH
EQ AS (
SELECT
    eq.trade_date,
    eq.symbol,
    eq.isin,
    eq.security_id,
    eq.distribution_id
FROM
    `kyd-storage.layer1_b3.raw_equitydata` eq
WHERE
    eq.instrument_market = '10'
    AND eq.instrument_asset <> 'TAXA'
    AND eq.trading_start_date <> '9999-12-31'
    AND eq.instrument_segment = '1'
    AND eq.security_category IN ('11',
    '13',
    '3') ),
CH AS (
SELECT
    data_referencia,
    cod_negociacao,
    cod_isin,
    preco_abertura,
    preco_min,
    preco_max,
    preco_ult,
    volume_titulos_negociados,
    qtd_negocios,
    qtd_titulos_negociados,
    LAG(preco_ult) OVER (PARTITION BY cod_negociacao ORDER BY data_referencia ASC) prev_preco_ult
FROM
    `kyd-storage.layer1_b3.raw_cotahistdata` er
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
    ch.qtd_negocios trade_quantity,
    ch.qtd_titulos_negociados traded_contracts,
    TRUNC(100*(ch.preco_ult / ch.prev_preco_ult - 1), 2) oscillation_percentage,
    SAFE_CAST(eq.distribution_id AS INT) distribution_id
FROM
    EQ eq
LEFT JOIN
    CH ch
ON
    CAST(eq.trade_date AS DATE) = CAST(ch.data_referencia AS DATE)
    AND eq.isin = ch.cod_isin
WHERE
    SAFE_CAST(eq.trade_date AS DATE) = '2021-06-10'),
EQMD AS (
SELECT
    SAFE_CAST(eq.trade_date AS date) refdate,
    eq.symbol,
    SAFE_CAST(md.first_price AS float64) open,
    SAFE_CAST(md.min_price AS float64) low,
    SAFE_CAST(md.max_price AS float64) high,
    SAFE_CAST(md.last_price AS float64) close,
    SAFE_CAST(md.volume AS float64) volume,
    SAFE_CAST(md.trade_quantity AS float64) trade_quantity,
    SAFE_CAST(md.traded_contracts AS float64) traded_contracts,
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
    AND eq.security_category IN ('11', '3', '13')
), EQ_FINAL AS (
    SELECT * FROM EQMD WHERE close IS NOT NULL
    UNION ALL
    SELECT * FROM CHEQ WHERE close IS NOT NULL
)
SELECT
    symbol,
    ARRAY_AGG(STRUCT (
        refdate,
        open,
        low,
        high,
        close,
        volume,
        trade_quantity,
        traded_contracts,
        oscillation_percentage,
        distribution_id) ORDER BY refdate) AS equity_data
FROM EQ_FINAL
GROUP BY symbol;
"""
)
results = query_job.result()


# query_job = client.query('''
# drop table if exists layer1_b3.tb_equities_adjusted
# ''')
# results = query_job.result()

df_eq = pd.read_gbq(
    """
SELECT symbol, ed.*  FROM `kyd-storage.layer1_b3.tb_equities`, unnest(equity_data) as ed
WHERE
symbol in (
SELECT distinct symbol
FROM `kyd-storage.layer1_b3.raw_stockindexinfo`
WHERE specification_code not in ('DRN', 'CI')
)
""",
    project_id=project_id,
    dialect='standard',
)


def _(df_eq):
    df_eq['refdate'] = df_eq['refdate'].astype(str)
    df_eq = df_eq.sort_values('refdate', ascending=False).reset_index()
    f = 1 + df_eq['oscillation_percentage'] / 100
    f = f.cumprod()
    f = f.shift()
    f.iloc[0] = 1
    df_eq['adj_close'] = df_eq.loc[0, 'close'] / f
    df_eq['adj_open'] = df_eq['adj_close'] * (
        1 - (df_eq['close'] - df_eq['open']) / df_eq['close']
    )
    df_eq['adj_high'] = df_eq['adj_close'] * (
        1 - (df_eq['close'] - df_eq['high']) / df_eq['close']
    )
    df_eq['adj_low'] = df_eq['adj_close'] * (
        1 - (df_eq['close'] - df_eq['low']) / df_eq['close']
    )

    return df_eq


df_ad = df_eq.groupby('symbol').apply(_).reset_index(drop=True)

pandas_gbq.to_gbq(
    df_ad,
    'layer1_b3.tb_equities_adjusted_aux',
    project_id=project_id,
    if_exists='replace',
)

query_job = client.query(
    """
CREATE OR REPLACE TABLE
  `kyd-storage.layer1_b3.tb_equities_adjusted` AS
SELECT
  symbol,
  ARRAY_AGG(STRUCT (
      SAFE_CAST(refdate AS DATE) as refdate,
      adj_open as open,
      adj_low as low,
      adj_high as high,
      adj_close as close,
      volume,
      trade_quantity,
      traded_contracts,
      oscillation_percentage,
      distribution_id )
  ORDER BY
    refdate) AS equity_data
FROM
  `kyd-storage.layer1_b3.tb_equities_adjusted_aux`
GROUP BY
  symbol;
"""
)
results = query_job.result()
