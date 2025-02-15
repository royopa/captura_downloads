-- equities historical register

CREATE OR REPLACE TABLE
  `kyd-storage.layer1_b3.tb_equities_historical_register` AS
WITH
  EQ AS (
  SELECT
    SAFE_CAST(eq.trade_date AS date) refdate,
    eq.symbol,
    eq.isin,
    eq.security_id,
    eq.distribution_id,
    SAFE_CAST(eq.security_category as INT) security_category
  FROM
    `kyd-storage.layer1_b3.raw_equitydata` eq
  WHERE
    eq.instrument_market = '10'
    AND eq.instrument_asset <> 'TAXA'
    AND eq.trading_start_date <> '9999-12-31'
    AND eq.instrument_segment = '1'
    ),
  CH AS (
  SELECT
    distinct
    SAFE_CAST(data_referencia AS DATE) data_referencia,
    cod_negociacao,
    cod_isin,
    num_dist
  FROM
    `kyd-storage.layer1_b3.raw_cotahistdata` er
  WHERE
    tipo_mercado = '010' ),
  CHEQ AS (
  SELECT
    ch.data_referencia refdate,
    ch.cod_negociacao symbol,
    eq.security_id,
    ch.cod_isin isin,
    sc.description security_category,
    SAFE_CAST(ch.num_dist AS INT) distribution_id
  FROM
    CH ch
  LEFT JOIN
    EQ eq
  ON
    eq.refdate = CAST(ch.data_referencia AS DATE)
    AND eq.isin = ch.cod_isin
    AND eq.distribution_id = ch.num_dist
  LEFT JOIN
    `kyd-storage.layer1_b3.tb_security_category` sc
  ON
    sc.security_category = eq.security_category
  )
SELECT
  *
FROM
  CHEQ

-- tabela auxiliar de consolidação de dados de equities
-- junta histórico de ações de marketdata e cotahist, filtrando por calendário
-- B3 e pegando instrumentos de tabela de cadastro de instrumentos

CREATE OR REPLACE TABLE
  `kyd-storage.layer1_b3.tb_equities_aux` AS
WITH
  CAL AS (
  SELECT
    SAFE_CAST(cal.refdate AS date) refdate,
    cal.isbizday_B3,
  FROM
    `kyd-storage.layer1_b3.tb_calendar` cal
  WHERE
    cal.isbizday_B3 = TRUE
    AND SAFE_CAST(cal.refdate AS date) <= CURRENT_DATE() ),
  EQ_CAL AS (
  SELECT
    CAL.refdate,
    EQ.symbol,
    EQ.isin,
    EQ.security_id,
    EQ.security_category,
    EQ.distribution_id
  FROM
    CAL
  LEFT JOIN
    `kyd-storage.layer1_b3.tb_equities_historical_register` EQ
  ON
    CAL.refdate = EQ.refdate ),
  CH AS (
  SELECT
    DISTINCT
    er.cod_isin isin,
    SAFE_CAST(er.data_referencia AS DATE) refdate,
    SAFE_CAST(er.preco_abertura AS FLOAT64) ch_open,
    SAFE_CAST(er.preco_min AS FLOAT64) ch_low,
    SAFE_CAST(er.preco_max AS FLOAT64) ch_high,
    SAFE_CAST(er.preco_ult AS FLOAT64) ch_close,
    SAFE_CAST(er.volume_titulos_negociados AS FLOAT64) ch_volume,
    SAFE_CAST(er.qtd_negocios AS FLOAT64) ch_trade_quantity,
    SAFE_CAST(er.qtd_titulos_negociados AS FLOAT64) ch_traded_contracts,
  FROM
    `kyd-storage.layer1_b3.raw_cotahistdata` er
  WHERE
      er.tipo_mercado = '010'
  ),
  EQ_CH_MD AS (
  SELECT
    ec.symbol,
    ec.isin,
    ec.security_category,
    ec.refdate,
    SAFE_CAST(ec.distribution_id AS int) distribution_id,
    LAG(SAFE_CAST(ec.distribution_id AS int)) OVER (PARTITION BY ec.symbol ORDER BY ec.refdate ASC) prev_distribution_id,
    ec.security_id,
    er.ch_open,
    er.ch_low,
    er.ch_high,
    er.ch_close,
    LAG(er.ch_close) OVER (PARTITION BY er.isin ORDER BY er.refdate ASC) ch_prev_close,
    er.ch_volume,
    er.ch_trade_quantity,
    er.ch_traded_contracts,
    SAFE_CAST(md.first_price AS float64) md_open,
    SAFE_CAST(md.min_price AS float64) md_low,
    SAFE_CAST(md.max_price AS float64) md_high,
    SAFE_CAST(md.last_price AS float64) md_close,
    SAFE_CAST(md.volume AS float64) md_volume,
    SAFE_CAST(md.trade_quantity AS float64) md_trade_quantity,
    SAFE_CAST(md.traded_contracts AS float64) md_traded_contracts,
    SAFE_CAST(md.oscillation_percentage AS float64) md_oscillation_percentage,
  FROM
    EQ_CAL ec
  LEFT JOIN
    CH er
  ON
    ec.refdate = er.refdate
    AND ec.isin = er.isin
  LEFT JOIN
    `kyd-storage.layer1_b3.raw_marketdata` md
  ON
    ec.refdate = SAFE_CAST(md.trade_date AS date)
    AND ec.symbol = md.symbol
  ),
EQ_FINAL AS (
  SELECT
    symbol,
    isin,
    security_category,
    refdate,
    distribution_id,
    (distribution_id - prev_distribution_id) change_distribution_id,
    security_id,
    ch_open,
    ch_low,
    ch_high,
    ch_close,
    ch_prev_close,
    ch_volume,
    ch_trade_quantity,
    ch_traded_contracts,
    TRUNC(100*(SAFE_DIVIDE(ch_close, ch_prev_close) - 1), 2) ch_oscillation_percentage,
    md_open,
    md_low,
    md_high,
    md_close,
    md_volume,
    md_trade_quantity,
    md_traded_contracts,
    md_oscillation_percentage
  FROM
    EQ_CH_MD
),
EQ_FORMAT AS (
SELECT
  symbol,
  isin,
  security_category,
  ARRAY_AGG(STRUCT ( refdate,
  distribution_id,
  change_distribution_id,
  security_id,
  ch_open,
  ch_low,
  ch_high,
  ch_close,
  ch_prev_close,
  ch_volume,
  ch_trade_quantity,
  ch_traded_contracts,
  ch_oscillation_percentage,
  md_open,
  md_low,
  md_high,
  md_close,
  md_volume,
  md_trade_quantity,
  md_traded_contracts,
  md_oscillation_percentage)
  ORDER BY
    refdate) AS equity_data
FROM
  EQ_FINAL
GROUP BY
  symbol,
  isin,
  security_category
)
select * from eq_format

-- adjusted prices

SELECT
  symbol,
  ed.refdate,
  case when ed.md_close is null and (ed.change_distribution_id = 0 or ed.change_distribution_id is null) then ed.ch_close
  else ed.md_close end close,
  case when ed.md_oscillation_percentage is null and (ed.change_distribution_id = 0 or ed.change_distribution_id is null) then ed.ch_oscillation_percentage
  else ed.md_oscillation_percentage end oscillation_percentage,
  ed.distribution_id,
  ed.change_distribution_id
FROM
  `kyd-storage.layer1_b3.tb_equities_aux` eq,
  UNNEST(equity_data) ed


-- equities register

CREATE OR REPLACE TABLE
  `kyd-storage.layer1_b3.tb_equities_register` AS
WITH
  EQ AS (
  SELECT
    distinct
      eq.symbol,
      eq.isin,
      eq.security_id,
      eq.distribution_id,
      SAFE_CAST(eq.security_category as INT) security_category
  FROM
    `kyd-storage.layer1_b3.raw_equitydata` eq
  WHERE
    eq.instrument_market = '10'
    AND eq.instrument_asset <> 'TAXA'
    AND eq.trading_start_date <> '9999-12-31'
    AND eq.instrument_segment = '1'
    ),
  CH AS (
  SELECT
    distinct
      cod_negociacao,
      cod_isin,
      num_dist
  FROM
    `kyd-storage.layer1_b3.raw_cotahistdata` er
  WHERE
    tipo_mercado = '010' ),
  CHEQ AS (
  SELECT
    ch.cod_negociacao symbol,
    eq.security_id,
    ch.cod_isin isin,
    sc.description security_category,
    SAFE_CAST(ch.num_dist AS INT) distribution_id
  FROM
    CH ch
  LEFT JOIN
    EQ eq
  ON
    eq.isin = ch.cod_isin
    AND eq.distribution_id = ch.num_dist
  LEFT JOIN
    `kyd-storage.layer1_b3.tb_security_category` sc
  ON
    sc.security_category = eq.security_category
  )
SELECT
  *
FROM
  CHEQ

-- TODO: tb_equities: open, close, high, low, volume, trades, contracts, distribution_id, security_category, isin


-- query para verificação de calendário de equities
-- TODO: fazer o mesmo com futures, índices ...
with EQ as (
  SELECT
    eq.trade_date refdate,
    count(*) count_equitydata
  FROM
    `kyd-storage.layer1_b3.raw_equitydata` eq
  group by trade_date
),
MD as (
  SELECT
    eq.trade_date refdate,
    count(*) count_marketdata
  FROM
    `kyd-storage.layer1_b3.raw_marketdata` eq
  group by trade_date
),
CH as (
  SELECT
    eq.data_referencia refdate,
    count(*) count_cotahistdata
  FROM
    `kyd-storage.layer1_b3.raw_cotahistdata` eq
  group by data_referencia
),
CAL as (
  SELECT
    safe_cast(cal.refdate as date) refdate,
    cal.is_holiday_B3,
  FROM
    `kyd-storage.layer1_b3.tb_calendar` cal
  where
    cal.is_holiday_B3 = true
    and safe_cast(cal.refdate as date) <= current_date()
)
SELECT
  CAL.refdate,
  EQ.count_equitydata,
  MD.count_marketdata,
  CH.count_cotahistdata,
FROM
  CAL
  left join EQ on safe_cast(CAL.refdate as date) = safe_cast(EQ.refdate as date)
  left join MD on safe_cast(CAL.refdate as date) = safe_cast(MD.refdate as date)
  left join CH on safe_cast(CAL.refdate as date) = safe_cast(CH.refdate as date)



-- verificação de gaps por equity x calendário

with eq_data as (
    select symbol, ed.refdate from `kyd-storage.layer1_b3.tb_equities_aux` eq, unnest(equity_data) ed
    where symbol = 'VALE3'
), cal as (
    select
        safe_cast(refdate as date) refdate
    from `kyd-storage.layer1_b3.tb_calendar`
    where isbizday_B3 = true and safe_cast(refdate as date) <= current_date()
)
select cal.refdate, ed.symbol from cal
left join eq_data ed on cal.refdate = ed.refdate
where symbol is null

-- contagem de gaps por equity

select symbol, ed.refdate, ed.md_close, ed.ch_close from `kyd-storage.layer1_b3.tb_equities_aux` eq, unnest(equity_data) ed
where symbol = 'PETR4' and ed.md_close is null


-- check for gaps

select symbol, ed.refdate, ed.md_close, ed.ch_close,
  ed.ch_distribution_id, ed.ch_change_distribution_id, ed.md_change_distribution_id
from `kyd-storage.layer1_b3.tb_equities_aux` eq, unnest(equity_data) ed
where symbol = 'ALPA4' and refdate between '2016-01-01' and '2016-03-10'
order by refdate


-- create partitioned table
CREATE OR REPLACE TABLE
  `kyd-storage.layer1_b3.tb_equities_adjusted_aux_part`
PARTITION BY
  refdate
AS
SELECT
  symbol,
  SAFE_CAST(refdate AS DATE) as refdate,
  adj_close as close,
  oscillation_percentage,
  distribution_id
FROM
  `kyd-storage.layer1_b3.tb_equities_adjusted_aux`

-- create equity prices table

CREATE OR REPLACE TABLE
  `kyd-storage.layer1_b3.tb_equity_prices`
PARTITION BY
  refdate
AS
SELECT
  symbol,
  SAFE_CAST(refdate AS DATE) as refdate,
  adj_close as close,
  oscillation_percentage,
  distribution_id
FROM
  `kyd-storage.layer1_b3.tb_equities_adjusted_aux`

UNION ALL

SELECT
  ticker_symbol symbol,
  SAFE_CAST(trade_date as date) refdate,
  SAFE_CAST(close_price as FLOAT64) close,
  100*SAFE_CAST(oscillation_val as FLOAT64) oscillation_percentage,
  NULL distribution_id
FROM `kyd-storage.layer1.raw-b3-BVBG087-index-info`
