import logging
import time

import pandas as pd
import pandas_gbq
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

project_id = "kyd-storage"

client = bigquery.Client(project=project_id)

logging.info("fetching data")
start_time = time.time()
df_eq = pd.read_gbq(
    """
  SELECT
    ret.refdate,
    ret.symbol,
    ret.pct_return,
    safe_cast(md.last_price as float64) close,
    safe_cast(md.max_price as float64) high,
    safe_cast(md.min_price as float64) low,
    safe_cast(md.first_price as float64) open,
  FROM
    `kyd-storage.layer2.tb_returns` ret
  LEFT JOIN
    `kyd-storage.layer1.raw-b3-BVBG086` md
  ON
    md.symbol = ret.symbol
    AND md.trade_date = '2023-04-28'
  WHERE
    ret.symbol IN (
    SELECT
      symbol
    FROM
      `kyd-storage.layer2.tb_equities_aux`
    WHERE
      security_category IN ('SHARES',
        'UNIT',
        'BDR') )
    AND md.last_price IS NOT NULL
  ORDER BY
    ret.symbol,
    ret.refdate desc
""",
    project_id=project_id,
    dialect="standard",
)

# 'SHARES', 'FUNDS', 'UNIT', 'BDR', 'ETF FOREIGN INDEX', 'ETF EQUITIES', 'INDEX'

end_time = time.time()

logging.info("data selected")


def _(df_eq):
    logging.info(f"{df_eq['symbol'].iloc[0]} - rows: {df_eq.shape[0]}")
    df_eq["refdate"] = df_eq["refdate"].astype(str)
    df_eq = df_eq.sort_values("refdate", ascending=False).reset_index()
    f = 1 + df_eq["pct_return"]
    f = f.cumprod()
    f = f.shift()
    f.iloc[0] = 1
    df_eq["close"] = df_eq.loc[0, "close"] / f
    # df_eq = df_eq.drop("close", axis="columns")

    return df_eq


logging.info("processing")

df_ad = df_eq.groupby("symbol").apply(_).reset_index(drop=True)

end_time_process = time.time()

pandas_gbq.to_gbq(
    df_ad,
    "layer2.tb_equities_adjusted_aux",
    project_id=project_id,
    if_exists="replace",
)

end_time_push = time.time()

# query_job = client.query(
#     """
# CREATE OR REPLACE TABLE
#   `kyd-storage.layer1_b3.tb_equities_adjusted` AS
# SELECT
#   symbol,
#   ARRAY_AGG(STRUCT (
#       SAFE_CAST(refdate AS DATE) as refdate,
#       adj_close as close,
#       oscillation_percentage,
#       distribution_id )
#   ORDER BY
#     refdate) AS equity_data
# FROM
#   `kyd-storage.layer1_b3.tb_equities_adjusted_aux`
# GROUP BY
#   symbol;
# """
# )
# results = query_job.result()

logging.info("Download time = {:.3f} seconds".format(end_time - start_time))
logging.info("Process time  = {:.3f} seconds".format(end_time_process - end_time))
logging.info("Push time     = {:.3f} seconds".format(end_time_push - end_time_process))
