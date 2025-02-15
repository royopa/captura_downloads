import pandas as pd
import pandas_gbq
from google.cloud import bigquery

project_id = 'kyd-storage'

client = bigquery.Client(project=project_id)
query_job = client.query(
    """
drop table if exists layer1_b3.tb_equities_adjusted
"""
)
results = query_job.result()

df = pd.read_gbq(
    """
SELECT
  symbol,
  max(ed.traded_contracts) max_contracts,
  avg(ed.traded_contracts) avg_contracts
FROM
  `layer1_b3.tb_equities`, unnest(equity_data) as ed
where
  ed.refdate >= '2022-01-01'
  and ed.traded_contracts > 0
group by
  symbol""",
    project_id=project_id,
    dialect='standard',
)

df = df[df['avg_contracts'] >= 100000].copy()

for symbol in df['symbol'][:10]:
    print(symbol)
    df_eq = pd.read_gbq(
        f"""
SELECT symbol, ed.*  FROM `kyd-storage.layer1_b3.tb_equities`, unnest(equity_data) as ed
where
symbol = '{symbol}'
    """,
        project_id=project_id,
        dialect='standard',
    )

    df_eq['refdate'] = df_eq['refdate'].astype(str)
    # print(df_eq.dtypes)

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

    pandas_gbq.to_gbq(
        df_eq,
        'layer1_b3.tb_equities_adjusted',
        project_id=project_id,
        if_exists='append',
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


df_ad = df_eq.groupby('symbol').apply(_)
