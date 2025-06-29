import logging

import pandas as pd
import pandas_gbq
from bizdays import Calendar, set_option

set_option("mode", "pandas")


project_id = "kyd-storage"

df_symbols = pandas_gbq.read_gbq(
    """
select
    symbol,
    min(ed.refdate) first_date,
    max(ed.refdate) last_date
from `kyd-storage.layer1_b3.tb_equities` eq, unnest(equity_data) ed
group by symbol
""",
    project_id,
)


def _(symbol, first_date, last_date):
    query = f"""
with eq_data as (
    select symbol, ed.refdate from `kyd-storage.layer1_b3.tb_equities` eq, unnest(equity_data) ed
    where symbol = '{symbol}'
), cal as (
    select
        safe_cast(refdate as date) refdate
    from `kyd-storage.layer1_b3.tb_calendar`
    where isbizday_B3 = true and refdate between '{first_date}' and '{last_date}'
)
select cal.refdate, ed.symbol from cal
left join eq_data ed on cal.refdate = ed.refdate
where symbol is null
"""
    return pandas_gbq.read_gbq(
        query,
        project_id,
    )


logger = logging.getLogger("pandas_gbq")
logger.setLevel(logging.ERROR)
logger.addHandler(logging.StreamHandler())

gaps = []
for idx in df_symbols.index:
    symbol = df_symbols.loc[idx, "symbol"]
    print(symbol)
    last_date = df_symbols.loc[idx, "last_date"]
    first_date = df_symbols.loc[idx, "first_date"]
    df = _(symbol, first_date, last_date)
    df["symbol"] = symbol
    gaps.append(df)

df_count = pd.concat(gaps)
df_count["symbol"].value_counts().to_csv("gap_count.csv")
df_count.to_csv("gaps.csv")
cal = Calendar.load("B3")
df_symbols["bizdays"] = cal.bizdays(df_symbols["first_date"], df_symbols["last_date"])
df_symbols.to_csv("dates.csv")
