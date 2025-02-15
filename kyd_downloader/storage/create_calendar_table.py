from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
from bizdays import Calendar, set_option

set_option('mode', 'pandas')


dates = []
date = datetime(2016, 1, 1)
while date <= datetime(2023, 12, 29):
    dates.append(date)
    date += timedelta(days=1)

df_dates = pd.DataFrame({'refdate': dates})

cal = Calendar.load('ANBIMA')
df_dates['isbizday_ANBIMA'] = cal.isbizday(df_dates['refdate'])

cal = Calendar.load('B3')
df_dates['isbizday_B3'] = cal.isbizday(df_dates['refdate'])

project_id = 'kyd-storage'

pandas_gbq.to_gbq(
    df_dates,
    'layer1_b3.tb_calendar',
    project_id=project_id,
    if_exists='replace',
)
