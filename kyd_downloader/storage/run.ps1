
.\venv\Scripts\Activate.ps1

.\Setenv.ps1

python bq_load_b3_data.py

python bq_b3_equity_prices.py

python bq_b3_futures.py

python bq_load_anbima_data.py