import logging

from kyd.loader import ParquetDataLoader

logging.basicConfig(level=logging.INFO)

loader = ParquetDataLoader('kyd-storage', 'layer1_b3')

loader.load('raw_cdi', 'gs://ks-layer1/CDI/*.parquet')
loader.load('raw_indexdata', 'gs://ks-layer1/BVBG087/IndxInf/*.parquet')
loader.load('raw_iopvdata', 'gs://ks-layer1/BVBG087/IOPVInf/*.parquet')
loader.load('raw_bdrdata', 'gs://ks-layer1/BVBG087/BDRInf/*.parquet')
loader.load('raw_marketdata', 'gs://ks-layer1/BVBG086/*.parquet')
loader.load('raw_equitydata', 'gs://ks-layer1/BVBG028/EqtyInf/*.parquet')
loader.load('raw_futuredata', 'gs://ks-layer1/BVBG028/FutrCtrctsInf/*.parquet')
loader.load(
    'raw_equityoptiondata', 'gs://ks-layer1/BVBG028/OptnOnEqtsInf/*.parquet'
)
loader.load('raw_cotahistdata', 'gs://ks-layer1/COTAHIST/*.parquet')
loader.load('raw_stockindexinfo', 'gs://ks-layer1/B3StockIndexInfo/*.parquet')
