import logging

from kyd.loader import ParquetDataLoader

logging.basicConfig(level=logging.INFO)

loader = ParquetDataLoader("kyd-storage", "layer1_anbima")

loader.load("raw_titpubdata", "gs://ks-layer1/AnbimaTitpub/*.parquet")
loader.load("raw_vnatitpubdata", "gs://ks-layer1/AnbimaVnaTitpub/*.parquet")
loader.load("raw_debenturesdata", "gs://ks-layer1/AnbimaDebentures/*.parquet")
