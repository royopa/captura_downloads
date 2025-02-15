import os
import tempfile

import google.cloud.storage as storage
import pandas as pd
from google.cloud.storage.blob import Blob

from kyd.parsers import unzip_file_to
from kyd.parsers.anbima import AnbimaVnaTPF

storage_client = storage.Client()
filename = 'gs://ks-rawdata-anbima-vnatitpub/2019-01-09.html'
blob = Blob.from_string(filename)

handle, foutput = tempfile.mkstemp()
tempf = os.fdopen(handle, 'w+b')
storage_client.download_blob_to_file(blob, tempf)
tempf.flush()
tempf.seek(0)

x = AnbimaVnaTPF(foutput)
print(x.data)
df = pd.DataFrame(x.data)
print(df.dtypes)
print(df.shape)
print(df.head())
# df['ask_yield'] = pd.to_numeric(df['ask_yield'], errors='coerce')
# df['bid_yield'] = pd.to_numeric(df['bid_yield'], errors='coerce')
# df['ref_yield'] = pd.to_numeric(df['ref_yield'], errors='coerce')
# x = ~df['ref_yield'].isna()
# if any(x):
#     df = df[x].copy()
# else:
#     raise Exception('Invalid data')
# df['cod_selic'] = df['cod_selic'].astype(int)


# print(pd.read_parquet("2023-03-30.parquet"))
