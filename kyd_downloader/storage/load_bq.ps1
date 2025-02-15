
# bq --location=US mk -d `
#     --default_table_expiration 3600 `
#     --description "B3 dataset to layer1 data operations" `
#     "kyd-storage-001:layer1_b3"

# bq update `
#     --default_table_expiration 0 `
#     "kyd-storage-001:layer1_b3"

# bq load `
#     --source_format=PARQUET `
#     --replace `
#     "kyd-storage-001:layer1_b3.marketdata" `
#     "gs://ks-layer1/BVBG086/*.parquet"

# bq rm -f -t "kyd-storage-001:layer1_b3.equities" 

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_b3.cotahist" `
    "gs://ks-layer1/COTAHIST/*.parquet"

# bq rm -f -t "kyd-storage-001:layer1_b3.marketdata"

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_b3.futures" `
    "gs://ks-layer1/BVBG028/FutrCtrctsInf/2021-*.parquet"

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_b3.equities" `
    "gs://ks-layer1/BVBG028/EqtyInf/2021-*.parquet"

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage:layer1_b3.raw_marketdata" `
    "gs://ks-layer1/BVBG086/*.parquet"

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_b3.indexes" `
    "gs://ks-layer1/BVBG087/IndxInf/2021-*.parquet"

bq extract `
    --destination_format PARQUET `
    --compression GZIP `
    "kyd-storage-001:layer1_b3.futures" `
    "gs://ks-layer1/futures.parquet"

bq extract `
    --destination_format PARQUET `
    --compression GZIP `
    "kyd-storage-001:layer1_b3.indexes" `
    "gs://ks-layer1/indexes.parquet"

bq extract `
    --destination_format PARQUET `
    --compression GZIP `
    "kyd-storage-001:layer1_b3.equities" `
    "gs://ks-layer1/equities.parquet"

bq extract `
    --destination_format PARQUET `
    --compression GZIP `
    "kyd-storage-001:layer1_b3.marketdata" `
    "gs://ks-layer1/marketdata.parquet"


bq mk --table `
    --schema "tipo_registro:STRING,data_referencia:TIMESTAMP,cod_bdi:STRING,cod_negociacao:STRING,tipo_mercado:STRING,nome_empresa:STRING,especificacao:STRING,num_dias_mercado_termo:STRING,cod_moeda:STRING,preco_abertura:FLOAT,preco_max:FLOAT,preco_min:FLOAT,preco_med:FLOAT,preco_ult:FLOAT,preco_melhor_oferta_compra:FLOAT,preco_melhor_oferta_venda:FLOAT,qtd_negocios:FLOAT,qtd_titulos_negociados:FLOAT,volume_titulos_negociados:FLOAT,preco_exercicio:FLOAT,indicador_correcao_preco_exercicio:STRING,data_vencimento:TIMESTAMP,fator_cot:FLOAT,preco_exercicio_pontos:FLOAT,cod_isin:STRING,num_dist:STRING" `
    --time_partitioning_field data_referencia `
    --time_partitioning_type DAY `
    "kyd-storage-001:layer1_b3.cotahist"


bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_b3.indexes" `
    "gs://ks-layer1/BVBG087/IndxInf/*.parquet"

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_b3.iopv" `
    "gs://ks-layer1/BVBG087/IOPVInf/*.parquet"


# Exportando arquivos para o curso de banco de dados

# -----------------------------------------------------------------------------
# Criando tabelas
# -----------------------------------------------------------------------------
bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_anbima.titpub" `
    "gs://ks-layer1/AnbimaTitpub/*.parquet"

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_anbima.vnatitpub" `
    "gs://ks-layer1/AnbimaVnaTitpub/*.parquet"

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_b3.futures" `
    "gs://ks-layer1/BVBG028/FutrCtrctsInf/*.parquet"

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_b3.equities" `
    "gs://ks-layer1/BVBG028/EqtyInf/*.parquet"

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_b3.marketdata" `
    "gs://ks-layer1/BVBG086/*.parquet"

bq load `
    --source_format=PARQUET `
    --replace `
    "kyd-storage-001:layer1_b3.indexes" `
    "gs://ks-layer1/BVBG087/IndxInf/*.parquet"

# -----------------------------------------------------------------------------
# Exportando arquivos
# -----------------------------------------------------------------------------

bq extract `
    --destination_format PARQUET `
    --compression GZIP `
    "kyd-storage-001:layer1_b3.futures" `
    "gs://ks-layer1/futures.parquet"

bq extract `
    --destination_format PARQUET `
    --compression GZIP `
    "kyd-storage-001:layer1_b3.indexes" `
    "gs://ks-layer1/indexes.parquet"

bq extract `
    --destination_format PARQUET `
    --compression GZIP `
    "kyd-storage-001:layer1_b3.equities" `
    "gs://ks-layer1/equities.parquet"

bq extract `
    --destination_format PARQUET `
    --compression GZIP `
    "kyd-storage-001:layer1_b3.marketdata" `
    "gs://ks-layer1/marketdata.parquet"

bq extract `
    --destination_format PARQUET `
    --compression GZIP `
    "kyd-storage-001:layer1_anbima.titpub" `
    "gs://ks-layer1/titpub.parquet"

#  ----------------------------------------------------------------------------

bq mk --external_table_definition=@PARQUET=gs://ks-layer1/AnbimaTitpub/*.parquet kyd-storage:layer1.raw-anbima-titpub
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/AnbimaDebentures/*.parquet kyd-storage:layer1.raw-anbima-debentures
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/AnbimaVnaTitpub/*.parquet kyd-storage:layer1.raw-anbima-vna-titpub
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/CDI/*.parquet kyd-storage:layer1.raw-b3-cdi
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG087/IndxInf/*.parquet kyd-storage:layer1.raw-b3-BVBG087-index-info
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG087/IOPVInf/*.parquet kyd-storage:layer1.raw-b3-BVBG087-iopv-info
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG087/BDRInf/*.parquet kyd-storage:layer1.raw-b3-BVBG087-bdr-info
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG086/*.parquet kyd-storage:layer1.raw-b3-BVBG086
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG028/EqtyInf/*.parquet kyd-storage:layer1.raw-b3-BVBG028-equity-info
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG028/FutrCtrctsInf/*.parquet kyd-storage:layer1.raw-b3-BVBG028-future-contracts-info
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG028/OptnOnEqtsInf/*.parquet kyd-storage:layer1.raw-b3-BVBG028-options-on-equity-info
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/COTAHIST/*.parquet kyd-storage:layer1.raw-b3-COTAHIST
bq mk --external_table_definition=@PARQUET=gs://ks-layer1/B3StockIndexInfo/*.parquet kyd-storage:layer1.raw-b3-stock-index-info


bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/AnbimaTitpub/*.parquet" > mydef.json
bq rm kyd-storage:layer1.raw-anbima-titpub
bq mk --table `
    --schema "symbol:string,refdate:string,cod_selic:integer,issue_date:string,maturity_date:string,bid_yield:float,ask_yield:float,ref_yield:float,price:float" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-anbima-titpub" `

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/COTAHIST/*.parquet" > mydef.json
bq rm kyd-storage:layer1.raw-b3-COTAHIST
bq mk --table `
    --schema "tipo_registro:STRING,data_referencia:STRING,cod_bdi:STRING,cod_negociacao:STRING,tipo_mercado:STRING,nome_empresa:STRING,especificacao:STRING,num_dias_mercado_termo:STRING,cod_moeda:STRING,preco_abertura:FLOAT,preco_max:FLOAT,preco_min:FLOAT,preco_med:FLOAT,preco_ult:FLOAT,preco_melhor_oferta_compra:FLOAT,preco_melhor_oferta_venda:FLOAT,qtd_negocios:FLOAT,qtd_titulos_negociados:FLOAT,volume_titulos_negociados:FLOAT,preco_exercicio:FLOAT,indicador_correcao_preco_exercicio:STRING,data_vencimento:STRING,fator_cot:FLOAT,preco_exercicio_pontos:FLOAT,cod_isin:STRING,num_dist:STRING" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-b3-COTAHIST"
