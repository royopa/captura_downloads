
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/AnbimaTitpub/*.parquet kyd-storage:layer1.raw-anbima-titpub
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/AnbimaDebentures/*.parquet kyd-storage:layer1.raw-anbima-debentures
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/AnbimaVnaTitpub/*.parquet kyd-storage:layer1.raw-anbima-vna-titpub
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/CDI/*.parquet kyd-storage:layer1.raw-b3-cdi
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG087/IndxInf/*.parquet kyd-storage:layer1.raw-b3-BVBG087-index-info
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG087/IOPVInf/*.parquet kyd-storage:layer1.raw-b3-BVBG087-iopv-info
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG087/BDRInf/*.parquet kyd-storage:layer1.raw-b3-BVBG087-bdr-info
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG086/*.parquet kyd-storage:layer1.raw-b3-BVBG086
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG028/EqtyInf/*.parquet kyd-storage:layer1.raw-b3-BVBG028-equity-info
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG028/FutrCtrctsInf/*.parquet kyd-storage:layer1.raw-b3-BVBG028-future-contracts-info
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/BVBG028/OptnOnEqtsInf/*.parquet kyd-storage:layer1.raw-b3-BVBG028-options-on-equity-info
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/COTAHIST/*.parquet kyd-storage:layer1.raw-b3-COTAHIST
# bq mk --external_table_definition=@PARQUET=gs://ks-layer1/B3StockIndexInfo/*.parquet kyd-storage:layer1.raw-b3-stock-index-info

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/AnbimaDebentures/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-anbima-debentures
bq mk --table `
    --schema "symbol:STRING,name:STRING,maturity_date:STRING,underlying:STRING,bid_yield:FLOAT,ask_yield:FLOAT,ref_yield:FLOAT,price:FLOAT,perc_price_par:FLOAT,duration:FLOAT,perc_reune:FLOAT,ref_ntnb:STRING,refdate:STRING" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-anbima-debentures" `

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/AnbimaVnaTitpub/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-anbima-vna-titpub
bq mk --table `
    --schema "refdate:STRING,value:FLOAT,rate:FLOAT,rate_start_date:STRING,proj:BOOLEAN,index_ref:STRING,instrument_ref:STRING" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-anbima-vna-titpub" `

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/AnbimaTitpub/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-anbima-titpub
bq mk --table `
    --schema "symbol:string,refdate:string,cod_selic:integer,issue_date:string,maturity_date:string,bid_yield:float,ask_yield:float,ref_yield:float,price:float" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-anbima-titpub" `

## COTAHIST

bq mkdef --autodetect --source_format=PARQUET "gs://ks-layer1/COTAHIST/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-b3-COTAHIST
bq mk --table `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-b3-COTAHIST"

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/COTAHIST/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-b3-COTAHIST
bq mk --table `
    --schema "tipo_registro:STRING,data_referencia:STRING,cod_bdi:STRING,cod_negociacao:STRING,tipo_mercado:STRING,nome_empresa:STRING,especificacao:STRING,num_dias_mercado_termo:STRING,cod_moeda:STRING,preco_abertura:FLOAT,preco_max:FLOAT,preco_min:FLOAT,preco_med:FLOAT,preco_ult:FLOAT,preco_melhor_oferta_compra:FLOAT,preco_melhor_oferta_venda:FLOAT,qtd_negocios:FLOAT,qtd_titulos_negociados:FLOAT,volume_titulos_negociados:FLOAT,preco_exercicio:FLOAT,indicador_correcao_preco_exercicio:STRING,data_vencimento:STRING,fator_cot:FLOAT,preco_exercicio_pontos:FLOAT,cod_isin:STRING,num_dist:STRING" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-b3-COTAHIST"

# BVBG028-EquityInfo
# BVBG028/EqtyInf/*.parquet kyd-storage:layer1.raw-b3-BVBG028-equity-info

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/BVBG028/EqtyInf/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-b3-BVBG028-equity-info
bq mk --table `
    --schema "creation_date:STRING,trade_date:STRING,security_id:STRING,security_proprietary:STRING,instrument_asset:STRING,instrument_asset_description:STRING,instrument_market:STRING,instrument_segment:STRING,instrument_description:STRING,security_category:STRING,isin:STRING,distribution_id:STRING,cfi_code:STRING,specification_code:STRING,corporation_name:STRING,symbol:STRING,payment_type:STRING,allocation_lot_size:STRING,price_factor:STRING,trading_start_date:STRING,trading_end_date:STRING,corporate_action_start_date:STRING,ex_distribution_number:STRING,custody_treatment_type:STRING,trading_currency:STRING,market_capitalisation:STRING,last_price:STRING,first_price:STRING,days_to_settlement:STRING,right_issue_price:STRING,instrument_type:STRING,governance_indicator:STRING" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-b3-BVBG028-equity-info" `

# BVBG028-FutureContractsInfo
# BVBG028/FutrCtrctsInf/*.parquet kyd-storage:layer1.raw-b3-BVBG028-future-contracts-info

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/BVBG028/FutrCtrctsInf/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-b3-BVBG028-future-contracts-info
bq mk --table `
    --schema "creation_date:STRING,trade_date:STRING,security_id:STRING,security_proprietary:STRING,instrument_asset:STRING,instrument_asset_description:STRING,instrument_market:STRING,instrument_segment:STRING,instrument_description:STRING,security_category:STRING,expiration_date:STRING,symbol:STRING,expiration_code:STRING,trading_start_date:STRING,trading_end_date:STRING,value_type_code:STRING,isin:STRING,cfi_code:STRING,delivery_type:STRING,payment_type:STRING,contract_multiplier:STRING,asset_settlement_indicator:STRING,allocation_lot_size:STRING,trading_currency:STRING,underlying_security_id:STRING,underlying_security_proprietary:STRING,withdrawal_days:STRING,working_days:STRING,calendar_days:STRING,instrument_type:STRING" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-b3-BVBG028-future-contracts-info" `

# BVBG028-OptionOnEquitiesInfo
# BVBG028/OptnOnEqtsInf/*.parquet kyd-storage:layer1.BVBG028-options-on-equity-info

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/BVBG028/OptnOnEqtsInf/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-b3-BVBG028-options-on-equity-info
bq mk --table `
    --schema "creation_date:STRING,trade_date:STRING,security_id:STRING,security_proprietary:STRING,instrument_asset:STRING,instrument_asset_description:STRING,instrument_market:STRING,instrument_segment:STRING,instrument_description:STRING,security_category:STRING,isin:STRING,symbol:STRING,exercise_price:STRING,option_style:STRING,expiration_date:STRING,option_type:STRING,underlying_security_id:STRING,underlying_security_proprietary:STRING,protection_flag:STRING,premium_upfront_indicator:STRING,trading_start_date:STRING,trading_end_date:STRING,payment_type:STRING,allocation_lot_size:STRING,price_factor:STRING,trading_currency:STRING,days_to_settlement:STRING,delivery_type:STRING,automatic_exercise_indicator:STRING,instrument_type:STRING" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-b3-BVBG028-options-on-equity-info" `

# BVBG086-Marketdata
# BVBG086/*.parquet kyd-storage:layer1.raw-b3-BVBG086

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/BVBG086/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-b3-BVBG086
bq mk --table `
    --schema "creation_date:STRING,trade_date:STRING,symbol:STRING,security_id:STRING,security_proprietary:STRING,security_market:STRING,open_interest:STRING,trade_quantity:STRING,volume:STRING,traded_contracts:STRING,best_ask_price:STRING,best_bid_price:STRING,first_price:STRING,min_price:STRING,max_price:STRING,average_price:STRING,last_price:STRING,regular_transactions_quantity:STRING,regular_traded_contracts:STRING,regular_volume:STRING,oscillation_percentage:STRING,adjusted_quote:STRING,adjusted_tax:STRING,previous_adjusted_quote:STRING,previous_adjusted_tax:STRING,variation_points:STRING,adjusted_value_contract:STRING,nonregular_transactions_quantity:STRING,nonregular_traded_contracts:STRING,nonregular_volume:STRING" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-b3-BVBG086" `

# BVBG087-IndexInfo
# BVBG087/IndxInf/*.parquet kyd-storage:layer1.raw-b3-BVBG087-index-info

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/BVBG087/IndxInf/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-b3-BVBG087-index-info
bq mk --table `
    --schema "trade_date:STRING,index_type:STRING,ticker_symbol:STRING,security_id:STRING,security_proprietary:STRING,security_market:STRING,asset_desc:STRING,settlement_price:STRING,open_price:STRING,min_price:STRING,max_price:STRING,average_price:STRING,close_price:STRING,last_price:STRING,oscillation_val:STRING,rising_shares_number:STRING,falling_shares_number:STRING,stable_shares_number:STRING" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-b3-BVBG087-index-info" `

# B3StockIndexInfo
# B3StockIndexInfo/*.parquet kyd-storage:layer1.raw-b3-stock-index-info

bq mkdef --noautodetect --source_format=PARQUET "gs://ks-layer1/B3StockIndexInfo/*.parquet" > mydef.json
bq rm -f kyd-storage:layer1.raw-b3-stock-index-info
bq mk --table `
    --schema "corporation_name:STRING,specification_code:STRING,symbol:STRING,indexes:STRING,refdate:STRING,duration_start_month:INTEGER,duration_end_month:INTEGER,duration_year:INTEGER" `
    --external_table_definition=mydef.json `
    "kyd-storage:layer1.raw-b3-stock-index-info" `
