
# kyd downloader

Google Cloud Functions to download financial data into Google Cloud Storage (GCS) buckets.


### Arquivos para download no Tesouro

https://www.tesourodireto.com.br/lumis/api/rest/documentos/lumgetdata/list.json?lumReturnFields=documentFile,title,description,tags&lumMaxRows=999

Estoque da dívida pública

<https://www.tesourotransparente.gov.br/ckan/dataset/0998f610-bc25-4ce3-b32c-a873447500c2/resource/b6280ed3-ef7e-4569-954a-bded97c2c8a1/download/EstoqueDPF.csv>

Seria legal avaliar a evolução do estoque da dívida.

Aqui é possível obter essa informação histórica.

<https://dadosabertos.bcb.gov.br/api/rest/dataset/cronograma-de-vencimentos-de-titulos>

Outra referência:

<https://basedosdados.org/dataset/estoque-da-divida-publica-federal>

### Arquivos para download na B3

<http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/boletim-diario/arquivos-para-download/>

Solicitar o token.

GET <https://arquivos.b3.com.br/api/download/requestname?fileName=LendingOpenPosition&date=2020-07-13&recaptchaToken=>

URL: `https://arquivos.b3.com.br/api/download/requestname?fileName=[filename]&date=[date %Y-%m-%d]&recaptchaToken=`

Response: 

```
{"redirectUrl":"~/download?token=NHEzUlk5LzRDSm5EQnZLblJsQjIxdz09N3dqSHU3TFNESVBibWdyNmdET2wxVlNBbHF0VTBUU2x2dnVmbVlVelZYQ0N0SXdjR0Rsdk1rdmQvd29hT3lYTkNSenRzU3RHWjJIVWJIK0ZUdEd1dWQ2dnAxVDZxSlM2T1cwL0JJUDA5NE5ZMHF5UFcwWjI5UU1xWTcwUUM0ZDdjZ2NrYVhuUXdNSnloQnJLZC85SEZiSSt1bmg4RjczaHVTZi9ORkIyamJYQi8rTUQyTW5BeS9mSFI3UlI0QXVFa21DMU5vMFdrbE5rZXg5TXd2ZC9BbjNQNGhacUxnVi94NTh2T2VZMk5rcS9TYUErL2s1ZElJTW5NSFpVck12ZHUxNjlkY2VSK0hUNm9YR2Y1K0NKVmc9PQ","token":"NHEzUlk5LzRDSm5EQnZLblJsQjIxdz09N3dqSHU3TFNESVBibWdyNmdET2wxVlNBbHF0VTBUU2x2dnVmbVlVelZYQ0N0SXdjR0Rsdk1rdmQvd29hT3lYTkNSenRzU3RHWjJIVWJIK0ZUdEd1dWQ2dnAxVDZxSlM2T1cwL0JJUDA5NE5ZMHF5UFcwWjI5UU1xWTcwUUM0ZDdjZ2NrYVhuUXdNSnloQnJLZC85SEZiSSt1bmg4RjczaHVTZi9ORkIyamJYQi8rTUQyTW5BeS9mSFI3UlI0QXVFa21DMU5vMFdrbE5rZXg5TXd2ZC9BbjNQNGhacUxnVi94NTh2T2VZMk5rcS9TYUErL2s1ZElJTW5NSFpVck12ZHUxNjlkY2VSK0hUNm9YR2Y1K0NKVmc9PQ","file":{"name":"LendingOpenPositionFile_20200713_1","extension":".csv"}}
```

Usar o token na URL para obter o arquivo

GET <https://arquivos.b3.com.br/api/download/?token=[TOKEN]>

Lista de `filename`:

- OTCInstrumentsConsolidated
- InstrumentsConsolidated
- MarginScenarioLiquidAssets
- EconomicIndicatorPrice
- OTCTradeInformationConsolidated
- TradeInformationConsolidated
- TradeInformationConsolidatedAfterHours
- DerivativesOpenPosition
- LoanBalance