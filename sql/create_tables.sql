-- Tabela para os registros que começam com 1
CREATE SCHEMA IF NOT EXISTS anbima;
CREATE SCHEMA IF NOT EXISTS b3;
CREATE SCHEMA IF NOT EXISTS bacen;

DROP TABLE IF EXISTS dbo.IndicadoresLayout1;
CREATE TABLE dbo.IndicadoresLayout1 (
    Id INT,
    Titulo NVARCHAR(255),
    DataReferencia DATE,
    Indice NVARCHAR(255),
    NumeroIndice FLOAT,
    VariacaoDiaria FLOAT,
    VariacaoMensal FLOAT,
    VariacaoAnual FLOAT,
    Variacao12Meses FLOAT,
    Variacao24Meses FLOAT,
    DurationDu INT,
    PesoGeral FLOAT,
    CarteiraMercado FLOAT,
    NumeroOperacoes INT,
    QuantidadeNegociada FLOAT,
    ValorNegociado FLOAT,
    PMR FLOAT,
    Convexidade FLOAT,
    Yield FLOAT,
    RedemptionYield FLOAT
);

-- Tabela para os registros que começam com 2
DROP TABLE IF EXISTS dbo.IndicadoresLayout2;
CREATE TABLE dbo.IndicadoresLayout2 (
    Id INT,
    Titulo NVARCHAR(255),
    DataReferencia DATE,
    Indice NVARCHAR(255),
    Titulos NVARCHAR(255),
    DataVencimento DATE,
    CodigoSELIC NVARCHAR(255),
    CodigoISIN NVARCHAR(255),
    TaxaIndicativa FLOAT,
    PU FLOAT,
    PUJuros FLOAT,
    Quantidade FLOAT,
    QuantidadeTeorica FLOAT,
    CarteiraMercado FLOAT,
    Peso FLOAT,
    PrazoDu INT,
    DurationDu INT,
    NumeroOperacoes INT,
    QuantidadeNegociada FLOAT,
    ValorNegociado FLOAT,
    PMR FLOAT,
    Convexidade FLOAT
);
