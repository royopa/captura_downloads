import os
from datetime import datetime
from pathlib import Path

import bizdays
import pandas as pd
from dotenv import load_dotenv

from .utils import (
    convert_columns_dtypes,
    get_engine,
    get_sqlalchemy_dtypes,
    load_with_bcp,
)

load_dotenv()


def main():
    data_atual = datetime.now()
    data_arquivo = data_atual.strftime("%Y%m%d")
    project_root_folder = Path(__file__).resolve().parents[3]
    file_name = f"{data_arquivo}_b3_instrumentos_listados.csv"
    file_path = os.path.join(project_root_folder, "downloads_bulk", file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    file_path_extracted = extract(file_path)
    if not os.path.exists(file_path_extracted):
        raise FileNotFoundError(f"File extracted not found: {file_path_extracted}")

    file_path_transformed = transform(file_path_extracted)
    if not os.path.exists(file_path_transformed):
        raise FileNotFoundError(f"File transformed not found: {file_path_transformed}")

    load(file_path_transformed)


def extract(file_path):
    folder_path = Path(file_path).parent
    file_name = Path(file_path).name
    print(f"Processing file {file_name} in folder {folder_path}")

    file_name_out = file_name.replace(".csv", "_utf8.csv")
    layout_path = os.path.join(folder_path, file_name_out)

    with open(layout_path, "w", encoding="utf-8") as layout_file:
        with open(file_path, "r", encoding="latin1") as file:
            for line in file:
                layout_file.write(line)

    print(f"Success. Generated file {file_name_out} in folder {folder_path}")

    return layout_path


def transform(file_path):
    data_atual = datetime.now()
    cal = bizdays.Calendar.load("B3")
    data_referencia = cal.offset(data_atual, -1)

    df = pd.read_csv(file_path, sep=";", encoding="utf-8", skiprows=1, low_memory=False)

    a_renomear = {
        "RptDt": "DT_REF",
        "TckrSymb": "NO_TICKER",
        "Asst": "NO_MERCADORIA",
        "AsstDesc": "DE_MERCADORIA",
        "SgmtNm": "NO_SEGMENTO",
        "MktNm": "NO_MERCADO",
        "SctyCtgyNm": "NO_CATEGORIA",
        "XprtnDt": "DT_VENCTO",
        "XprtnCd": "CO_EXPIRACAO",
        "TradgStartDt": "DT_INI_NEGOCIACAO",
        "TradgEndDt": "DT_FIM_NEGOCIACAO",
        "BaseCd": "CO_BASE_CONTAGEM_DIAS",
        "ConvsCritNm": "NO_CRITERIO_CONVERSAO",
        "MtrtyDtTrgtPt": "NU_PONTOS_VENCTO",
        "ReqrdConvsInd": "IC_CONVERSAO_REQUERIDA",
        "ISIN": "CO_ISIN",
        "CFICd": "CO_CFI",
        "DlvryNtceStartDt": "DT_INI_AVISO_ENTREGA",
        "DlvryNtceEndDt": "DT_FIM_AVISO_ENTREGA",
        "OptnTp": "NO_TIPO_OPCAO",
        "CtrctMltplr": "VR_MULTIPLO_CONTRATO",
        "AsstQtnQty": "VR_QTD_MERCADORIA",
        "AllcnRndLot": "NU_LOTE_ALOCACAO",
        "TradgCcy": "NO_MOEDA_NEGOCIACAO",
        "DlvryTpNm": "NO_TIPO_ENTREGA",
        "WdrwlDays": "NU_DIAS_SAQUE",
        "WrkgDays": "NU_DIAS_UTEIS",
        "ClnrDays": "NU_DIAS_CORRIDOS",
        "RlvrBasePricNm": "NO_PRECO_BASE_LIQUIDACAO",
        "OpngFutrPosDay": "NU_DIAS_POSICAO_FUTURA",
        "SdTpCd1": "NO_POSICAO_1",
        "UndrlygTckrSymb1": "CO_ATIVO_OBJETO_1",
        "SdTpCd2": "NU_POSICAO_2",
        "UndrlygTckrSymb2": "CO_ATIVO_OBJETO_2",
        "PureGoldWght": "VR_PESO_OURO",
        "ExrcPric": "VR_PRECO_EXERCICIO",
        "OptnStyle": "NO_ESTILO_OPCAO",
        "ValTpNm": "NO_TIPO_VALORIZACAO",
        "PrmUpfrntInd": "IC_PREMIO_ANTECIPADO",
        "OpngPosLmtDt": "DT_LIMITE_POSICAO",
        "DstrbtnId": "CO_DISTRIBUICAO",
        "PricFctr": "NU_FATOR_PRECO",
        "DaysToSttlm": "NU_DIAS_LIQUIDACAO",
        "SrsTpNm": "NO_TIPO_SERIE",
        "PrtcnFlg": "IC_PROTECAO",
        "AutomtcExrcInd": "IC_EXERCICIO_AUTOMATICO",
        "SpcfctnCd": "CO_ESPECIFICACAO_ACAO",
        "CrpnNm": "NO_INSTITUICAO",
        "CorpActnStartDt": "DT_INI_ACAO_CORPORATIVA",
        "CtdyTrtmntTpNm": "NO_TIPO_TRATAMENTO_CUSTODIA",
        "MktCptlstn": "VR_CAPITAL_SOCIAL",
        "CorpGovnLvlNm": "NO_NIVEL_GOVERNANCA",
    }

    df = df.rename(columns=a_renomear)

    for column_name in df.columns:
        if column_name.startswith("DT_"):
            df[column_name] = pd.to_datetime(
                df[column_name], format="%Y-%m-%d", errors="coerce"
            )
    df = convert_columns_dtypes(df)

    file_name = Path(file_path).name
    schema = file_name.split("_")[1]
    table_name = file_name.split("_b3_")[1].split(".")[0]
    table_name = table_name.replace(".csv", "")
    table_name = table_name.replace(".json", "")
    table_name = table_name.replace(".txt", "")
    table_name = table_name.replace(".csv", "")
    table_name = table_name.replace("_utf8", "")

    print(f"Creating table {schema}.{table_name} in database...", end=" ")
    df.head(0).to_sql(
        table_name,
        con=get_engine(),
        if_exists="replace",
        index=False,
        schema=schema,
        dtype=get_sqlalchemy_dtypes(df),
    )
    print("OK")

    print("Reformatting csv file to bcp import...", end=" ")
    file_path_out = file_path
    df.to_csv(
        file_path_out,
        sep=";",
        index=False,
        encoding="utf-8",
        lineterminator="\n",
    )
    print("OK")

    return file_path_out


def load(file_path):
    return load_with_bcp(file_path)


if __name__ == "__main__":
    main()
