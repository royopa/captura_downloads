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
    file_name = f"{data_arquivo}_anbima_mercado_secundario_debentures.txt"
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

    file_name_out = file_name.replace(".txt", "_utf8.txt")
    layout_path = os.path.join(folder_path, file_name_out)

    with open(layout_path, "w", encoding="utf-8") as layout_file:
        with open(file_path, "r", encoding="latin1") as file:
            for line in file:
                layout_file.write(line)

    print(f"Success. Generated file {file_name_out} in folder {folder_path}")

    return layout_path


def transform(file_path):
    data_atual = datetime.now()
    cal = bizdays.Calendar.load("ANBIMA")
    data_referencia = cal.offset(data_atual, -1)

    df = pd.read_csv(
        file_path,
        sep="@",
        encoding="utf-8",
        decimal=",",
        thousands=".",
        skiprows=2,
    )

    df.insert(0, "DT_REF", data_referencia)

    a_renomear = {
        "Código": "CO_DEBENTURE",
        "Nome": "NO_EMISSOR",
        "Repac./  Venc.": "DT_VENCTO",
        "Índice/ Correção": "DE_INDICE_CORRECAO",
        "Taxa de Compra": "VR_TAXA_COMPRA",
        "Taxa de Venda": "VR_TAXA_VENDA",
        "Taxa Indicativa": "VR_TAXA_INDICATIVA",
        "Desvio Padrão": "VR_DESVIO_PADRAO",
        "Intervalo Indicativo Minimo": "VR_INTERVALO_INDICATIVO_MIN",
        "Intervalo Indicativo Máximo": "VR_INTERVALO_INDICATIVO_MAX",
        "PU": "VR_PU",
        "% PU Par / % VNE": "PC_PU_PAR_VNE",
        "Duration": "NU_DURATION",
        "% Reune": "PC_REUNE",
        "Referência NTN-B": "DT_REF_NTN_B",
    }

    df = df.rename(columns=a_renomear)

    df["DT_REF"] = pd.to_datetime(df["DT_REF"])
    df["DT_VENCTO"] = pd.to_datetime(df["DT_VENCTO"], format="%d/%m/%Y")
    df["DT_REF_NTN_B"] = pd.to_datetime(df["DT_REF_NTN_B"], format="%d/%m/%Y")

    df = convert_columns_dtypes(df)

    file_name = Path(file_path).name
    schema = file_name.split("_")[1]
    table_name = file_name.split("_anbima_")[1].split(".")[0]

    print("Creating table no database...", end=" ")
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
    file_path_out = file_path.replace(".txt", ".csv")
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
