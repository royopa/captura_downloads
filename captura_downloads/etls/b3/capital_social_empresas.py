import json
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
    file_name = f"{data_arquivo}_b3_capital_social_empresas.json"
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

    file_name_out = file_name.replace(".json", "_utf8.json")
    layout_path = os.path.join(folder_path, file_name_out)

    with open(layout_path, "w", encoding="utf-8") as layout_file:
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                layout_file.write(line)

    print(f"Success. Generated file {file_name_out} in folder {folder_path}")

    return layout_path


def transform(file_path):
    data_atual = datetime.now()
    cal = bizdays.Calendar.load("B3")
    data_referencia = cal.offset(data_atual, -1)

    # Ler o conteúdo do arquivo JSON
    with open(file_path, "r", encoding="utf-8") as file:
        json_data = file.read()

    # Carregar o JSON em um dicionário Python
    data = json.loads(json_data)
    df = pd.DataFrame(data.get("results"))

    df.insert(0, "DT_REF", data_referencia)

    a_renomear = {
        "tradingName": "NO_TRADING_NAME",
        "issuingCompany": "NO_ISSUING_COMPANY",
        "companyName": "NO_COMPANY",
        "market": "NO_MARKET",
        "typeCapital": "NO_TYPE_CAPITAL",
        "value": "VR_CAPITAL",
        "approvedDate": "DT_APPROVED",
        "commonShares": "NU_COMMON_SHARES",
        "preferredShares": "NU_PREFERRED_SHARES",
        "totalQtyShares": "NU_TOTAL_QTY_SHARES",
    }

    df = df.rename(columns=a_renomear)

    df["DT_REF"] = pd.to_datetime(df["DT_REF"])
    df["DT_APPROVED"] = pd.to_datetime(df["DT_APPROVED"], format="%d/%m/%Y")

    for column_name in df.columns:
        if column_name.startswith("NU_") or column_name.startswith("VR_"):
            df[column_name] = df[column_name].str.replace(".", "")
            df[column_name] = df[column_name].str.replace(",", ".")
            df[column_name] = pd.to_numeric(df[column_name], errors="coerce")

    df = convert_columns_dtypes(df)

    file_name = Path(file_path).name
    schema = file_name.split("_")[1]
    table_name = file_name.split("_b3_")[1].split(".")[0]
    table_name = table_name.replace(".csv", "")
    table_name = table_name.replace(".json", "")
    table_name = table_name.replace(".txt", "")
    table_name = table_name.replace(".csv", "")
    table_name = table_name.replace("_utf8", "")

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
    file_path_out = file_path.replace(".json", ".csv")
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
