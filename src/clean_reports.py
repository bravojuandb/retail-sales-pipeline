"""
This includes the following transforming functions:
 1. read_reports -> DONE
 2. clean_reports
 3. write_parquet
 3. validate_schema

"""
import yaml
import pandas as pd
from pathlib import Path
from typing import Iterable

def load_config(path: Path) -> dict:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg


def read_reports(
    file: Path,
    *,
    delimiter: str,
    decimal: str,
    thousands: str
) -> pd.DataFrame:
    return pd.read_csv(
        file, 
        delimiter=delimiter,
        decimal=decimal,
        thousands=thousands,
        dtype=str
        )

def clean_reports(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # 1. Date
    out["date"] = pd.to_datetime(
        out["date"],
        format="%d/%m/%Y",
        errors="raise")

    # 2. Floats (money amounts)
    for col in ["amount", "vat", "total"]:
        out[col] = pd.to_numeric(
            out[col]
            .str.replace(".", "", regex=False)  
            .str.replace(",", ".", regex=False), 
            errors="raise"
        ).astype("float64")

    # 3. Integers (IDs, invoice numbers)
    for col in ["delivery_note", "customer_id", "invoice_num"]:
        out[col] = pd.to_numeric(
            out[col]
            .str.replace(".", "", regex=False)
            .str.replace(",", "", regex=False),
            errors="raise"
        ).astype("int64")

    return out

def validate_schema(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
def write_parquet(
    df: pd.DataFrame, 
    out_dir: Path, 
    base_name: str = "reports_clean.parquet"
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / base_name
    df.to_parquet(out_path, index=False)
    return out_path


def main():
    cfg = load_config(Path("configs/config.dev.yaml"))
    raw_path = Path(cfg["inputs"]["raw_path"])
    clean_path = Path(cfg["outputs"]["clean_dir"])
    fmt = cfg["format"]

    df = read_reports(
        raw_path,
        delimiter=fmt["delimiter"],
        decimal=fmt["decimal"],
        thousands=fmt["thousands"],
    )
    
    required = ["date","amount","vat","total","delivery_note","customer_id","invoice_num"]
    validate_schema(df, required)

    cleaned_df = clean_reports(df)
    out_path = write_parquet(cleaned_df, clean_path)
    print(cleaned_df.head(10))
    print(cleaned_df.dtypes)
    print(f"Rows cleaned: {len(cleaned_df)} → {out_path}")

if __name__ == "__main__":
    main()