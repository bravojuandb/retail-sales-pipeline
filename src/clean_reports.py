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

def load_config(path: Path) -> dict:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg


def read_reports(file: Path, *, delimiter= str, decimal= str, thousands= str) -> pd.DataFrame:
    return pd.read_csv(
        file, 
        delimiter=delimiter,
        decimal=decimal,
        thousands=thousands,
        dtype=str
        )

def main():
    cfg = load_config(Path("configs/config.dev.yaml"))
    raw_path = Path(cfg["inputs"]["raw_path"])
    fmt = cfg["format"]

    df = read_reports(
        raw_path,
        delimiter=fmt["delimiter"],
        decimal=fmt["decimal"],
        thousands=fmt["thousands"],
    )
    print(df.head())
    print(f"Rows read: {len(df)}")

if __name__ == "__main__":
    main()