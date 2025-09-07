"""
This includes the following transforming functions:
 1. read_reports -> DONE
 2. clean_reports -> DONE
 3. validate_schema -> DONE
 4. write_parquet -> DONE
 5. main -> DONE

"""
import yaml
import pandas as pd
from pathlib import Path
from typing import Iterable
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


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

def clean_reports(df: pd.DataFrame, *, date_fmt: str) -> pd.DataFrame:
    out = df.copy()

    # 1. Date
    out["date"] = pd.to_datetime(
        out["date"],
        format=date_fmt,
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
    base_name: str
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / base_name
    df.to_parquet(out_path,
                  index=False,
                  engine="pyarrow",
                  compression="snappy")
    return out_path


def main():
    start_time = time.perf_counter()
    logger.info("Pipeline started")

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
    logger.info("Rows in: %d", len(df))

    required = cfg["schema"]["required_columns"]
    validate_schema(df, required)

    cleaned_df = clean_reports(df, date_fmt=fmt["date_format"])
    logger.info("Rows out: %d", len(cleaned_df))

    base_name = f"{raw_path.stem}_clean.parquet"
    out_path = write_parquet(cleaned_df, clean_path, base_name=base_name)
    check_df = pd.read_parquet(out_path, engine="pyarrow")

    logger.info("Wrote cleaned parquet | path=%s rows=%d", out_path, len(cleaned_df))
    logger.info("Parquet read-back OK | rows=%d cols=%d", len(check_df), check_df.shape[1])

    elapsed = time.perf_counter() - start_time
    logger.info("Pipeline finished in %.2f seconds", elapsed)
  

if __name__ == "__main__":
    main()