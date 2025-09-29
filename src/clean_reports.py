"""
This includes the following transforming functions:
 1. read_reports -> DONE
 2. clean_reports -> DONE
 3. validate_schema -> DONE
 4. write_parquet -> DONE
 5. main -> DONE

"""
import os
import yaml
import pandas as pd
from pathlib import Path
from typing import Iterable, Optional, Union
import logging
import time
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_config(path: Path) -> dict:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg

def storage_options_from_env() -> dict:
    opts = {}
    if os.getenv("AWS_PROFILE"):
        opts["profile"] = os.getenv("AWS_PROFILE")
    if os.getenv("AWS_DEFAULT_REGION"):
        opts["client_kwargs"] = {"region_name": os.getenv("AWS_DEFAULT_REGION")}
    return opts

def build_paths(cfg: dict) -> tuple[Union[str, Path], Union[str, Path], dict]:

    storage = cfg.get("storage", {})
    use_s3 = storage.get("use_s3", False)

    raw_path = Path(cfg["inputs"]["raw_path"])
    base_name = f"{raw_path.stem}_clean.parquet"

    if use_s3:
        bucket = storage["s3_bucket"]
        raw_prefix = storage["s3_prefix_raw"].lstrip("/")
        clean_prefix = storage["s3_prefix_clean"].lstrip("/")
        src = f"s3://{bucket}/{raw_prefix}{raw_path.name}"
        dst = f"s3://{bucket}/{clean_prefix}{base_name}"
        storage_options = storage_options_from_env()
    else:
        src = raw_path
        clean_dir = Path(cfg["outputs"]["clean_dir"])
        clean_dir.mkdir(parents=True, exist_ok=True)
        dst = clean_dir / base_name
        storage_options = {}

    return src, dst, storage_options



def read_reports(
    file: Union[str, Path],
    *,
    delimiter: str,
    decimal: str,
    thousands: str,
    storage_options: Optional[dict] = None,
) -> pd.DataFrame:
    return pd.read_csv(
        file,
        delimiter=delimiter,
        decimal=decimal,
        thousands=thousands,
        dtype=str,
        storage_options=storage_options,  # <- enables s3://
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
    out_path: Union[str, Path],
    *,
    storage_options: Optional[dict] = None,
) -> Union[str, Path]:
    df.to_parquet(
        out_path,
        index=False,
        engine="pyarrow",
        compression="snappy",
        storage_options=storage_options,
    )
    return out_path

def main():
    load_dotenv()
    start_time = time.perf_counter()
    logger.info("Pipeline started")

    cfg = load_config(Path("configs/config.dev.yaml"))
    fmt = cfg["format"]

    src, dst, storage_options = build_paths(cfg)

    logger.info("Reading from: %s", src)
    df = read_reports(
        src,
        delimiter=fmt["delimiter"],
        decimal=fmt["decimal"],
        thousands=fmt["thousands"],
        storage_options=storage_options,
    )
    logger.info("Rows in: %d", len(df))

    required = cfg["schema"]["required_columns"]
    validate_schema(df, required)

    cleaned_df = clean_reports(df, date_fmt=fmt["date_format"])
    logger.info("Rows out: %d", len(cleaned_df))

    logger.info("Writing parquet to: %s", dst)
    out_path = write_parquet(cleaned_df, dst, storage_options=storage_options)

    check_df = pd.read_parquet(out_path, engine="pyarrow", storage_options=storage_options)
    logger.info("Parquet read-back OK | rows=%d cols=%d", len(check_df), check_df.shape[1])

    elapsed = time.perf_counter() - start_time
    logger.info("Pipeline finished in %.2f seconds", elapsed)

if __name__ == "__main__":
    main()