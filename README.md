# Retail Sales Pipeline

A minimal data pipeline that  
- **ingests** raw European-style retail reports (CSV) from local storage or the cloud (s3),  
- **parses** dates and monetary values into international formats, and  
- **persists** query-ready Parquet files in a clean/ layer locally or in S3

[Sample Data](data/raw/sample_report.csv) structure:

```
| Column       | Description                                 |
|--------------|---------------------------------------------|
| delivery_note| number of delivery note (40000 +)           |
| date         | date of operation (the whole year)          |
| customer_id  | customer Id (5000 +)                        |
| amount       | net amount before taxes                     |
| vat          | 21%                                         |
| total        | total after taxes                           |
| invoice_num  | issued at the end of the month, grouping ops|
```


## Quickstart

Clone this repo and:
```bash
# Create and activate a virtual environment
# macOS/Linux
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies:
pip install -r requirements.txt

# Run script
python src/clean_reports.py
```

## Run in GitHub Codespaces (no local setup)

1. On GitHub, click **Code ▸ Codespaces ▸ Create codespace on main**.

2. Wait for the container to start (first boot may take a couple of minutes).

3. In the Codespaces terminal, run:
```bash
   pip install -r requirements.txt
   python src/clean_reports.py
   ls -lh data/clean/
```
4. You should see a preview of the cleaned DataFrame in the console and a new Parquet file under `data/clean/`.

Tip: Codespaces already has Python preinstalled. You can still use a venv if you prefer:

```bash
> python -m venv .venv && source .venv/bin/activate
> pip install -r requirements.txt
```

## Output: 

- data/clean/sample_report_clean.parquet.  

- Check this quick [*Analysis*](notebooks/analysis.ipynb) of the original dataset:

  - Summary
  - Top customers
  - Monthly revenue
  - Top Refunds

## Project Layout

```
RETAIL-SALES-PIPELINE/
├── configs/             # YAML configs for environment settings
├── data/
│   ├── raw/             # Original CSVs
│   └── clean/           # Processed Parquet files
├── src/
│   └── clean_reports.py # Data cleaning & transformation
├── tests/               # Unit tests for pipeline functions
├── notebooks/           # Jupyter notebook for exploration and analysis
├── gallery/             # Screenshots
├── requirements.txt     # Python dependencies
├── README.md
├── .env.example         # Example environment variables 
├── .gitignore
└── LICENSE
```

## Configuration

Settings are defined in `configs/config.dev.yaml`:

```yaml
inputs:
  raw_path: "data/raw/sample_report.csv"
outputs:
  clean_dir: "data/clean/"
format:
  delimiter: ";"
  decimal: ","
  thousands: "."
  date_format: "%d/%m/%Y"
schema:
  required_columns: [date, amount, vat, total, delivery_note, customer_id, invoice_num]

storage: 
  use_s3: false
  s3_bucket: "juanbravo-annual-delivery-notes"
  s3_prefix_raw: "raw_data/"
  s3_prefix_clean: "clean_data/"
```

## Project Roadmap

### Core
- ✅ Raw: private S3 bucket with `raw/` layout (manual upload).
- ✅ Clean: `clean_reports.py` reads raw (EU formats), parses dates, fixes dtypes, writes Parquet to `clean/`.
- ✅ README: problem, architecture (raw → clean), how to run locally with a sample file.
- ✅ Logging: INFO-level start/end + rows in/out.
- ✅ Tests: basic pytest checks (date parsing, decimals/thousands, schema).
- ✅ Analysis: Jupyter notebook with top customers, monthly revenue trend, refunds (negative totals).
- ✅ AWS s3: read/write the full report in the cloud.


### Plus
- CLI (argparse): commands for `read` (schema + row count) and `clean` (write Parquet).
- CI (GitHub Actions): install deps, run tests, lint (`ruff`).
- Database integration: load cleaned table into Postgres, with one SQL query saved as `.sql`.
