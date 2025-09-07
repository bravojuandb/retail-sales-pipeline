import pandas as pd
import pytest
from clean_reports import clean_reports, validate_schema

def test_decimal_and_date_conversion():

    raw = pd.DataFrame({
        "date": ["31/12/2024", "01/01/2025"],
        "amount": ["1.234,56", "78,90"],
        "vat": ["123,45", "12,34"],
        "total": ["1.358,01", "91,24"],
        "delivery_note": ["40.273", "40.274"],
        "customer_id": ["5.115", "5.254"],
        "invoice_num": ["10.318", "9.849"],
    })

    cleaned = clean_reports(raw, date_fmt="%d/%m/%Y")

    # Dates converted to datetime64
    assert pd.api.types.is_datetime64_any_dtype(cleaned["date"])
    assert cleaned.loc[0, "date"].strftime("%Y-%m-%d") == "2024-12-31"

    # Numbers converted to float64
    assert cleaned["amount"].iloc[0] == 1234.56
    assert cleaned["total"].iloc[1] == 91.24

    # Integers parsed correctly
    assert cleaned["delivery_note"].iloc[0] == 40273
    assert cleaned["invoice_num"].iloc[1] == 9849


def test_validate_schema_raises_on_missing_required_columns():
    df = pd.DataFrame({
        "date": ["31/12/2024"],
        "amount": ["1,00"],
        # "vat" intentionally missing
        "total": ["1,21"],
        "delivery_note": ["40.273"],
        "customer_id": ["5.115"],
        "invoice_num": ["10.318"],
    })
    required = ["date","amount","vat","total","delivery_note","customer_id","invoice_num"]

    with pytest.raises(ValueError) as exc:
        validate_schema(df, required)
    assert "vat" in str(exc.value)