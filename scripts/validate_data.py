from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd


VALID_ASSET_TYPES = {
    "Equity",
    "Bond",
    "ETF",
    "Mutual Fund",
    "Preferred Stock",
}

PRICE_SPIKE_THRESHOLD = 0.25  # 25%


@dataclass
class ValidationContext:
    previous_df: pd.DataFrame
    current_df: pd.DataFrame
    project_root: Path


def load_data(project_root: Path) -> ValidationContext:
    data_dir = project_root / "data"
    previous_df = pd.read_csv(data_dir / "previous_day_data.csv")
    current_df = pd.read_csv(data_dir / "current_day_data.csv")

    date_columns = ["issue_date", "maturity_date", "last_updated"]
    for df in (previous_df, current_df):
        for column in date_columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")

    return ValidationContext(
        previous_df=previous_df,
        current_df=current_df,
        project_root=project_root,
    )


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def build_flagged_records(mask: pd.Series, df: pd.DataFrame, issue_type: str, details: str) -> pd.DataFrame:
    flagged = df.loc[mask].copy()
    if flagged.empty:
        return pd.DataFrame()
    flagged["issue_type"] = issue_type
    flagged["issue_details"] = details
    return flagged


def check_missing_ticker(ctx: ValidationContext) -> pd.DataFrame:
    mask = ctx.current_df["ticker"].apply(normalize_text).eq("")
    return build_flagged_records(mask, ctx.current_df, "MISSING_TICKER", "Ticker is blank or null.")


def check_missing_required_fields(ctx: ValidationContext) -> pd.DataFrame:
    required_columns = ["cusip", "security_name", "asset_type", "sector", "price"]
    mask = pd.Series(False, index=ctx.current_df.index)
    for column in required_columns:
        if pd.api.types.is_numeric_dtype(ctx.current_df[column]):
            column_mask = ctx.current_df[column].isna()
        else:
            column_mask = ctx.current_df[column].apply(normalize_text).eq("")
        mask = mask | column_mask
    return build_flagged_records(mask, ctx.current_df, "MISSING_REQUIRED_FIELD", "One or more required fields are missing.")


def check_duplicate_records(ctx: ValidationContext) -> pd.DataFrame:
    mask = ctx.current_df.duplicated(subset=["ticker", "cusip"], keep=False)
    return build_flagged_records(mask, ctx.current_df, "DUPLICATE_RECORD", "Duplicate ticker + CUSIP combination found.")


def check_invalid_asset_type(ctx: ValidationContext) -> pd.DataFrame:
    mask = ~ctx.current_df["asset_type"].isin(VALID_ASSET_TYPES)
    return build_flagged_records(mask, ctx.current_df, "INVALID_ASSET_TYPE", "Asset type is not in the approved value list.")


def check_negative_price(ctx: ValidationContext) -> pd.DataFrame:
    mask = ctx.current_df["price"] < 0
    return build_flagged_records(mask, ctx.current_df, "NEGATIVE_PRICE", "Price is less than zero.")


def check_future_issue_date(ctx: ValidationContext) -> pd.DataFrame:
    today = pd.Timestamp.today().normalize()
    mask = ctx.current_df["issue_date"].notna() & (ctx.current_df["issue_date"] > today)
    return build_flagged_records(mask, ctx.current_df, "FUTURE_ISSUE_DATE", "Issue date is in the future.")


def check_date_logic(ctx: ValidationContext) -> pd.DataFrame:
    mask = (
        ctx.current_df["issue_date"].notna()
        & ctx.current_df["maturity_date"].notna()
        & (ctx.current_df["maturity_date"] < ctx.current_df["issue_date"])
    )
    return build_flagged_records(mask, ctx.current_df, "DATE_LOGIC_ERROR", "Maturity date is earlier than issue date.")


def check_missing_from_current_feed(ctx: ValidationContext) -> pd.DataFrame:
    previous_keys = ctx.previous_df[["ticker", "cusip"]].copy()
    current_keys = ctx.current_df[["ticker", "cusip"]].copy()
    previous_keys["key"] = previous_keys["ticker"].apply(normalize_text) + "|" + previous_keys["cusip"].apply(normalize_text)
    current_keys["key"] = current_keys["ticker"].apply(normalize_text) + "|" + current_keys["cusip"].apply(normalize_text)
    missing_keys = set(previous_keys["key"]) - set(current_keys["key"])
    previous_df = ctx.previous_df.copy()
    previous_df["key"] = previous_df["ticker"].apply(normalize_text) + "|" + previous_df["cusip"].apply(normalize_text)
    mask = previous_df["key"].isin(missing_keys)
    flagged = build_flagged_records(mask, previous_df, "MISSING_FROM_CURRENT_FEED", "Security existed previously but is missing in the current feed.")
    return flagged.drop(columns=["key"], errors="ignore")


def check_price_spike(ctx: ValidationContext) -> pd.DataFrame:
    merged = ctx.current_df.merge(
        ctx.previous_df[["ticker", "cusip", "price"]],
        on=["ticker", "cusip"],
        how="left",
        suffixes=("_current", "_previous"),
    )
    merged["pct_change"] = (merged["price_current"] - merged["price_previous"]) / merged["price_previous"]
    mask = merged["price_previous"].notna() & (merged["pct_change"].abs() > PRICE_SPIKE_THRESHOLD)
    flagged = merged.loc[mask].copy()
    if flagged.empty:
        return pd.DataFrame()
    flagged["issue_type"] = "PRICE_SPIKE"
    flagged["issue_details"] = f"Absolute price change exceeded {PRICE_SPIKE_THRESHOLD:.0%}."
    flagged.rename(columns={"price_current": "price"}, inplace=True)
    flagged["previous_price"] = flagged["price_previous"]
    flagged["percent_change"] = (flagged["pct_change"] * 100).round(2)
    columns_to_drop = ["price_previous", "pct_change"]
    return flagged.drop(columns=columns_to_drop, errors="ignore")


def check_unexpected_attribute_change(ctx: ValidationContext) -> pd.DataFrame:
    merged = ctx.current_df.merge(
        ctx.previous_df[["ticker", "cusip", "sector", "asset_type"]],
        on=["ticker", "cusip"],
        how="left",
        suffixes=("_current", "_previous"),
    )
    mask = (
        merged["sector_previous"].notna()
        & ((merged["sector_current"] != merged["sector_previous"]) | (merged["asset_type_current"] != merged["asset_type_previous"]))
    )
    flagged = merged.loc[mask].copy()
    if flagged.empty:
        return pd.DataFrame()
    flagged["issue_type"] = "UNEXPECTED_ATTRIBUTE_CHANGE"
    flagged["issue_details"] = (
        "Sector or asset type changed versus the previous day. Review for intentional reference data updates."
    )
    flagged.rename(
        columns={
            "sector_current": "sector",
            "asset_type_current": "asset_type",
        },
        inplace=True,
    )
    return flagged


def run_checks(ctx: ValidationContext) -> pd.DataFrame:
    checks: list[Callable[[ValidationContext], pd.DataFrame]] = [
        check_missing_ticker,
        check_missing_required_fields,
        check_duplicate_records,
        check_invalid_asset_type,
        check_negative_price,
        check_future_issue_date,
        check_date_logic,
        check_missing_from_current_feed,
        check_price_spike,
        check_unexpected_attribute_change,
    ]

    flagged_frames = [check(ctx) for check in checks]
    non_empty = [frame for frame in flagged_frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()

    flagged_records = pd.concat(non_empty, ignore_index=True, sort=False)
    preferred_columns = [
        "ticker",
        "cusip",
        "security_name",
        "asset_type",
        "sector",
        "exchange",
        "currency",
        "price",
        "issue_date",
        "maturity_date",
        "status",
        "last_updated",
        "previous_price",
        "percent_change",
        "issue_type",
        "issue_details",
    ]
    ordered_columns = [column for column in preferred_columns if column in flagged_records.columns]
    remaining_columns = [column for column in flagged_records.columns if column not in ordered_columns]
    return flagged_records[ordered_columns + remaining_columns].sort_values(by=["issue_type", "ticker"], na_position="last")


def write_outputs(flagged_records: pd.DataFrame, ctx: ValidationContext) -> None:
    output_dir = ctx.project_root / "output"
    output_dir.mkdir(exist_ok=True)

    flagged_records.to_csv(output_dir / "flagged_records.csv", index=False)

    summary = (
        flagged_records.groupby("issue_type")
        .size()
        .reset_index(name="issue_count")
        .sort_values(by="issue_count", ascending=False)
    )
    summary.to_csv(output_dir / "validation_report.csv", index=False)

    total_current_records = len(ctx.current_df)
    total_previous_records = len(ctx.previous_df)
    total_flagged = len(flagged_records)

    with open(output_dir / "audit_summary.txt", "w", encoding="utf-8") as handle:
        handle.write("Financial Security Data Quality Monitor - Audit Summary\n")
        handle.write("=" * 58 + "\n\n")
        handle.write(f"Previous day record count: {total_previous_records}\n")
        handle.write(f"Current day record count: {total_current_records}\n")
        handle.write(f"Total flagged records: {total_flagged}\n\n")
        handle.write("Issue counts by category:\n")
        for _, row in summary.iterrows():
            handle.write(f"- {row['issue_type']}: {row['issue_count']}\n")
        handle.write("\nTop priority review items:\n")
        priority_items = ["NEGATIVE_PRICE", "DATE_LOGIC_ERROR", "MISSING_FROM_CURRENT_FEED", "PRICE_SPIKE"]
        for issue_type in priority_items:
            count = int(summary.loc[summary["issue_type"] == issue_type, "issue_count"].sum())
            handle.write(f"- {issue_type}: {count}\n")


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    ctx = load_data(project_root)
    flagged_records = run_checks(ctx)

    if flagged_records.empty:
        print("No validation issues found.")
        return

    write_outputs(flagged_records, ctx)
    print("Validation complete. Output files written to the output directory.")


if __name__ == "__main__":
    main()
