from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Tuple

import sys

import pandas as pd


def _find_report_files(input_dir: Path, pattern: str) -> List[Path]:
    """Return report files matching pattern in input_dir."""
    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {input_dir}")
    return sorted(input_dir.glob(pattern))


def read_earnings_deductions(
    input_dir: Path,
    pattern: str = "Earnings and Deductions by Pay*.xlsx",
    pay_group: str = "3EK",
) -> pd.DataFrame:
    """Read the 'Earnings and Deductions by Pay' report from Excel files.

    Files whose first "Pay Run Name" value does not start with ``pay_group``
    are counted and reported once after scanning.
    """
    files = _find_report_files(input_dir, pattern)
    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' found in {input_dir}")
    frames = []
    skipped = 0
    for file in files:
        try:
            df = pd.read_excel(file, engine="openpyxl")
            df["_source_file"] = file.name
            if "Pay Run Name" in df.columns:
                first_name = (
                    df["Pay Run Name"].dropna().astype(str).str.strip().head(1).tolist()
                )
                if first_name:
                    prefix = first_name[0][: len(pay_group)]
                    if pay_group and prefix != pay_group:
                        skipped += 1
                        continue
            frames.append(df)
        except Exception as e:
            raise RuntimeError(f"Failed to read {file}: {e}") from e
    if skipped:
        print(
            f"Running in {pay_group} mode, skipped {skipped} file(s) due to pay group mismatch",
            file=sys.stderr,
        )
    if not frames:
        raise RuntimeError("No Excel files could be read.")
    return pd.concat(frames, ignore_index=True)


def read_pay_run_info(
    input_dir: Path,
    pattern: str = "Pay Run Info*.xlsx",
    pay_group: str = "3EK",
    year: int = 2025,
    start: int = 1,
    end: int = 52,
) -> pd.DataFrame:
    """Read pay run info file mapping pay run IDs to period end and pay dates.

    Only rows for the specified ``pay_group`` and pay-period ``year`` range are kept.
    Duplicate combinations of pay group and pay period are skipped with a warning.
    """
    files = _find_report_files(input_dir, pattern)
    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' found in {input_dir}")

    df = pd.read_excel(files[0], engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    required = {
        "Pay Run Id",
        "Pay Run Pay Period",
        "Pay Group Name",
        "Pay Run Pay Date",
        "Period End",
    }
    missing = required.difference(df.columns)
    if missing:
        raise KeyError(
            f"Missing required column(s) in pay run info file: {', '.join(sorted(missing))}"
        )

    # Filter for desired pay group first
    df = df[df["Pay Group Name"].astype(str).str.startswith(pay_group)]

    # Normalize period fields first - keep as strings
    df["Pay Run Pay Period"] = (
        df["Pay Run Pay Period"].astype(str).str.extract(r"(\d+)(?:-\d+)?$")[0]
    )
    df["Pay Run Pay Date"] = pd.to_datetime(df["Pay Run Pay Date"], errors="coerce")
    df["Period End"] = pd.to_datetime(df["Period End"], errors="coerce")
    
    # Deduplicate within the filtered pay group only (after normalization)
    # Keep the entry with the most recent pay date for each period
    original_count = len(df)
    df = df.sort_values("Pay Run Pay Date", ascending=False)
    df = df.drop_duplicates(subset=["Pay Group Name", "Pay Run Pay Period"], keep="first")
    
    # Report any duplicates that were found and removed
    if original_count > len(df):
        duplicates_removed = original_count - len(df)
        print(f"Warning: {duplicates_removed} duplicate entries removed for pay group '{pay_group}'", file=sys.stderr)
    
    # Filter by pay date year first
    year_mask = df["Pay Run Pay Date"].dt.year == year
    year_filtered = df[year_mask]
    
    # Then restrict to configured pay period ranges
    period_ints = pd.to_numeric(year_filtered["Pay Run Pay Period"], errors="coerce")
    in_range = year_filtered[period_ints.between(start, end)]

    return in_range.reset_index(drop=True)


def calculate_401k_matches(
    df: pd.DataFrame,
    deduction_codes: List[str],
    match_percent: float = 25.0,
    match_minimum: float = 10.0,
    match_max_percent: float = 6.0,
    match_code: str = "401-K Match",
) -> pd.DataFrame:
    """Calculate expected and actual 401K match amounts per employee and pay run.

    Args:
        df: Combined earnings/deductions data.
        deduction_codes: Record codes treated as 401K employee deductions.
        match_percent: Percentage of deductions matched by employer.
        match_minimum: Minimum deduction total before a match applies.
        match_max_percent: Maximum match as a percent of normal earnings.
        match_code: Earning code used for employer match.
    """
    results: List[dict] = []
    columns = [
        "Employee Number",
        "Pay Run Id",
        "401K Deductions",
        "Expected Match",
        "Actual Match",
        "Match Difference",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)

    required = {
        "Employee Number",
        "Pay Run Id",
        "Record Code",
        "Record Type",
        "Current Amount",
    }
    missing = required.difference(df.columns)
    if missing:
        raise KeyError(
            f"Missing required column(s) for 401K processing: {', '.join(sorted(missing))}"
        )

    ded_codes = {str(c).strip().lower() for c in deduction_codes}

    for (emp_no, pay_run_id), group in df.groupby(["Employee Number", "Pay Run Id"]):
        codes = group["Record Code"].astype(str).str.strip()
        codes_cf = codes.str.casefold()
        types = group["Record Type"].astype(str)

        ded_mask = types.str.contains("deduction", case=False) & codes_cf.isin(ded_codes)
        deduction_total = (
            pd.to_numeric(group.loc[ded_mask, "Current Amount"], errors="coerce")
            .fillna(0)
            .sum()
        )

        earn_mask = types.str.contains("earning", case=False)
        match_mask = earn_mask & (codes_cf == match_code.lower())
        normal_mask = earn_mask & ~match_mask
        normal_earnings = (
            pd.to_numeric(group.loc[normal_mask, "Current Amount"], errors="coerce")
            .fillna(0)
            .sum()
        )

        actual_match = (
            pd.to_numeric(group.loc[match_mask, "Current Amount"], errors="coerce")
            .fillna(0)
            .sum()
        )

        expected_match = 0.0
        if deduction_total >= match_minimum:
            capped_deductions = min(
                deduction_total, normal_earnings * (match_max_percent / 100.0)
            )
            expected_match = capped_deductions * (match_percent / 100.0)

        match_difference = round(expected_match - actual_match, 2)
        
        # Exclude variances of ±0.01 (treat as zero difference)
        if abs(match_difference) == 0.01:
            match_difference = 0.0
        
        results.append(
            {
                "Employee Number": emp_no,
                "Pay Run Id": pay_run_id,
                "401K Deductions": round(deduction_total, 2),
                "Expected Match": round(expected_match, 2),
                "Actual Match": round(actual_match, 2),
                "Match Difference": match_difference,
            }
        )

    return pd.DataFrame(results, columns=columns)
