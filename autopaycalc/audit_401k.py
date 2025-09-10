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
    
    After processing, renames files to include pay group and period number
    for easier identification.
    """
    files = _find_report_files(input_dir, pattern)
    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' found in {input_dir}")
    frames = []
    skipped = 0
    duplicates_dir = input_dir / "duplicates"
    duplicates_found = False
    
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
                    
                    # Extract period number from Pay Run Name (e.g., "3EK - 22-00" -> "22")
                    pay_run_name = first_name[0]
                    period_match = pd.Series([pay_run_name]).str.extract(r"(\d+)(?:-\d+)?$")[0]
                    if not period_match.empty and pd.notna(period_match.iloc[0]):
                        period_num = period_match.iloc[0]
                        
                        # Check if filename already contains pay group and period
                        expected_name = f"Earnings and Deductions by Pay {pay_group} {period_num}.xlsx"
                        if file.name != expected_name:
                            new_path = file.parent / expected_name
                            
                            # If target file already exists, move to duplicates folder
                            if new_path.exists():
                                duplicates_dir.mkdir(exist_ok=True)
                                duplicate_path = duplicates_dir / file.name
                                print(f"ERROR: Duplicate file detected: {file.name}", file=sys.stderr)
                                print(f"Moving to: {duplicate_path}", file=sys.stderr)
                                file.rename(duplicate_path)
                                duplicates_found = True
                            else:
                                # print(f"Renaming {file.name} to {expected_name}", file=sys.stderr)
                                file.rename(new_path)
                        
            frames.append(df)
        except Exception as e:
            raise RuntimeError(f"Failed to read {file}: {e}") from e
    if skipped > 0:
        # This warning is no longer needed.
        pass
    if duplicates_found:
        raise RuntimeError(
            "Duplicate files were detected and moved to the duplicates folder. "
            "Please review the duplicates, remove them from the input directory, "
            "and run the process again to avoid double-loaded entries."
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
    end: int = 53,
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

    # Debug: Show original data before filtering
    # print(f"Debug: Original Pay Run Info has {len(df)} rows", file=sys.stderr)
    
    # Filter for desired pay group first
    df = df[df["Pay Group Name"].astype(str).str.startswith(pay_group)]
    # print(f"Debug: After pay group filter ({pay_group}): {len(df)} rows", file=sys.stderr)

    # Normalize period fields first - keep as strings
    df["Pay Run Pay Period"] = (
        df["Pay Run Pay Period"].astype(str).str.extract(r"(\d+)(?:-\d+)?$")[0]
    )
    df["Pay Run Pay Date"] = pd.to_datetime(df["Pay Run Pay Date"], errors="coerce")
    df["Period End"] = pd.to_datetime(df["Period End"], errors="coerce")
    
    # Show available periods before deduplication  
    all_periods = sorted(df["Pay Run Pay Period"].dropna().unique(), key=lambda x: int(x) if x.isdigit() else float('inf'))
    # print(f"Debug: Available periods after normalization: {all_periods}", file=sys.stderr)
    
    # Show pay dates for periods 14-21 to understand year filtering
    periods_14_21 = df[df["Pay Run Pay Period"].isin(['14', '15', '16', '17', '18', '19', '20', '21'])]
    if not periods_14_21.empty:
        # for _, row in periods_14_21.iterrows():
        #     # print(f"  Period {row['Pay Run Pay Period']}: {row['Pay Run Pay Date']} (year: {row['Pay Run Pay Date'].year if pd.notna(row['Pay Run Pay Date']) else 'NaT'})")
        pass
    else:
        # print(f"Debug: No periods 14-21 found after normalization")
        pass
    
    # No deduplication - Pay Run Info should contain unique entries per pay run
    # Each row represents a distinct pay run with its own Pay Run Id
    
    # First restrict to configured pay period ranges (before year filtering)
    period_ints = pd.to_numeric(df["Pay Run Pay Period"], errors="coerce")
    in_range = df[period_ints.between(start, end, inclusive='both')]
    
    # Then filter by pay date year
    year_mask = in_range["Pay Run Pay Date"].dt.year == year
    year_filtered = in_range[year_mask]
    
    # Show periods available after both filters
    # year_periods = sorted(year_filtered["Pay Run Pay Period"].dropna().unique())
    
    # final_periods = sorted(year_filtered["Pay Run Pay Period"].dropna().unique())
    
    return year_filtered.reset_index(drop=True)


def calculate_401k_matches(
    df: pd.DataFrame,
    deduction_codes: List[str],
    excluded_earnings_codes: List[str] = None,
    match_percent: float = 25.0,
    match_minimum: float = 10.0,
    match_max_percent: float = 6.0,
    match_codes: List[str] = None,
) -> pd.DataFrame:
    """Calculate expected and actual 401K match amounts per employee and pay run.

    Args:
        df: Combined earnings/deductions data.
        deduction_codes: Record codes treated as 401K employee deductions.
        excluded_earnings_codes: Record codes to exclude from total earnings calculation.
        match_percent: Percentage of deductions matched by employer.
        match_minimum: Minimum deduction total before a match applies.
        match_max_percent: Maximum match as a percent of normal earnings.
        match_codes: List of earning codes used for employer match (will be summed).
    """
    results: List[dict] = []
    columns = [
        "Employee Number",
        "Pay Run Id",
        "401K Deductions",
        "Current Earnings",
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

    excluded_earnings_codes = excluded_earnings_codes or []
    excluded_codes = [str(c).strip().lower() for c in excluded_earnings_codes]
    deduction_codes_cf = [str(c).strip().lower() for c in deduction_codes]
    match_codes = match_codes or ["401-K ER MATCH"]
    match_codes_cf = [str(c).strip().lower() for c in match_codes]

    for (emp_no, pay_run_id), group in df.groupby(["Employee Number", "Pay Run Id"]):
        types = group["Record Type"].astype(str).str.strip()
        codes = group["Record Code"].astype(str).str.strip()
        codes_cf = codes.str.lower()
        amounts = pd.to_numeric(group["Current Amount"], errors="coerce").fillna(0.0)

        deduction_mask = types.str.contains("deduction", case=False) & codes_cf.isin(
            deduction_codes_cf
        )
        deductions = amounts[deduction_mask].sum()

        earn_mask = types.str.contains("earning", case=False)
        match_mask = earn_mask & codes_cf.isin(match_codes_cf)
        excluded_mask = codes_cf.isin(excluded_codes)
        normal_mask = earn_mask & ~match_mask & ~excluded_mask
        
        # Calculate current pay run earnings excluding match codes and excluded codes
        current_normal_earnings = amounts[normal_mask].sum()

        actual_match = pd.to_numeric(group.loc[match_mask, "Current Amount"], errors="coerce").fillna(0).sum()

        expected_match = 0.0
        if deductions >= match_minimum:
            capped_deductions = min(
                deductions, current_normal_earnings * (match_max_percent / 100.0)
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
                "401K Deductions": round(deductions, 2),
                "Current Earnings": round(current_normal_earnings, 2),
                "Expected Match": round(expected_match, 2),
                "Actual Match": round(actual_match, 2),
                "Match Difference": match_difference,
            }
        )

    return pd.DataFrame(results, columns=columns)
