import argparse
import csv
import re
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple, Set
from collections.abc import Iterable

import pandas as pd
import yaml

import warnings

# Suppress openpyxl style warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.styles.stylesheet")


def check_file_locks(file_paths: List[Path]) -> List[Path]:
    """Check if any of the given files are locked/in use. Returns list of locked files."""
    locked_files = []
    for file_path in file_paths:
        if file_path.exists():
            try:
                # Try to open the file in write mode to check if it's locked
                with file_path.open("a", encoding="utf-8"):
                    pass
            except (PermissionError, OSError):
                locked_files.append(file_path)
    return locked_files


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def find_excel_files(input_dir: Path, pattern: str = "*.xlsx") -> List[Path]:
    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {input_dir}")
    return sorted(input_dir.glob(pattern))


def detect_employee_file(input_dir: Path) -> Optional[str]:
    """Try to detect an employee list file by inspecting headers for EmployeeNumber and EffectiveStart/End."""
    for p in find_excel_files(input_dir, "*.xlsx"):
        try:
            df_head = pd.read_excel(p, engine="openpyxl", nrows=0)
            cols = [str(c).strip() for c in df_head.columns]
            has_emp = ("EmployeeNumber" in cols) or ("Employee Number" in cols) or ("Employee No" in cols)
            has_start = ("EffectiveStart" in cols) or ("Effective Start" in cols)
            has_end = ("EffectiveEnd" in cols) or ("Effective End" in cols) or ("TerminationDate" in cols)
            if has_emp and (has_start or has_end):
                return p.name
        except Exception:
            continue
    return None


def read_excels(files: List[Path]) -> pd.DataFrame:
    frames = []
    for file in files:
        try:
            df = pd.read_excel(file, engine="openpyxl")
            df["_source_file"] = file.name
            # Only include hours/earnings files that contain the expected column
            if "Pay Run Name" in df.columns:
                frames.append(df)
        except Exception as e:
            print(f"Warning: failed to read {file}: {e}", file=sys.stderr)
    if not frames:
        raise RuntimeError("No Excel files could be read.")
    return pd.concat(frames, ignore_index=True)


def read_employee_list(input_dir: Path, filename: str) -> pd.DataFrame:
    """Read employee list from Excel file with employee details including start/end dates."""
    file_path = input_dir / filename
    if not file_path.exists():
        print(f"Warning: Employee list file not found: {file_path}", file=sys.stderr)
        return pd.DataFrame()
    
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
        # Clean column names (remove extra spaces)
        df.columns = df.columns.str.strip()

        # Map known column names from Emplist to our canonical names (tolerate slight variations)
        def _has(col_name: str) -> bool:
            return col_name in df.columns

        # Employee number
        if _has("EmployeeNumber"):
            df["Employee Number"] = df["EmployeeNumber"]
        elif _has("Employee No"):
            df["Employee Number"] = df["Employee No"]

        # Start/End effective dates
        if _has("EffectiveStart"):
            df["Start Date"] = df["EffectiveStart"]
        elif _has("Effective Start"):
            df["Start Date"] = df["Effective Start"]

        if _has("EffectiveEnd"):
            df["End Date"] = df["EffectiveEnd"]
        elif _has("Effective End"):
            df["End Date"] = df["Effective End"]
        # Fallback: use TerminationDate as End Date if EffectiveEnd missing
        if "End Date" not in df.columns and _has("TerminationDate"):
            df["End Date"] = df["TerminationDate"]

        # Derive Expected Daily Hours
        expected_daily = None
        if "NormalWeeklyHours" in df.columns:
            try:
                expected_daily = pd.to_numeric(df["NormalWeeklyHours"], errors="coerce") / 5.0
            except Exception:
                expected_daily = None
        if expected_daily is None and "AverageDailyHours" in df.columns:
            # Some sources put 40 in AverageDailyHours (actually weekly). Convert when value looks weekly.
            col = pd.to_numeric(df["AverageDailyHours"], errors="coerce")
            # Heuristic: values > 24 are treated as weekly and divided by 5
            expected_daily = col.where(col <= 24, col / 5.0)
        if expected_daily is not None:
            df["Expected Daily Hours"] = expected_daily

        return df
    except Exception as e:
        print(f"Warning: failed to read employee list {file_path}: {e}", file=sys.stderr)
        return pd.DataFrame()


def split_employee_column(df: pd.DataFrame) -> pd.DataFrame:
    # Expecting 'Employee' column to contain "Name - Employee Number"
    if "Employee" not in df.columns:
        # Fallback: try the third column (index 2) if present
        if df.shape[1] >= 3:
            employee_col = df.columns[2]
        else:
            raise KeyError("'Employee' column not found and cannot infer column C.")
    else:
        employee_col = "Employee"

    split = df[employee_col].astype(str).str.split(" - ", n=1, expand=True)
    df["Name"] = split[0].str.strip()
    df["Employee Number"] = split[1].str.strip() if split.shape[1] > 1 else pd.NA
    return df


def _parse_include_numbers(raw: Optional[Iterable]) -> Set[str]:
    """Parse include list from config; supports CSV string or YAML list."""
    if raw is None:
        return set()
    if isinstance(raw, str):
        items = [x.strip() for x in raw.split(",") if x.strip()]
        return set(items)
    try:
        return {str(x).strip() for x in raw if str(x).strip()}
    except TypeError:
        return set()


def add_business_dates_to_pivot(
    pivot_df: pd.DataFrame,
    base_end_date: Optional[datetime] = None,
    base_pay_date: Optional[datetime] = None,
    period_length_days: int = 7,
) -> pd.DataFrame:
    """Add business dates to pivot columns based on pay period numbers.

    - base_end_date: The end date (e.g., 12/31/2024) for PP1 from config.
    - period_length_days: Number of days in each period (default 7).
    """
    # Base date for pay period 1; use config if provided, else fallback to legacy default
    base_date = base_end_date if base_end_date is not None else datetime(2024, 12, 31)
    
    # Get pay run columns (exclude Employee Number, Start Date, End Date)
    date_cols = ["Employee Number", "Start Date", "End Date"]
    pay_run_cols = [col for col in pivot_df.columns if col not in date_cols]
    
    def _infer_period_num_from_name(name: str) -> Optional[int]:
        # Extract first non-zero integer from the name like "ZXJ - 01-00"
        nums = re.findall(r"\d+", str(name))
        for s in nums:
            try:
                n = int(s)
                if n > 0:
                    return n
            except Exception:
                continue
        # If no positive int found, return None to allow fallback
        return None

    # Create a mapping of pay run names to business dates
    date_mapping = {}
    print(f"Debug: Pay run columns found: {sorted(pay_run_cols)}")
    for i, col in enumerate(sorted(pay_run_cols)):
        period_num = _infer_period_num_from_name(col) or (i + 1)
        # Advance by period_length_days per period
        business_date = base_date + timedelta(days=(period_num - 1) * max(1, int(period_length_days)))
        date_mapping[col] = business_date.strftime("%m/%d/%Y")
        print(f"Debug: Column '{col}' -> Period {period_num} -> Business Date {business_date.strftime('%m/%d/%Y')}")
    
    # Create new column names with business dates and pay dates
    renamed_cols = {}
    pay_date_mapping = {}
    
    for col in pivot_df.columns:
        if col in date_mapping:
            # Calculate pay date for this period
            period_num = _infer_period_num_from_name(col) or 1
            if base_pay_date:
                pay_date = base_pay_date + timedelta(days=(period_num - 1) * max(1, int(period_length_days)))
                pay_date_str = pay_date.strftime("%m/%d/%Y")
                pay_date_mapping[col] = pay_date
                renamed_cols[col] = f"{col} (End: {date_mapping[col]}, Pay: {pay_date_str})"
            else:
                renamed_cols[col] = f"{col} ({date_mapping[col]})"
        else:
            renamed_cols[col] = col
    
    # Rename columns
    pivot_df = pivot_df.rename(columns=renamed_cols)
    
    # Return both the dataframe and pay date mapping
    return pivot_df, pay_date_mapping


def _infer_period_num_from_name(name: str) -> Optional[int]:
    """Extract first non-zero integer from the name like 'ZXJ - 01-00'."""
    nums = re.findall(r"\d+", str(name))
    for s in nums:
        try:
            n = int(s)
            if n > 0:
                return n
        except Exception:
            continue
    return None


def _extract_business_date(col_name: str) -> Optional[str]:
    """Extract MM/DD/YYYY date from a summary column name like '... (End: 12/31/2024, Pay: 01/08/2025)'."""
    # Try new format first: "End: MM/DD/YYYY"
    m = re.search(r"End:\s*(\d{1,2}/\d{1,2}/\d{4})", str(col_name))
    if m:
        return m.group(1)
    
    # Fallback to old format: "(MM/DD/YYYY)"
    m = re.search(r"\((\d{1,2}/\d{1,2}/\d{4})\)$", str(col_name))
    return m.group(1) if m else None


def _get_sample_record_lengths(_: Path) -> tuple[int, int]:
    """Return (h_len, d_len) strictly from Adjustment Entries Template.csv.

    No fallback is performed. Raises on error.
    """
    template = Path("Adjustment Entries Template.csv")
    if not template.exists():
        raise FileNotFoundError("Required 'Adjustment Entries Template.csv' not found in working directory")
    with template.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            first = next(reader)
            second = next(reader)
        except StopIteration:
            raise ValueError("Template must contain at least two header rows (H and D)")
        if not (first and first[0] == "H" and second and second[0] == "D"):
            raise ValueError("Template first two rows must begin with H and D, respectively")
        return len(first), len(second)


def _read_header_rows(_: Path) -> tuple[List[str], List[str]]:
    """Read header label rows strictly from Adjustment Entries Template.csv.

    No fallback is performed. Raises on error.
    """
    template = Path("Adjustment Entries Template.csv")
    if not template.exists():
        raise FileNotFoundError("Required 'Adjustment Entries Template.csv' not found in working directory")
    with template.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            first = next(reader)
            second = next(reader)
        except StopIteration:
            raise ValueError("Template must contain at least two header rows (H and D)")
        if not (first and first[0] == "H" and second and second[0] == "D"):
            raise ValueError("Template first two rows must begin with H and D, respectively")
        return first, second


def _business_days_inclusive(start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]) -> int:
    """Count business days (Mon-Fri) inclusive between start and end.

    Returns 0 if invalid or end < start.
    """
    if start is None or end is None:
        return 0
    if pd.isna(start) or pd.isna(end) or end < start:
        return 0
    s = pd.to_datetime(start).normalize()
    e = pd.to_datetime(end).normalize()
    days = (e - s).days + 1
    full_weeks, rem = divmod(days, 7)
    weekdays = full_weeks * 5
    start_wd = int(s.weekday())  # Mon=0..Sun=6
    for i in range(rem):
        if (start_wd + i) % 7 < 5:
            weekdays += 1
    return weekdays


def _pp_range_from_business_date(bdate: pd.Timestamp, period_length_days: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Given the business date (period end), return [start, end] inclusive window.

    start = end - (period_length_days - 1) days
    """
    end = pd.to_datetime(bdate).normalize()
    start = (end - timedelta(days=max(1, int(period_length_days)) - 1)).normalize()
    return start, end


def write_adjustments_csv(
    summary_df: pd.DataFrame,
    output_dir: Path,
    code_name: str = "Driver Hours",
    sample_format_path: Optional[Path] = None,
    period_length_days: int = 7,
    proration: str = "business_days",
    default_daily_hours: float = 8.0,
    grant_autopay_hours: float = 40.0,
    pay_date_mapping: dict = None,
) -> tuple[Path, float, dict]:
    """Write an adjustments import CSV with Header (H) and Detail (D) records.

    - One H record per employee number.
    - One D record per non-zero hours cell, using the column's business date.
    - Output name: adjustments_YYYYMMDD_HHMMSS.csv in output_dir.
    - Column counts mirror the provided sample file if available.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    # Output file name with Year+Month only (no day/time)
    ym = datetime.now().strftime("%Y%m")
    out_path = output_dir / f"adjustments_{ym}.csv"

    # Determine field counts for H/D rows
    if sample_format_path is None:
        sample_format_path = Path("Driver Hour Sample.csv")
    h_len, d_len = _get_sample_record_lengths(sample_format_path)

    # Identify pay columns (exclude identifier/date columns)
    ignore_cols = {"Employee Number", "Start Date", "End Date"}
    # Also exclude any column that starts with "Expected Daily Hours"
    pay_cols: List[str] = [c for c in summary_df.columns 
                          if c not in ignore_cols and not c.startswith("Expected Daily Hours")]
    
    # Removed general debug output

    check_counter = 1
    total_adjust_hours = 0.0
    per_employee: dict = {}
    
    # Collect proration info for separate debug section
    proration_info = []
    
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)

        # Write mandatory two header rows (labels)
        h_labels, d_labels = _read_header_rows(sample_format_path)
        
        # Define columns to exclude completely from CSV
        excluded_columns = {"Import Set", "Month", "Year", "Quarter", "Saved By"}
        
        # Remove excluded columns and empty trailing columns from both header rows
        filtered_h_labels = []
        filtered_d_labels = []
        
        # Filter H row - remove columns where H label is in excluded list or empty
        for h_label in h_labels:
            if h_label.strip() and h_label not in excluded_columns:
                filtered_h_labels.append(h_label)
        
        # Filter D row - remove columns where D label is in excluded list or empty  
        for d_label in d_labels:
            if d_label.strip() and d_label not in excluded_columns:
                filtered_d_labels.append(d_label)
        
        writer.writerow(filtered_h_labels)
        writer.writerow(filtered_d_labels)

        # Build index maps for name-based placement using filtered labels
        h_idx = {name: i for i, name in enumerate(filtered_h_labels)}
        d_idx = {name: i for i, name in enumerate(filtered_d_labels)}

        for _, row in summary_df.iterrows():
            emp_no = str(row.get("Employee Number", "")).strip()
            if not emp_no:
                continue
            
            # Removed general employee debug output

            # Header (H) record
            h = [""] * len(filtered_h_labels)
            h[0] = "H"
            if "Employee No." in h_idx:
                h[h_idx["Employee No."]] = emp_no
            # Exclude Import Set column per user request
            # if "Import Set" in h_idx:
            #     h[h_idx["Import Set"]] = ""
            # Remaining fields left blank intentionally
            # Ensure exact length
            writer.writerow(h)

            # Detail (D) records for each pay period with proration by employment dates
            # Determine employment dates for this row (optional)
            start_dt = pd.to_datetime(row.get("Start Date"), errors="coerce") if "Start Date" in summary_df.columns else pd.NaT
            end_dt = pd.to_datetime(row.get("End Date"), errors="coerce") if "End Date" in summary_df.columns else pd.NaT

            # Use grant_autopay_hours from config, not employee data
            # This is the total weekly hours we want to grant for autopay
            weekly_grant_hours = grant_autopay_hours
            expected_daily = weekly_grant_hours / 5.0  # Convert weekly to daily for business days

            # Initialize per-employee accumulator
            emp_hours_on_file = 0.0
            emp_adj_hours = 0.0
            
            # Track if employee starts after period 1 or ends before last period
            first_period_date = None
            last_period_date = None
            for col in pay_cols:
                bdate_str = _extract_business_date(col) or ""
                bdate = pd.to_datetime(bdate_str, errors="coerce") if bdate_str else pd.NaT
                if not pd.isna(bdate):
                    if first_period_date is None or bdate < first_period_date:
                        first_period_date = bdate
                    if last_period_date is None or bdate > last_period_date:
                        last_period_date = bdate

            for col in pay_cols:
                bdate_str = _extract_business_date(col) or ""
                bdate = pd.to_datetime(bdate_str, errors="coerce") if bdate_str else pd.NaT
                if pd.isna(bdate):
                    continue
                # Pay period range from business date
                week_start, week_end = _pp_range_from_business_date(bdate, period_length_days)
                
                # Determine effective employment period for this pay period
                eff_start = week_start
                eff_end = week_end
                should_prorate = False
                
                # Check if employee start date cuts into this period
                if not pd.isna(start_dt) and start_dt.normalize() > week_start:
                    should_prorate = True
                    eff_start = start_dt.normalize()
                    
                # Check if employee end date cuts into this period  
                if not pd.isna(end_dt) and end_dt.normalize() < week_end:
                    should_prorate = True
                    eff_end = end_dt.normalize()
                
                # Calculate overlap days (always calculate, regardless of proration)
                if str(proration).lower() == "calendar_days":
                    overlap_days = (eff_end - eff_start).days + 1 if eff_end >= eff_start else 0
                else:
                    overlap_days = _business_days_inclusive(eff_start, eff_end)
                
                if overlap_days <= 0:
                    continue

                val = row.get(col, "")
                # Normalize value to numeric; treat blanks as 0
                try:
                    hours = float(val) if val != "" else 0.0
                except (TypeError, ValueError):
                    hours = 0.0

                # Target hours for this period = expected_daily * eligible business days
                target_hours = expected_daily * overlap_days
                # Adjustment is target - recorded hours, but not negative
                adj_hours = max(0.0, target_hours - hours)
                
                # Collect proration info for debug section
                if should_prorate:
                    proration_info.append({
                        'employee': emp_no,
                        'period': bdate_str,
                        'overlap_days': overlap_days,
                        'target_hours': target_hours,
                        'recorded_hours': hours,
                        'adjustment': adj_hours,
                        'reason': 'Prorated'
                    })
                
                # Track totals
                emp_hours_on_file += hours
                emp_adj_hours += adj_hours
                if adj_hours <= 0:
                    continue

                d = [""] * len(filtered_d_labels)
                d[0] = "D"
                # Map by header names per template
                if "Code" in d_idx:
                    d[d_idx["Code"]] = code_name
                if "Hrs." in d_idx:
                    d[d_idx["Hrs."]] = f"{adj_hours:g}"
                if "Amount" in d_idx:
                    d[d_idx["Amount"]] = "0"
                # Year/Quarter from period end date if available
                if pd.notna(bdate):
                    # Exclude Year column per user request
                    # if "Year" in d_idx:
                    #     d[d_idx["Year"]] = str(int(bdate.year))
                    # Exclude Quarter column per user request
                    # if "Quarter" in d_idx:
                    #     q = (int((bdate.month - 1) / 3) + 1)
                    #     d[d_idx["Quarter"]] = str(q)
                    # Exclude Month column per user request
                    # if "Month" in d_idx:
                    #     d[d_idx["Month"]] = str(int(bdate.month))
                    pass
                # Prior Run as PP_YYYY using pay run number and pay date year (zero-padded)
                original_name = col.split(" (")[0] if " (" in col else col
                pp_num = _infer_period_num_from_name(original_name) or 0
                
                # Use pay date year instead of period end date year
                pay_year = bdate.year  # Default fallback
                if pay_date_mapping and original_name in pay_date_mapping:
                    pay_date = pay_date_mapping[original_name]
                    if pd.notna(pay_date):
                        pay_year = pay_date.year
                
                prior_run_val = f"{pp_num:02d}_{int(pay_year)}" if pp_num else ""
                if "Prior Run" in d_idx:
                    d[d_idx["Prior Run"]] = prior_run_val
                
                writer.writerow(d)

                total_adjust_hours += adj_hours

            check_counter += 1

            # Check for employees starting after first period or ending before last period
            if not pd.isna(start_dt) and first_period_date and start_dt.normalize() > first_period_date:
                proration_info.append({
                    'employee': emp_no,
                    'period': 'Employment Period',
                    'overlap_days': None,
                    'target_hours': None,
                    'recorded_hours': None,
                    'adjustment': None,
                    'reason': f'Started {start_dt.strftime("%m/%d/%Y")} (after first period {first_period_date.strftime("%m/%d/%Y")})'
                })
            
            if not pd.isna(end_dt) and last_period_date and end_dt.normalize() < last_period_date:
                proration_info.append({
                    'employee': emp_no,
                    'period': 'Employment Period',
                    'overlap_days': None,
                    'target_hours': None,
                    'recorded_hours': None,
                    'adjustment': None,
                    'reason': f'Ended {end_dt.strftime("%m/%d/%Y")} (before last period {last_period_date.strftime("%m/%d/%Y")})'
                })

            # Record per-employee totals
            per_employee[emp_no] = {
                "start_date": start_dt if pd.notna(start_dt) else None,
                "end_date": end_dt if pd.notna(end_dt) else None,
                "hours_on_file": emp_hours_on_file,
                "new_hours_to_record": emp_adj_hours,
            }

    # Collect adjustment warnings for combined section
    adjustment_warnings = []
    
    # Add proration info to warnings
    if proration_info:
        for info in proration_info:
            if info['reason'] == 'Prorated':
                adjustment_warnings.append(f"Employee {info['employee']}, Period {info['period']}: "
                                         f"Prorated {info['overlap_days']}/5 days, "
                                         f"Target={info['target_hours']:.2f}h, Recorded={info['recorded_hours']:.2f}h, "
                                         f"Adjustment={info['adjustment']:.2f}h")
            else:
                adjustment_warnings.append(f"Employee {info['employee']}: {info['reason']}")
    
    # Store warnings for later display
    global adjustment_warnings_list
    adjustment_warnings_list = adjustment_warnings

    return out_path, total_adjust_hours, per_employee


def create_monthly_adjustments(
    summary_df: pd.DataFrame,
    output_dir: Path,
    code_name: str = "Driver Hours",
    sample_format_path: Optional[Path] = None,
    period_length_days: int = 7,
    proration: str = "business_days",
    default_daily_hours: float = 8.0,
    grant_autopay_hours: float = 40.0,
    pay_date_mapping: dict = None,
) -> List[Path]:
    """Create separate adjustment import files grouped by quarter in which each pay run falls.

    Groups by quarter of the pay date (derived from business date). Filenames: adjustments_Q{q}_{YYYY}.csv
    """
    if not pay_date_mapping:
        return []
    
    # Group pay periods by pay date quarter
    quarterly_groups = {}
    ignore_cols = {"Employee Number", "Start Date", "End Date"}
    # Also exclude any column that starts with "Expected Daily Hours"
    pay_cols = [c for c in summary_df.columns 
                if c not in ignore_cols and not c.startswith("Expected Daily Hours")]
    
    print(f"DEBUG: Total pay columns found: {len(pay_cols)}")
    print(f"DEBUG: Pay date mapping has {len(pay_date_mapping)} entries")
    
    for col in pay_cols:
        # Extract original column name (before business date was added)
        original_col = col.split(" (")[0] if " (" in col else col
        
        if original_col in pay_date_mapping:
            pay_date = pay_date_mapping[original_col]
            if pd.notna(pay_date):
                q = (int(pay_date.month) - 1) // 3 + 1
                quarter_key = f"Q{q}_{int(pay_date.year)}"
                print(f"DEBUG: Column '{col}' -> Pay Date {pay_date} -> Quarter {quarter_key}")
                if quarter_key not in quarterly_groups:
                    quarterly_groups[quarter_key] = []
                quarterly_groups[quarter_key].append(col)
            else:
                print(f"DEBUG: Column '{col}' has NaN pay date")
        else:
            print(f"DEBUG: Column '{col}' (original: '{original_col}') not found in pay_date_mapping")
    
    print(f"DEBUG: Quarterly groups created: {list(quarterly_groups.keys())}")
    for quarter_key, cols in quarterly_groups.items():
        print(f"DEBUG: {quarter_key} has {len(cols)} columns: {cols[:3]}{'...' if len(cols) > 3 else ''}")
    output_files = []
    for quarter_key, quarter_cols in quarterly_groups.items():
        # Subset dataframe to this quarter's columns
        keep_cols = ["Employee Number", "Start Date", "End Date"] + quarter_cols
        expected_cols = [c for c in summary_df.columns if c.startswith("Expected Daily Hours")]
        keep_cols.extend(expected_cols)
        quarter_df = summary_df[keep_cols].copy()
        

        # Prepare writer
        output_dir.mkdir(parents=True, exist_ok=True)
        quarter_path = output_dir / f"adjustments_{quarter_key}.csv"
        if sample_format_path is None:
            sample_format_path = Path("Adjustment Entries Template.csv")
        h_len, d_len = _get_sample_record_lengths(sample_format_path)

        check_counter = 1
        with quarter_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            h_labels, d_labels = _read_header_rows(sample_format_path)
            
            # Define columns to exclude completely from CSV
            excluded_columns = {"Import Set", "Month", "Year", "Quarter", "Saved By"}
            
            # Remove excluded columns and empty trailing columns from both header rows
            filtered_h_labels = []
            filtered_d_labels = []
            
            # Filter H row - remove columns where H label is in excluded list or empty
            for h_label in h_labels:
                if h_label.strip() and h_label not in excluded_columns:
                    filtered_h_labels.append(h_label)
            
            # Filter D row - remove columns where D label is in excluded list or empty  
            for d_label in d_labels:
                if d_label.strip() and d_label not in excluded_columns:
                    filtered_d_labels.append(d_label)
            
            writer.writerow(filtered_h_labels)
            writer.writerow(filtered_d_labels)

            # Build index maps for name-based placement using filtered labels
            h_idx = {name: i for i, name in enumerate(filtered_h_labels)}
            d_idx = {name: i for i, name in enumerate(filtered_d_labels)}

            for _, row in quarter_df.iterrows():
                emp_no = str(row.get("Employee Number", "")).strip()
                if not emp_no:
                    continue

                start_dt = pd.to_datetime(row.get("Start Date"), errors="coerce") if "Start Date" in quarter_df.columns else pd.NaT
                end_dt = pd.to_datetime(row.get("End Date"), errors="coerce") if "End Date" in quarter_df.columns else pd.NaT
                expected_daily = (grant_autopay_hours / 5.0)

                # Check if this employee has any adjustments for this quarter's columns
                employee_has_adjustments = False
                employee_adjustments = []

                print(f"DEBUG: Processing employee {emp_no} for quarter {quarter_key} with {len(quarter_cols)} columns")

                for col in quarter_cols:
                    # Extract original column name to get pay date
                    original_name = col.split(" (")[0] if " (" in col else col
                    
                    # Check if this column's pay date falls within this quarter
                    if pay_date_mapping and original_name in pay_date_mapping:
                        pay_date = pay_date_mapping[original_name]
                        if pd.notna(pay_date):
                            pay_quarter = (int(pay_date.month) - 1) // 3 + 1
                            pay_quarter_key = f"Q{pay_quarter}_{int(pay_date.year)}"
                            
                            # Only process this column if its pay date matches this quarter
                            if pay_quarter_key != quarter_key:
                                print(f"DEBUG: Skipping column {col} - pay date {pay_date} is {pay_quarter_key}, not {quarter_key}")
                                continue
                        else:
                            print(f"DEBUG: Skipping column {col} - no valid pay date")
                            continue
                    else:
                        print(f"DEBUG: Skipping column {col} - not found in pay_date_mapping")
                        continue

                    bdate_str = _extract_business_date(col) or ""
                    bdate = pd.to_datetime(bdate_str, errors="coerce") if bdate_str else pd.NaT
                    if pd.isna(bdate):
                        continue
                    week_start, week_end = _pp_range_from_business_date(bdate, period_length_days)
                    eff_start = week_start if pd.isna(start_dt) else max(week_start, start_dt.normalize())
                    eff_end = week_end if pd.isna(end_dt) else min(week_end, end_dt.normalize())
                    if str(proration).lower() == "calendar_days":
                        overlap_days = (eff_end - eff_start).days + 1 if eff_end >= eff_start else 0
                    else:
                        overlap_days = _business_days_inclusive(eff_start, eff_end)
                    if overlap_days <= 0:
                        continue

                    val = row.get(col, "")
                    try:
                        hours = float(val) if val != "" else 0.0
                    except (TypeError, ValueError):
                        hours = 0.0
                    target_hours = expected_daily * overlap_days
                    adj_hours = max(0.0, target_hours - hours)
                    if adj_hours <= 0:
                        print(f"DEBUG: Employee {emp_no}, Column {col}: No adjustment needed (adj_hours={adj_hours})")
                        continue

                    # Build Prior Run as PP_YYYY (zero-padded)
                    pp_num = _infer_period_num_from_name(original_name) or 0
                    
                    # Use pay date year instead of period end date year
                    pay_year = bdate.year  # Default fallback
                    if pay_date_mapping and original_name in pay_date_mapping:
                        pay_date = pay_date_mapping[original_name]
                        if pd.notna(pay_date):
                            pay_year = pay_date.year
                    
                    prior_run_val = f"{pp_num:02d}_{int(pay_year)}" if pp_num else ""

                    print(f"DEBUG: Employee {emp_no}, Column {col}: Adding adjustment {adj_hours} hours (target={target_hours}, current={hours}) for {quarter_key}")

                    # Store adjustment data for this employee
                    employee_adjustments.append({
                        'adj_hours': adj_hours,
                        'code_name': code_name,
                        'prior_run_val': prior_run_val
                    })
                    employee_has_adjustments = True

                # Only write records for employees who have adjustments in this quarter
                if employee_has_adjustments:
                    # H record
                    h = [""] * len(filtered_h_labels)
                    h[0] = "H"
                    if "Employee No." in h_idx:
                        h[h_idx["Employee No."]] = emp_no
                    writer.writerow(h)

                    # Write all D records for this employee
                    for adj in employee_adjustments:
                        d = [""] * len(filtered_d_labels)
                        d[0] = "D"
                        if "Code" in d_idx:
                            d[d_idx["Code"]] = adj['code_name']
                        if "Hrs." in d_idx:
                            d[d_idx["Hrs."]] = f"{adj['adj_hours']:g}"
                        if "Amount" in d_idx:
                            d[d_idx["Amount"]] = "0"
                        if "Prior Run" in d_idx:
                            d[d_idx["Prior Run"]] = adj['prior_run_val']
                        writer.writerow(d)

                    check_counter += 1

        output_files.append(quarter_path)
        print(f"DEBUG: Quarterly adjustment saved: {quarter_path}")
        
        # Count total adjustment hours in this quarterly file for verification
        total_quarter_hours = sum(adj['adj_hours'] for emp_adjs in [employee_adjustments] for adj in emp_adjs if 'employee_adjustments' in locals())
        print(f"DEBUG: {quarter_key} file contains approximately {total_quarter_hours:.2f} adjustment hours")

    print(f"DEBUG: Created {len(output_files)} quarterly files total")
    return output_files


def create_monthly_outputs(
    summary_df: pd.DataFrame,
    output_dir: Path,
    base_filename: str,
    pay_date_mapping: dict,
) -> List[Path]:
    """Create separate output files for each month based on pay dates."""
    if summary_df.empty:
        return []
    
    # Group columns by pay date month
    monthly_groups = {}
    ignore_cols = {"Employee Number", "Start Date", "End Date", "Expected Daily Hours"}
    
    for col in summary_df.columns:
        if col in ignore_cols:
            continue
            
        # Extract original column name to find pay date
        original_col = col.split(" (")[0] if " (" in col else col
        pay_date = pay_date_mapping.get(original_col)
        
        if pay_date:
            month_key = pay_date.strftime("%Y-%m")
            month_name = pay_date.strftime("%Y_%m_%B")
            
            if month_key not in monthly_groups:
                monthly_groups[month_key] = {
                    'name': month_name,
                    'columns': [],
                    'pay_dates': []
                }
            monthly_groups[month_key]['columns'].append(col)
            monthly_groups[month_key]['pay_dates'].append(pay_date)
    
    # Create output files for each month
    output_files = []
    base_name = Path(base_filename).stem
    
    for month_key, group_info in monthly_groups.items():
        month_name = group_info['name']
        month_cols = group_info['columns']
        
        # Create monthly dataframe with base columns + month columns
        base_cols = [col for col in summary_df.columns if col in ignore_cols]
        monthly_df = summary_df[base_cols + month_cols].copy()
        
        # Remove rows where all pay period columns are empty
        pay_cols_mask = monthly_df[month_cols].apply(lambda row: (row != "").any(), axis=1)
        monthly_df = monthly_df[pay_cols_mask]
        
        if not monthly_df.empty:
            output_file = output_dir / f"{base_name}_{month_name}.csv"
            monthly_df.to_csv(output_file, index=False)
            output_files.append(output_file)
            print(f"Monthly output saved: {output_file}")
    
    return output_files


def summarize(
    df: pd.DataFrame,
    include_numbers: Optional[Set[str]] = None,
    exclude_codes: Optional[Set[str]] = None,
    employee_df: Optional[pd.DataFrame] = None,
    base_end_date: Optional[datetime] = None,
    base_pay_date: Optional[datetime] = None,
    period_length_days: int = 7,
) -> Tuple[pd.DataFrame, dict, pd.DataFrame, pd.DataFrame]:

    # Ensure required columns exist
    required_columns = ["Pay Run Name", "Employee", "Record Code", "Current Hours"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns in input data: {missing}")

    # Clean and convert data
    df = df.copy()
    df["Pay Run Name"] = df["Pay Run Name"].astype(str).str.strip()
    df["Record Code"] = df["Record Code"].astype(str).str.strip()
    df["Current Hours"] = pd.to_numeric(df["Current Hours"], errors="coerce").fillna(0.0)
    
    # Extract employee number from Employee column
    try:
        # Handle cases where Employee is in format "Name - Number" or just the number
        if df["Employee"].str.contains("-").any():
            df["Employee Number"] = df["Employee"].str.split(" - ").str[-1].str.strip()
        else:
            df["Employee Number"] = df["Employee"].str.strip()
    except Exception as e:
        print(f"Error extracting employee number: {e}", file=sys.stderr)
        raise KeyError("Could not extract employee number from 'Employee' column")
    
    # Store original data before exclusions for warning breakdowns
    df_original = df.copy()
    
    # Exclude specified earning codes
    exclude_codes = exclude_codes or set()
    if exclude_codes:
        print(f"Debug: Excluding earning codes: {exclude_codes}")
        print(f"Debug: Records before exclusion: {len(df)}")
        
        # Check what earning codes exist for employee 318050
        emp_318050_data = df[df["Employee Number"] == "318050"]
        if not emp_318050_data.empty:
            unique_codes = emp_318050_data["Record Code"].unique()
            print(f"Debug: Employee 318050 earning codes: {sorted(unique_codes)}")
            
            # Check for PTS codes specifically
            pts_codes = [code for code in unique_codes if "PTS" in code]
            print(f"Debug: Employee 318050 PTS codes: {pts_codes}")
            
            for pts_code in pts_codes:
                pts_data = df[(df["Record Code"] == pts_code) & (df["Employee Number"] == "318050")]
                print(f"Debug: Employee 318050 {pts_code} records: {len(pts_data)}")
        
        df = df[~df["Record Code"].isin(exclude_codes)].copy()
        print(f"Debug: Records after exclusion: {len(df)}")
        
        # Verify PTS-PAYTOSTAY is gone for employee 318050
        remaining_pts = df[(df["Record Code"] == "PTS-PAYTOSTAY") & (df["Employee Number"] == "318050")]
        print(f"Debug: Employee 318050 PTS-PAYTOSTAY records remaining: {len(remaining_pts)}")

    # Filter out any rows where Pay Run Name is empty
    df = df[df["Pay Run Name"] != ""]
    
    # Optional filter: include only specified Employee Numbers
    include_numbers = include_numbers or set()
    if include_numbers:
        df = df[df["Employee Number"].isin(include_numbers)].copy()
        all_employees = sorted(include_numbers)
    else:
        all_employees = sorted(df["Employee Number"].unique())
    
    if df.empty:
        pay_runs = []
    else:
        pay_runs = sorted(df["Pay Run Name"].unique())
    
    # Create a pivot table of total hours by employee and pay run
    if not df.empty:
        pivot_df = (
            df.groupby(["Employee Number", "Pay Run Name"], dropna=False)
            ["Current Hours"].sum()
            .unstack(fill_value=0.0)
            .reindex(columns=pay_runs, fill_value=0.0)
            .round(2)
        )
        
        # Ensure all employees are included, even if they have no records
        pivot_df = pivot_df.reindex(all_employees).fillna(0.0)
        
        # Replace 0.0 with empty string for better readability, ensuring numeric columns
        for col in pivot_df.columns:
            if col != "Employee Number":
                pivot_df[col] = pd.to_numeric(pivot_df[col], errors='coerce').fillna(0.0)
                pivot_df[col] = pivot_df[col].apply(lambda x: "" if x == 0.0 else x)
    else:
        # Create empty DataFrame with correct columns if no data
        pivot_df = pd.DataFrame(columns=pay_runs, index=all_employees)
    
    # Reset index to make Employee Number a column
    pivot_df = pivot_df.reset_index()
    
    # Merge with employee data if provided
    if employee_df is not None and not employee_df.empty:
        # Filter employee data for the same employee numbers
        if include_numbers:
            employee_filtered = employee_df[employee_df["Employee Number"].astype(str).str.strip().isin(include_numbers)].copy()
        else:
            employee_filtered = employee_df.copy()
        
        # Prepare employee data for merge
        employee_filtered["Employee Number"] = employee_filtered["Employee Number"].astype(str).str.strip()
        
        # Select relevant columns for merge (Employee Number, Start Date, End Date, Expected Daily Hours)
        merge_cols = ["Employee Number"]
        if "Start Date" in employee_filtered.columns:
            merge_cols.append("Start Date")
        if "End Date" in employee_filtered.columns:
            merge_cols.append("End Date")
        if "Expected Daily Hours" in employee_filtered.columns:
            merge_cols.append("Expected Daily Hours")
        
        # Normalize date columns to datetime if present
        for dc in ("Start Date", "End Date"):
            if dc in employee_filtered.columns:
                employee_filtered[dc] = pd.to_datetime(employee_filtered[dc], errors="coerce")

        # Aggregate to one row per employee: min Start Date, max End Date, first non-null Expected Daily Hours
        agg_map = {}
        if "Start Date" in merge_cols:
            agg_map["Start Date"] = "min"
        if "End Date" in merge_cols:
            agg_map["End Date"] = "max"
        if "Expected Daily Hours" in merge_cols:
            # Using 'max' as a proxy for first non-null, assuming it's constant per employee
            agg_map["Expected Daily Hours"] = "max"

        if agg_map:
            employee_merge = (
                employee_filtered.groupby("Employee Number", as_index=False)
                .agg(agg_map)
            )
        else:
            employee_merge = employee_filtered[merge_cols].drop_duplicates(subset=["Employee Number"])
        
        # Merge with pivot table
        pivot_df = pivot_df.merge(employee_merge, on="Employee Number", how="left")
        
        # Reorder columns to put dates after Employee Number
        cols = ["Employee Number"]
        if "Start Date" in pivot_df.columns:
            cols.append("Start Date")
        if "End Date" in pivot_df.columns:
            cols.append("End Date")
        if "Expected Daily Hours" in pivot_df.columns:
            cols.append("Expected Daily Hours")
        # Add remaining columns (pay run columns)
        cols.extend([col for col in pivot_df.columns if col not in cols])
        pivot_df = pivot_df[cols]
    
    # Add business dates for pay periods
    pay_date_mapping = {}
    if not pivot_df.empty:
        pivot_df, pay_date_mapping = add_business_dates_to_pivot(pivot_df, base_end_date=base_end_date, base_pay_date=base_pay_date, period_length_days=period_length_days)

    # Filter employees by Start/End dates overlapping the overall period
    if not pivot_df.empty and ("Start Date" in pivot_df.columns or "End Date" in pivot_df.columns):
        # Collect business dates from pay columns
        ignore_cols = {"Employee Number", "Start Date", "End Date"}
        pay_cols = [c for c in pivot_df.columns if c not in ignore_cols]
        pay_dates = [pd.to_datetime(_extract_business_date(c), errors="coerce") for c in pay_cols]
        pay_dates = [d for d in pay_dates if pd.notna(d)]
        if pay_dates:
            period_min = min(pay_dates)
            period_max = max(pay_dates)

            def _row_overlaps(row) -> bool:
                sd = row.get("Start Date")
                ed = row.get("End Date")
                # Treat NaT as unbounded on that side
                if pd.isna(sd):
                    sd = period_min
                if pd.isna(ed):
                    ed = period_max
                return (sd <= period_max) and (ed >= period_min)

            pivot_df = pivot_df[pivot_df.apply(_row_overlaps, axis=1)].copy()
    
    return pivot_df.sort_values("Employee Number"), pay_date_mapping, df_original, df


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read .xlsx files from a folder, split Employee column into Name and Employee Number, "
            "exclude empty Pay Run Names, optionally filter by an include list, and summarize unique employees and total amount by Pay Run Name."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.yaml"),
        help="Path to YAML config file (default: config.yaml)",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Override input folder (takes precedence over config)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional CSV path to write the summary",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    # Load config
    cfg = load_config(args.config) if args.config else {}
    
    # Resolve paths from config or command line arguments
    input_path = args.input if args.input is not None else Path(str(cfg.get("input_path", "")).strip())
    output_path = args.output if args.output is not None else (
        Path(str(cfg.get("output_path", ""))) / cfg.get("output_name", "output.csv")
        if "output_path" in cfg and "output_name" in cfg
        else None
    )
    
    include_numbers = _parse_include_numbers(cfg.get("include_employee_numbers"))
    exclude_codes = set(cfg.get("exclude_earning_codes", []))

    if not input_path:
        print("Error: input path not provided (via --input or config.yaml input_path)", file=sys.stderr)
        return 2

    try:
        # Get input mask from config
        input_mask = cfg.get("input_mask", "*.xlsx")
        employee_input_name = str(cfg.get("employee_input_name", "")).strip()
        # Pay period configuration
        pp1_end_raw = cfg.get("pay_period1_end_date")
        pp1_pay_raw = cfg.get("pay_period1_pay_date")
        base_end_date = None
        base_pay_date = None
        if pp1_end_raw:
            try:
                base_end_date = pd.to_datetime(pp1_end_raw).to_pydatetime()
            except Exception:
                base_end_date = None
        if pp1_pay_raw:
            try:
                base_pay_date = pd.to_datetime(pp1_pay_raw).to_pydatetime()
            except Exception:
                base_pay_date = None
        period_length_days = int(cfg.get("pay_period_length_days", 7))
        proration_mode = str(cfg.get("proration_mode", "business_days")).lower()
        default_daily_hours = float(cfg.get("expected_daily_hours", 8.0))
        grant_autopay_hours = float(cfg.get("grant_autopay_hours", 40.0))
        if not employee_input_name:
            autodetected = detect_employee_file(input_path)
            if autodetected:
                employee_input_name = autodetected
                print(f"Using detected employee list: {employee_input_name}")
        
        # Process files (hours/earnings) limited by mask, excluding the employee list file
        files = find_excel_files(input_path, input_mask)
        if employee_input_name:
            # Exclude by case-insensitive name and stem to handle values with/without extension
            emp_name = Path(str(employee_input_name)).name.strip().lower()
            emp_stem = Path(emp_name).stem.lower()
            before = len(files)
            files = [f for f in files if (f.name.lower() != emp_name and f.stem.lower() != emp_stem)]
            after = len(files)
            # No debug logging here; silently exclude employee list file
        if not files:
            print(f"No files matching '{input_mask}' found in: {input_path}", file=sys.stderr)
            return 1
            
        print(f"Found {len(files)} Excel file(s) to process...")
        df = read_excels(files)
        df = split_employee_column(df)
        
        # Read employee list if specified
        employee_df = None
        if employee_input_name:
            print(f"Reading employee list from {employee_input_name}...")
            employee_df = read_employee_list(input_path, employee_input_name)
            if not employee_df.empty:
                print(f"Loaded {len(employee_df)} employee records")
        
        # Check for locked output files before processing
        output_dir = output_path.parent if output_path else Path.cwd()
        ym = datetime.now().strftime("%Y%m")
        potential_output_files = [
            output_path if output_path else Path.cwd() / "AutoPay Adjustments Summary.xlsx",
            output_dir / f"adjustments_{ym}.csv",
            output_dir / f"adjustments_Q1_{datetime.now().year}.csv",
            output_dir / f"adjustments_Q2_{datetime.now().year}.csv",
            output_dir / f"adjustments_Q3_{datetime.now().year}.csv",
            output_dir / f"adjustments_Q4_{datetime.now().year}.csv",
        ]
        
        locked_files = check_file_locks(potential_output_files)
        if locked_files:
            print("ERROR: The following output files are locked (possibly open in Excel):", file=sys.stderr)
            for locked_file in locked_files:
                print(f"  - {locked_file}", file=sys.stderr)
            print("Please close these files and try again.", file=sys.stderr)
            return 1

        # Generate summary
        print("Generating summary...")
        summary, pay_date_mapping, df_original, df_filtered = summarize(
            df,
            include_numbers=include_numbers,
            exclude_codes=exclude_codes,
            employee_df=employee_df,
            base_end_date=base_end_date,
            base_pay_date=base_pay_date,
            period_length_days=period_length_days,
        )
        
        # Calculate and display summary statistics
        total_employees = len(summary["Employee Number"].unique())
        total_pay_runs = len([col for col in summary.columns if col != "Employee Number"])
        
        # Calculate total hours from pay run columns only (exclude metadata columns)
        total_hours = 0.0
        ignore_cols = {"Employee Number", "Start Date", "End Date"}
        pay_cols = [c for c in summary.columns 
                   if c not in ignore_cols and not c.startswith("Expected Daily Hours")]
        
        for col in pay_cols:
            # Convert to numeric, treating empty strings and non-numeric as 0
            numeric_col = pd.to_numeric(summary[col], errors='coerce').fillna(0.0)
            total_hours += numeric_col.sum()
        
        print("\n=== Summary Statistics ===")
        print(f"Total Employees: {total_employees}")
        print(f"Total Pay Runs: {total_pay_runs}")
        print(f"Total Hours Recorded: {total_hours:.2f}")
        
        # Generate adjustments import file alongside the summary
        adj_output_dir = output_path.parent if output_path else Path.cwd()
        adj_path, total_adj_hours, per_emp = write_adjustments_csv(
            summary_df=summary,
            output_dir=adj_output_dir,
            code_name="Driver Hours",
            pay_date_mapping=pay_date_mapping,
            sample_format_path=Path("Adjustment Entries Template.csv"),
            period_length_days=period_length_days,
            proration=proration_mode,
            default_daily_hours=default_daily_hours,
            grant_autopay_hours=grant_autopay_hours,
        )
        print(f"Adjustment import saved to: {adj_path}")
        
        # Display combined adjustments section will be handled after Excel generation
        
        # Save to Excel file with multiple tabs if output path is specified
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create per-employee summary data
            per_emp_summary = []
            for emp_no in sorted(per_emp.keys(), key=lambda x: (str(x))):
                rec = per_emp[emp_no]
                def _fmt_date(dt):
                    try:
                        return dt.strftime("%m/%d/%Y") if dt else ""
                    except Exception:
                        return ""
                per_emp_summary.append({
                    'Employee': emp_no,
                    'Start Date': _fmt_date(rec.get('start_date')),
                    'End Date': _fmt_date(rec.get('end_date')),
                    'Hours On File': rec.get('hours_on_file', 0.0),
                    'New Hours To Record': rec.get('new_hours_to_record', 0.0)
                })
            
            # Create adjustment amounts by period data
            adjustment_data = []
            ignore_cols = {"Employee Number", "Start Date", "End Date"}
            pay_cols = [c for c in summary.columns 
                       if c not in ignore_cols and not c.startswith("Expected Daily Hours")]
            
            # No need for original summary comparison - we'll use the filtered data
            
            # Initialize warning list as local variable
            over_target_warnings_local = []
            
            for _, row in summary.iterrows():
                emp_no = str(row.get("Employee Number", "")).strip()
                if not emp_no:
                    continue
                    
                start_dt = pd.to_datetime(row.get("Start Date"), errors="coerce") if "Start Date" in summary.columns else pd.NaT
                end_dt = pd.to_datetime(row.get("End Date"), errors="coerce") if "End Date" in summary.columns else pd.NaT
                expected_daily = grant_autopay_hours / 5.0
                
                for col in pay_cols:
                    bdate_str = _extract_business_date(col) or ""
                    bdate = pd.to_datetime(bdate_str, errors="coerce") if bdate_str else pd.NaT
                    if pd.isna(bdate):
                        continue
                        
                    week_start, week_end = _pp_range_from_business_date(bdate, period_length_days)
                    
                    # Calculate effective employment period
                    eff_start = week_start
                    eff_end = week_end
                    if not pd.isna(start_dt) and start_dt.normalize() > week_start:
                        eff_start = start_dt.normalize()
                    if not pd.isna(end_dt) and end_dt.normalize() < week_end:
                        eff_end = end_dt.normalize()
                    
                    # Calculate overlap days
                    if str(proration_mode).lower() == "calendar_days":
                        overlap_days = (eff_end - eff_start).days + 1 if eff_end >= eff_start else 0
                    else:
                        overlap_days = _business_days_inclusive(eff_start, eff_end)
                    
                    if overlap_days <= 0:
                        continue
                    
                    val = row.get(col, "")
                    try:
                        hours = float(val) if val != "" else 0.0
                    except (TypeError, ValueError):
                        hours = 0.0
                    
                    target_hours = expected_daily * overlap_days
                    adj_hours = max(0.0, target_hours - hours)
                    
                    # Check for over-target hours using filtered data (excluding codes already removed)
                    # Only warn if the non-excluded hours exceed the target
                    if hours > target_hours:
                        print(f"Debug: Employee {emp_no}, Period {bdate_str}: hours={hours}, target={target_hours}")
                        # Get breakdown of earning codes for this employee/period from FILTERED data (non-excluded only)
                        original_col_name = col.split(" (")[0] if " (" in col else col
                        filtered_period_data = df_filtered[(df_filtered["Employee Number"] == emp_no) & (df_filtered["Pay Run Name"] == original_col_name)]
                        
                        # Build earning code breakdown from filtered data (non-excluded codes only)
                        code_breakdown = []
                        if not filtered_period_data.empty:
                            for _, record in filtered_period_data.iterrows():
                                code = record.get("Record Code", "Unknown")
                                record_hours = record.get("Current Hours", 0)
                                if record_hours and record_hours > 0:
                                    code_breakdown.append(f"{code}: {record_hours}h")
                        
                        # Only show warning if there are legitimate (non-excluded) codes contributing
                        if code_breakdown:
                            breakdown_str = ", ".join(code_breakdown)
                            over_target_warnings_local.append(f"WARNING: Employee {emp_no}, Period {bdate_str}: "
                                                             f"Recorded {hours:.2f}h exceeds target {target_hours:.2f}h "
                                                             f"({breakdown_str})")
                    
                    # Include all periods, not just those with adjustments > 0
                    adjustment_data.append({
                        'Employee': emp_no,
                        'Period': bdate_str,
                        'Column': col,
                        'Hours Recorded': hours,
                        'Target Hours': target_hours,
                        'Adjustment Hours': adj_hours,
                        'Overlap Days': overlap_days
                    })
            
            # Write Excel file with multiple tabs
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Tab 1: Per-Employee Summary
                per_emp_df = pd.DataFrame(per_emp_summary)
                per_emp_df.to_excel(writer, sheet_name='Employee Summary', index=False)
                
                # Tab 2: Previous Hours (current summary data)
                summary.to_excel(writer, sheet_name='Previous Hours', index=False)
                
                # Tab 3: Adjustment Amounts by Period
                if adjustment_data:
                    adj_df = pd.DataFrame(adjustment_data)
                    adj_df.to_excel(writer, sheet_name='Adjustments by Period', index=False)
            
            print(f"\nResults saved to: {output_path}")
            
            # Display combined adjustments section
            all_adjustment_warnings = []
            if 'adjustment_warnings_list' in globals():
                all_adjustment_warnings.extend(adjustment_warnings_list)
            all_adjustment_warnings.extend(over_target_warnings_local)
            
            if all_adjustment_warnings:
                print("\n=== ADJUSTMENTS ===")
                for warning in all_adjustment_warnings:
                    print(warning)
                print("=" * 20)
            
            # Monthly recorded hours files are not desired - removed

        # Verify totals before generating quarterly files
        def verify_adjustment_totals(adj_path: Path, quarterly_files: List[Path], expected_total: float) -> bool:
            """Verify that the sum of hours in adjustment files matches the expected total."""
            total_from_files = 0.0
            
            # Read main adjustment file
            if adj_path.exists():
                try:
                    with adj_path.open("r", encoding="utf-8-sig") as f:
                        reader = csv.reader(f)
                        for row in reader:
                            if row and row[0] == "D" and len(row) > 3:  # Detail row with hours
                                try:
                                    hours = float(row[3]) if row[3] else 0.0
                                    total_from_files += hours
                                except (ValueError, IndexError):
                                    continue
                except Exception as e:
                    print(f"Warning: Could not verify main adjustment file: {e}", file=sys.stderr)
            
            # Read quarterly files
            for qfile in quarterly_files:
                if qfile.exists():
                    try:
                        with qfile.open("r", encoding="utf-8-sig") as f:
                            reader = csv.reader(f)
                            for row in reader:
                                if row and row[0] == "D" and len(row) > 3:  # Detail row with hours
                                    try:
                                        hours = float(row[3]) if row[3] else 0.0
                                        total_from_files += hours
                                    except (ValueError, IndexError):
                                        continue
                    except Exception as e:
                        print(f"Warning: Could not verify quarterly file {qfile.name}: {e}", file=sys.stderr)
            
            # Check if totals match (within small tolerance for floating point)
            tolerance = 0.01
            matches = abs(total_from_files - expected_total) <= tolerance
            
            print(f"=== ADJUSTMENT FILE VERIFICATION ===")
            print(f"Expected Total Hours: {expected_total:.2f}")
            print(f"Actual Total from Files: {total_from_files:.2f}")
            if matches:
                print("✓ Verification PASSED - Totals match")
            else:
                print("✗ Verification FAILED - Totals do not match")
                print(f"Difference: {abs(total_from_files - expected_total):.2f} hours")
            print("=" * 40)
            
            return matches

        # Generate quarterly adjustment files using same logic as main file (if enabled)
        quarterly_adj_files = []
        if pay_date_mapping and cfg.get('generate_monthly_adjustments', True):
            # First verify main file totals
            main_verification = verify_adjustment_totals(adj_path, [], total_adj_hours)
            
            if main_verification:
                # Group pay periods by quarter
                quarterly_periods = {}
                for col in pay_cols:
                    original_col = col.split(" (")[0] if " (" in col else col
                    if original_col in pay_date_mapping:
                        pay_date = pay_date_mapping[original_col]
                        if pd.notna(pay_date):
                            q = (int(pay_date.month) - 1) // 3 + 1
                            quarter_key = f"Q{q}_{int(pay_date.year)}"
                            if quarter_key not in quarterly_periods:
                                quarterly_periods[quarter_key] = []
                            quarterly_periods[quarter_key].append(col)
                
                print(f"DEBUG: Creating quarterly files for quarters: {list(quarterly_periods.keys())}")
                
                # Generate each quarterly file using original adjustment logic
                for quarter_key, quarter_cols in quarterly_periods.items():
                    quarter_path = adj_output_dir / f"adjustments_{quarter_key}.csv"
                    quarter_total = 0.0
                    
                    with quarter_path.open("w", encoding="utf-8-sig", newline="") as f:
                        writer = csv.writer(f)
                        template_path = Path("Adjustment Entries Template.csv")
                        h_labels, d_labels = _read_header_rows(template_path)
                        
                        # Filter out excluded columns
                        excluded_columns = {"Import Set", "Month", "Year", "Quarter", "Saved By"}
                        filtered_h_labels = [h for h in h_labels if h.strip() and h not in excluded_columns]
                        filtered_d_labels = [d for d in d_labels if d.strip() and d not in excluded_columns]
                        
                        writer.writerow(filtered_h_labels)
                        writer.writerow(filtered_d_labels)
                        
                        h_idx = {name: i for i, name in enumerate(filtered_h_labels)}
                        d_idx = {name: i for i, name in enumerate(filtered_d_labels)}
                        
                        # Process each employee for this quarter's periods only
                        for _, row in summary.iterrows():
                            emp_no = str(row.get("Employee Number", "")).strip()
                            if not emp_no:
                                continue
                            
                            start_dt = pd.to_datetime(row.get("Start Date"), errors="coerce")
                            end_dt = pd.to_datetime(row.get("End Date"), errors="coerce")
                            expected_daily = (grant_autopay_hours / 5.0)
                            
                            employee_has_adjustments = False
                            
                            # Process only this quarter's columns for this employee
                            for col in quarter_cols:
                                # Verify this column's pay date is in this quarter
                                original_col = col.split(" (")[0] if " (" in col else col
                                if original_col not in pay_date_mapping:
                                    continue
                                    
                                pay_date = pay_date_mapping[original_col]
                                if pd.isna(pay_date):
                                    continue
                                    
                                pay_quarter = (int(pay_date.month) - 1) // 3 + 1
                                pay_quarter_key = f"Q{pay_quarter}_{int(pay_date.year)}"
                                if pay_quarter_key != quarter_key:
                                    continue
                                
                                # Use same logic as main adjustment file
                                bdate_str = _extract_business_date(col) or ""
                                bdate = pd.to_datetime(bdate_str, errors="coerce") if bdate_str else pd.NaT
                                if pd.isna(bdate):
                                    continue
                                    
                                week_start, week_end = _pp_range_from_business_date(bdate, period_length_days)
                                eff_start = week_start if pd.isna(start_dt) else max(week_start, start_dt.normalize())
                                eff_end = week_end if pd.isna(end_dt) else min(week_end, end_dt.normalize())
                                
                                if str(proration_mode).lower() == "calendar_days":
                                    overlap_days = (eff_end - eff_start).days + 1 if eff_end >= eff_start else 0
                                else:
                                    overlap_days = _business_days_inclusive(eff_start, eff_end)
                                
                                if overlap_days <= 0:
                                    continue
                                
                                val = row.get(col, "")
                                try:
                                    hours = float(val) if val != "" else 0.0
                                except (TypeError, ValueError):
                                    hours = 0.0
                                
                                target_hours = expected_daily * overlap_days
                                adj_hours = max(0.0, target_hours - hours)
                                
                                if adj_hours <= 0:
                                    continue
                                
                                # Write H record if this is first adjustment for this employee
                                if not employee_has_adjustments:
                                    h = [""] * len(filtered_h_labels)
                                    h[0] = "H"
                                    if "Employee No." in h_idx:
                                        h[h_idx["Employee No."]] = emp_no
                                    writer.writerow(h)
                                    employee_has_adjustments = True
                                
                                # Write D record
                                d = [""] * len(filtered_d_labels)
                                d[0] = "D"
                                if "Code" in d_idx:
                                    d[d_idx["Code"]] = "Driver Hours"
                                if "Hrs." in d_idx:
                                    d[d_idx["Hrs."]] = f"{adj_hours:g}"
                                if "Amount" in d_idx:
                                    d[d_idx["Amount"]] = "0"
                                
                                # Prior Run
                                pp_num = _infer_period_num_from_name(original_col) or 0
                                pay_year = pay_date.year if pd.notna(pay_date) else bdate.year
                                prior_run_val = f"{pp_num:02d}_{int(pay_year)}" if pp_num else ""
                                if "Prior Run" in d_idx:
                                    d[d_idx["Prior Run"]] = prior_run_val
                                
                                writer.writerow(d)
                                quarter_total += adj_hours
                    
                    quarterly_adj_files.append(quarter_path)
                    print(f"Created quarterly file {quarter_key} with {quarter_total:.2f} adjustment hours")
                
                if quarterly_adj_files:
                    print(f"Created {len(quarterly_adj_files)} quarterly adjustment files")
                    
                    # Final verification including quarterly files
                    final_verification = verify_adjustment_totals(adj_path, quarterly_adj_files, total_adj_hours)
                    if not final_verification:
                        print("ERROR: Final verification failed. Quarterly files may contain duplicate data.", file=sys.stderr)
                        # Remove quarterly files if verification fails
                        for qfile in quarterly_adj_files:
                            if qfile.exists():
                                qfile.unlink()
                                print(f"Removed invalid quarterly file: {qfile.name}")
                        quarterly_adj_files = []
            else:
                print("ERROR: Main adjustment file verification failed. Skipping quarterly file generation.", file=sys.stderr)

        # Warnings: average daily hours check (use configured AverageDailyHours per employee; compare to 8)
        try:
            for _, row in summary.iterrows():
                emp_no = str(row.get("Employee Number", "")).strip()
                if not emp_no:
                    continue
                expected_daily = row.get("Expected Daily Hours")
                try:
                    expected_daily_val = float(expected_daily) if expected_daily not in ("", None, pd.NA) else 8.0
                except Exception:
                    expected_daily_val = 8.0
                if abs(expected_daily_val - default_daily_hours) > 1e-6:
                    print(f"Warning: Employee {emp_no} AverageDailyHours {expected_daily_val:.2f} != {default_daily_hours}")
        except Exception as warn_e:
            print(f"Warning computing average daily hours: {warn_e}", file=sys.stderr)


        # Report overall total adjustment hours
        print(f"Total Auto Pay Adjustment Hours: {total_adj_hours:.2f}")

        # Final per-employee summary
        print("\n=== Per-Employee Summary ===")
        print("Employee, Start Date, End Date, Hours On File, New Hours To Record")
        for emp_no in sorted(per_emp.keys(), key=lambda x: (str(x))):
            rec = per_emp[emp_no]
            def _fmt_date(dt):
                try:
                    return dt.strftime("%m/%d/%Y") if dt else ""
                except Exception:
                    return ""
            print(
                f"{emp_no}, {_fmt_date(rec.get('start_date'))}, {_fmt_date(rec.get('end_date'))}, "
                f"{rec.get('hours_on_file', 0.0):.2f}, {rec.get('new_hours_to_record', 0.0):.2f}"
            )
        
        return 0
        
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
