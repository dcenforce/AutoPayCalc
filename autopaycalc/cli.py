import argparse
import sys
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Iterable, Set
import csv
import re

import pandas as pd
import yaml

# Suppress openpyxl style warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.styles.stylesheet")


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


def _extract_business_date(col_name: str) -> Optional[str]:
    """Extract MM/DD/YYYY date from a summary column name like '... (End: 12/31/2024, Pay: 01/08/2025)'."""
    # Try new format first: "End: MM/DD/YYYY"
    m = re.search(r"End:\s*(\d{1,2}/\d{1,2}/\d{4})", str(col_name))
    if m:
        return m.group(1)
    
    # Fallback to old format: "(MM/DD/YYYY)"
    m = re.search(r"\((\d{1,2}/\d{1,2}/\d{4})\)$", str(col_name))
    return m.group(1) if m else None


def _get_sample_record_lengths(sample_path: Path) -> tuple[int, int]:
    """Infer the H and D record field counts from the provided sample CSV.

    Returns (h_len, d_len). Falls back to conservative defaults if the sample is missing.
    """
    try:
        with sample_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            first = next(reader)
            second = next(reader)
            # Validate that they start with H / D, otherwise ignore
            h_len = len(first) if first and first[0] == "H" else 35
            d_len = len(second) if second and second[0] == "D" else 35
            return h_len, d_len
    except Exception:
        # Reasonable defaults matching the provided sample structure (approximate)
        return 35, 35


def _read_header_rows(sample_path: Path) -> tuple[List[str], List[str]]:
    """Read the two header label rows (H header labels, D header labels).

    If the sample is not available, fall back to default labels.
    """
    try:
        with sample_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            first = next(reader)
            second = next(reader)
            if first and first[0] == "H" and second and second[0] == "D":
                return first, second
    except Exception:
        pass

    # Fallback default labels (without trailing empty fields)
    h_labels = [
        "H",
        "Employee Name",
        "Employee No.",
        "Check Type",
        "Check Template",
        "Mark as Pay Out",
        "Locked",
        "Check Number",
        "Check Date",
        "Disbursement Method",
        "Reason",
        "Comment",
        "Residence State",
        "Voided Check Pay Date",
        "Voided Check Net Pay",
        "Voided Payment Number",
        "Status",
        "Total Earnings",
        "Total Deductions",
        "EE Taxes",
        "ER Taxes",
        "Net Pay",
        "Printed",
        "Import Set",
        "Saved By",
        "Saved At",
        "Override Payee",
        "Use as the statement's mailing name and address",
        "Name as on Check",
        "Address Line 1",
        "Address Line 2",
        "Address Line 3",
        "City",
        "State",
        "Zip / Postal Code",
        "Total Distribution Percent",
    ]
    d_labels = [
        "D",
        "Replace",
        "Code",
        "Trailing Taxation Period Start Date",
        "Trailing Taxation Period End Date",
        "Hrs.",
        "Rate",
        "Amount",
        "Job Assignment",
        "Work Location",
        "Legal Entity",
        "Labor %",
        "Labor Metrics",
        "Business Date",
        "Comment",
        "Workers Comp Account",
        "Workers Comp Code",
        "Project",
        "Docket",
        "Limited Taxable Wage",
        "Total Taxable Wage",
        "PPN",
        "FLSA Adjust Start Date",
        "FLSA Adjust End Date",
        "Debit Arrears",
        "Do Not Disburse to Payee",
        "Pay Out Accruals",
        "Ordered Amount Type",
        "Percent",
        "Limit Amount",
        "Disposable Earning Amount",
        "Send To Payment Solutions",
        "Stop Pay Confirmed",
        "Pay Periods for Tax",
        "Saved By",
        "Saved At",
        "Saved At UTC",
    ]
    return h_labels, d_labels


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
) -> tuple[Path, float, dict]:
    """Write an adjustments import CSV with Header (H) and Detail (D) records.

    - One H record per employee number.
    - One D record per non-zero hours cell, using the column's business date.
    - Output name: adjustments_YYYYMMDD_HHMMSS.csv in output_dir.
    - Column counts mirror the provided sample file if available.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"adjustments_{ts}.csv"

    # Determine field counts for H/D rows
    if sample_format_path is None:
        sample_format_path = Path("Driver Hour Sample.csv")
    h_len, d_len = _get_sample_record_lengths(sample_format_path)

    # Identify pay columns (exclude identifier/date columns)
    ignore_cols = {"Employee Number", "Start Date", "End Date", "Expected Daily Hours"}
    pay_cols: List[str] = [c for c in summary_df.columns if c not in ignore_cols]
    
    # Removed general debug output

    check_counter = 1
    total_adjust_hours = 0.0
    per_employee: dict = {}
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)

        # Write mandatory two header rows (labels)
        h_labels, d_labels = _read_header_rows(sample_format_path)
        # Ensure header lengths align with inferred counts
        if len(h_labels) < h_len:
            h_labels = h_labels + [""] * (h_len - len(h_labels))
        elif len(h_labels) > h_len:
            h_labels = h_labels[:h_len]
        if len(d_labels) < d_len:
            d_labels = d_labels + [""] * (d_len - len(d_labels))
        elif len(d_labels) > d_len:
            d_labels = d_labels[:d_len]
        writer.writerow(h_labels)
        writer.writerow(d_labels)

        for _, row in summary_df.iterrows():
            emp_no = str(row.get("Employee Number", "")).strip()
            if not emp_no:
                continue
            
            # Removed general employee debug output

            # Header (H) record
            h = [""] * max(h_len, 8)
            h[0] = "H"
            # 1: Employee Name (leave blank)
            h[2] = emp_no  # Employee No.
            h[3] = "Manual"  # Check Type
            h[4] = "Manual"  # Check Template
            h[6] = "No"       # Locked
            h[7] = str(check_counter)  # Check Number
            # Remaining fields left blank intentionally
            # Ensure exact length
            if len(h) < h_len:
                h.extend([""] * (h_len - len(h)))
            elif len(h) > h_len:
                h = h[:h_len]
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

            for col in pay_cols:
                bdate_str = _extract_business_date(col) or ""
                bdate = pd.to_datetime(bdate_str, errors="coerce") if bdate_str else pd.NaT
                if pd.isna(bdate):
                    continue
                
                # Detailed debugging for specific employee
                if emp_no == "318038":
                    print(f"Debug 318038: Column '{col}' -> Business Date: {bdate_str}")
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
                
                # Debug output only when proration occurs
                if should_prorate:
                    print(f"Debug: Employee {emp_no}, Period {bdate_str}: "
                          f"Prorated {overlap_days}/5 days, "
                          f"Target={target_hours:.2f}h, Recorded={hours:.2f}h, Adjustment={adj_hours:.2f}h")
                
                # Detailed debugging for specific employee
                if emp_no == "318038":
                    print(f"Debug 318038: Period {bdate_str}, Week: {week_start.strftime('%m/%d/%Y')} to {week_end.strftime('%m/%d/%Y')}, "
                          f"Should_prorate={should_prorate}, Overlap_days={overlap_days}, "
                          f"Target={target_hours:.2f}h, Recorded={hours:.2f}h, Adjustment={adj_hours:.2f}h")
                
                # Track totals
                emp_hours_on_file += hours
                emp_adj_hours += adj_hours
                if adj_hours <= 0:
                    continue

                d = [""] * max(d_len, 14)
                d[0] = "D"
                d[1] = "No"            # Replace
                d[2] = code_name        # Code
                # d[3], d[4] (Trailing Taxation Period dates) left blank
                d[5] = f"{adj_hours:g}" # Hrs. (adjusted/prorated)
                d[6] = "0"             # Rate
                d[7] = "0"             # Amount
                # ... many optional fields left blank per sample
                d[13] = bdate_str       # Business Date

                if len(d) < d_len:
                    d.extend([""] * (d_len - len(d)))
                elif len(d) > d_len:
                    d = d[:d_len]
                writer.writerow(d)

                total_adjust_hours += adj_hours

            check_counter += 1

            # Record per-employee totals
            per_employee[emp_no] = {
                "start_date": start_dt if pd.notna(start_dt) else None,
                "end_date": end_dt if pd.notna(end_dt) else None,
                "hours_on_file": emp_hours_on_file,
                "new_hours_to_record": emp_adj_hours,
            }

    return out_path, total_adjust_hours, per_employee


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
) -> pd.DataFrame:

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
    
    # Exclude specified earning codes
    exclude_codes = exclude_codes or set()
    if exclude_codes:
        df = df[~df["Record Code"].isin(exclude_codes)].copy()

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
    
    return pivot_df.sort_values("Employee Number"), pay_date_mapping


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
        
        # Generate summary
        print("Generating summary...")
        summary, pay_date_mapping = summarize(
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
        
        # Calculate total hours, handling mixed data types
        total_hours = 0.0
        for col in summary.columns:
            if col != "Employee Number":
                # Convert to numeric, treating empty strings and non-numeric as 0
                numeric_col = pd.to_numeric(summary[col], errors='coerce').fillna(0.0)
                total_hours += numeric_col.sum()
        
        print("\n=== Summary Statistics ===")
        print(f"Total Employees: {total_employees}")
        print(f"Total Pay Runs: {total_pay_runs}")
        print(f"Total Hours Recorded: {total_hours:.2f}")
        
        # Save to file if output path is specified
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            summary.to_csv(output_path, index=False)
            print(f"\nResults saved to: {output_path}")
            
            # Create monthly output files based on pay dates
            if pay_date_mapping:
                monthly_files = create_monthly_outputs(
                    summary, 
                    output_path.parent, 
                    output_path.name,
                    pay_date_mapping
                )
                if monthly_files:
                    print(f"Created {len(monthly_files)} monthly output files")

        # Always generate adjustments import file alongside the summary
        adj_output_dir = output_path.parent if output_path else Path.cwd()
        adj_path, total_adj_hours, per_emp = write_adjustments_csv(
            summary_df=summary,
            output_dir=adj_output_dir,
            code_name="Driver Hours",
            sample_format_path=Path("Driver Hour Sample.csv"),
            period_length_days=period_length_days,
            proration=proration_mode,
            default_daily_hours=default_daily_hours,
            grant_autopay_hours=grant_autopay_hours,
        )
        print(f"Adjustment import saved to: {adj_path}")

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
