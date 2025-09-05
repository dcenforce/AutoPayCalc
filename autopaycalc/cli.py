import argparse
import sys
from pathlib import Path
from typing import Optional, List, Iterable, Set

import pandas as pd
import yaml


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def find_excel_files(input_dir: Path) -> List[Path]:
    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {input_dir}")
    return sorted(input_dir.glob("*.xlsx"))


def read_excels(files: List[Path]) -> pd.DataFrame:
    frames = []
    for file in files:
        try:
            df = pd.read_excel(file, engine="openpyxl")
            df["_source_file"] = file.name
            frames.append(df)
        except Exception as e:
            print(f"Warning: failed to read {file}: {e}", file=sys.stderr)
    if not frames:
        raise RuntimeError("No Excel files could be read.")
    return pd.concat(frames, ignore_index=True)


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


def summarize(df: pd.DataFrame, include_numbers: Optional[Set[str]] = None) -> pd.DataFrame:
    required = ["Employee Number", "Record Code", "Current Hours"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns for summary: {missing}")

    # Clean and convert data
    df = df.copy()
    df["Employee Number"] = df["Employee Number"].astype(str).str.strip()
    df["Record Code"] = df["Record Code"].astype(str).str.strip()
    df["Current Hours"] = pd.to_numeric(df["Current Hours"], errors="coerce").fillna(0.0)

    # Optional filter: include only specified Employee Numbers
    include_numbers = include_numbers or set()
    if include_numbers:
        df = df[df["Employee Number"].isin(include_numbers)].copy()

    # Group by Employee Number and Record Code, sum Current Hours
    grouped = (
        df.groupby(["Employee Number", "Record Code"], dropna=False)
          .agg(TotalHours=("Current Hours", "sum"))
          .reset_index()
    )
    
    # Filter for records where TotalHours > 0
    grouped = grouped[grouped["TotalHours"] > 0].sort_values(
        ["Employee Number", "Record Code"]
    )
    
    return grouped


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

    # Load config and resolve input path
    cfg = load_config(args.config) if args.config else {}
    input_path = args.input if args.input is not None else Path(str(cfg.get("input_path", "")).strip())
    include_numbers = _parse_include_numbers(cfg.get("include_employee_numbers"))

    if not input_path:
        print("Error: input path not provided (via --input or config.yaml input_path)", file=sys.stderr)
        return 2

    try:
        files = find_excel_files(input_path)
        if not files:
            print(f"No .xlsx files found in: {input_path}", file=sys.stderr)
            return 1
        df = read_excels(files)
        df = split_employee_column(df)
        summary = summarize(df, include_numbers=include_numbers)

        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            summary.to_csv(args.output, index=False)
            print(f"Summary written to: {args.output}")
        else:
            # Print to console
            pd.set_option("display.max_rows", None)
            pd.set_option("display.width", 120)
            print(summary.to_string(index=False))
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
