import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd


EARNINGS_FILE = "Earnings and Deductions by Pay_sample.xlsx"
PAY_RUN_INFO_FILE = "Pay Run Info_9-9-2025-114153-AM.xlsx"


def _write_sample_reports(tmpdir: Path) -> None:
    """Create minimal Excel reports for 401K testing."""
    earnings = pd.DataFrame(
        [
            {
                "Employee": "Alice - 1001",
                "Pay Run Name": "3EK-28",
                "Pay Run Id": "PR123",
                "Record Type": "Earning",
                "Record Code": "REG",
                "Current Amount": 200.0,
            },
            {
                "Employee": "Alice - 1001",
                "Pay Run Name": "3EK-28",
                "Pay Run Id": "PR123",
                "Record Type": "Deduction",
                "Record Code": "401-401K",
                "Current Amount": 40.0,
            },
            {
                "Employee": "Alice - 1001",
                "Pay Run Name": "3EK-28",
                "Pay Run Id": "PR123",
                "Record Type": "Earning",
                "Record Code": "401-K ER MATCH",
                "Current Amount": 0.0,
            },
        ]
    )
    earnings.to_excel(tmpdir / EARNINGS_FILE, index=False)

    pay_run_info = pd.DataFrame(
        [
            {
                "Pay Run Id": "PR123",
                "Pay Run Pay Period": 28,
                "Pay Group Name": "3EK",
                "Pay Run Pay Date": "2025-07-15",
                "Period End": "2025-07-14",
            }
        ]
    )
    pay_run_info.to_excel(tmpdir / PAY_RUN_INFO_FILE, index=False)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        _write_sample_reports(tmpdir)
        output = tmpdir / "summary.csv"
        subprocess.run(
            [
                sys.executable,
                "-m",
                "autopaycalc.cli",
                "--config",
                "401KSettings.YAML",
                "--input",
                str(tmpdir),
                "--output",
                str(output),
            ],
            check=True,
        )
        print(output.read_text())


if __name__ == "__main__":
    main()
