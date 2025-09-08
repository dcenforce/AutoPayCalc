AutoPayCalc — Instructions

Overview
- This tool reads Weekly “Earnings and Hours by Pay” report exports and produces:
  - A summary CSV of hours by employee and pay period
  - An adjustments import CSV (H/D format) for autopay hours

Prerequisites
- In your payroll system, go to Payroll > Reports and run the “Earnings and Hours by Pay” report for each pay period in scope. Export to Excel (.xlsx).
- Run your employee export process (often named “EmpList for Autopay.dat”), then open it and save as Excel: emplist.xlsx.

Folder setup
- Place all weekly report files and emplist.xlsx in the folder you set as `input_path` in config.yaml.
- Optionally keep the employee file elsewhere and set `employee_input_name` to its file name.

Update config.yaml
- Required paths:
  - input_path: path to the folder with your Excel files
  - output_path: folder to write output files
  - output_name: file name for the summary (e.g., "Recorded Hours.csv")
- File scanning:
  - input_mask: glob to match just the weekly Hours/Earnings files (e.g., "Earnings and Hours by Pay*.xlsx")
  - employee_input_name: emplist.xlsx (the employee list saved as Excel)
- Pay period configuration (remove hardcoding):
  - pay_period1_end_date: the end date for period 1 (e.g., "2024-12-31")
  - pay_period_length_days: number of days in a period (default 7)
  - proration_mode: "business_days" or "calendar_days" (default "business_days")

Run
- Activate your virtual environment and run:
  - python -m autopaycalc --config config.yaml

Results
- Summary CSV saved to `output_path/output_name`
- Adjustments CSV saved to `output_path/adjustments_YYYYMMDD_HHMMSS.csv`
- Console prints a per-employee summary: Start Date, End Date, Hours On File, New Hours To Record

Notes
- The tool merges `emplist.xlsx` on `EmployeeNumber` and reads `EffectiveStart`/`EffectiveEnd` (or variants). It derives Expected Daily Hours from NormalWeeklyHours/5 when available.
- Adjustments are prorated per period using the configured proration mode and pay period length.
