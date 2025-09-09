# AutoPayCalc

Utilities for processing payroll exports in two modes:

- **Autopay** – grants autopay hours and produces adjustment files.
- **401K** – audits employee 401K deductions versus employer matches and can emit quick-entry CSVs.

Both modes are driven by YAML configuration files. `config.yaml` is for autopay runs, while `401KSettings.YAML` is for 401K audits and lists the pay-run info workbook, pay-period range, pay-group prefix, deduction codes treated as 401K contributions, and the employer match code (`401-K ER MATCH`).

For codex testing a helper script generates minimal Excel input on the fly:

```bash
python -m autopaycalc.cli --help
python tests/test_401k_cli.py
```

The test script builds temporary workbooks, runs the 401K audit, and prints a summary of missing matches. Rows with an existing match that still require an adjustment are marked with a warning in the summary.
