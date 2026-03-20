from pathlib import Path

import pandas as pd


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    flagged_path = project_root / "output" / "flagged_records.csv"

    if not flagged_path.exists():
        raise FileNotFoundError("Run validate_data.py first so flagged_records.csv exists.")

    flagged = pd.read_csv(flagged_path)
    summary = flagged.groupby("issue_type").size().reset_index(name="issue_count")
    summary = summary.sort_values(by="issue_count", ascending=False)

    summary_path = project_root / "output" / "validation_report.csv"
    summary.to_csv(summary_path, index=False)
    print(f"Validation report written to {summary_path}")


if __name__ == "__main__":
    main()
