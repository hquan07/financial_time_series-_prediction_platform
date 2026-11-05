"""Generate clean parameterized EDA notebooks with no stored cell output."""

from pathlib import Path

import nbformat

ROOT = Path(__file__).resolve().parents[1]
REPORTS = [
    ("01_data_quality_and_overview", "quality"),
    ("02_returns_and_volatility", "returns"),
    ("03_correlation_and_market_regimes", "correlation"),
    ("04_technical_indicators", "indicators"),
    ("05_macro_market_relationships", "macro"),
]


def main() -> None:
    destination = ROOT / "notebooks"
    destination.mkdir(exist_ok=True)
    for filename, kind in REPORTS:
        notebook = nbformat.v4.new_notebook()
        notebook.metadata["kernelspec"] = {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        }
        notebook.cells = [
            nbformat.v4.new_markdown_cell(
                f"# {filename.replace('_', ' ').title()}\n\n"
                "Select an immutable published dataset version before running real EDA."
            ),
            nbformat.v4.new_code_cell(
                "DATASET_NAME = 'market_features_v1'\n"
                "DATASET_VERSION = 'synthetic-v1'\n"
                "SYNTHETIC = True\n"
                "OUTPUT_DIR = 'artifacts/eda'"
            ),
            nbformat.v4.new_code_cell(
                "from pathlib import Path\n"
                "from financial_time_series.cli.eda import run_report\n"
                f"summary = run_report('{kind}', DATASET_NAME, DATASET_VERSION, "
                "Path(OUTPUT_DIR), synthetic=SYNTHETIC)\n"
                "summary"
            ),
        ]
        nbformat.write(notebook, destination / f"{filename}.ipynb")


if __name__ == "__main__":
    main()
