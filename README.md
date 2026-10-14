# Order Data Pipeline

A Python pipeline built for my Databases & Automation university course. It will later be linked into Power Automate.

It extracts orders (CSV), customers (SQLite) and delivery logs (JSON), cleans and merges them into one master dataset, draws two summary charts, and exports a JSON payload of high-priority escalations for failed deliveries. Each run appends an entry to an audit log, whether it succeeds or fails.

## Requirements

- Python 3.14+
- [uv](https://docs.astral.sh/uv/) (dependencies are declared inline in each script)

## Usage

```powershell
uv run scripts/generate_data.py   # create sample input data
uv run scripts/pipeline.py        # run the pipeline
uv run scripts/test_pipeline.py   # run the tests
```

Input is read from `input/` and results are written to `output/`.

## Outputs

- `combined_master_dataset.csv`: cleaned, merged data
- `high_priority_escalations.csv` / `.json`: escalations for Power Automate
- `pipeline_audit.jsonl`: one line per run
- Two summary charts

## Roadmap

- Trigger the pipeline and consume the escalation JSON from Power Automate
