# Order Data Pipeline

A Python pipeline built for my Databases & Automation university course. It will later be linked into Power Automate.

It extracts orders (CSV), customers (SQLite) and delivery logs (JSON), cleans and merges them into one master dataset, draws three summary charts, and exports a JSON payload of high-priority escalations for failed deliveries. Each run appends an entry to an audit log, whether it succeeds or fails.

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (dependencies are declared inline in each script)

## Usage

```powershell
uv run scripts/generate_data.py   # create sample input data
uv run scripts/preview.py         # print the raw sources (for report screenshots)
uv run scripts/pipeline.py        # run the pipeline
uv run scripts/test_pipeline.py   # run the tests
```

Input is read from `input/` and results are written to `output/`.

Customer emails are hashed with a keyed HMAC. Set the key with the `PIPELINE_HASH_KEY` environment variable; if it is unset a demo key is used, which is fine for the sample data only.

## Escalation rules

Only orders with a `Failed Delivery` status are escalated:

| Priority | Rule |
| --- | --- |
| P1 | Failed delivery for a VIP customer, at any order value |
| P2 | Failed delivery for any other customer (including unmatched ones) worth £100 or more |

Failed orders below £100 for non-VIP customers are not escalated. Delayed and pending orders appear in the charts as value at risk but are not escalated.

## Outputs

- `combined_master_dataset.csv`: cleaned, merged data (customer email is stored only as a hash)
- `high_priority_escalations.csv` / `.json`: escalations for Power Automate, identifying customers by `customer_id` only
- `pipeline_audit.jsonl`: one line per run
- Three summary charts: value at risk by tier, delivery status by carrier, failed deliveries by hub

## Roadmap

- Trigger the pipeline and consume the escalation JSON from Power Automate
