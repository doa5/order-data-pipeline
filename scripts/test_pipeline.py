# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas>=3.0.6",
#     "matplotlib>=3.10",
#     "pytest>=8",
# ]
# ///
"""Tests the pipeline against the 'Expected results' table in the algorithm plan.

Run:  uv run scripts/test_pipeline.py
Outputs are written to a temporary folder, so the real audit log is untouched.
"""

import json
import shutil
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

import pipeline as p

SOURCES = [p.ORDERS_FILE, p.CRM_FILE, p.LOGS_FILE]


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    out = tmp_path_factory.mktemp("out")
    result = p.run_pipeline(input_dir=p.INPUT_DIR, out_dir=out, verbose=False)
    return result, out


@pytest.fixture
def sandbox(tmp_path):
    """A writable copy of the three sources, for corrupting in failure tests."""
    for name in SOURCES:
        shutil.copy(p.INPUT_DIR / name, tmp_path / name)
    return tmp_path


def read_audit(folder):
    return [
        json.loads(line)
        for line in (Path(folder) / p.AUDIT_FILE)
        .read_text(encoding="utf-8")
        .splitlines()
    ]


# ---------------------------------------------------------------- expected results table
def test_rows_in_and_merged(run):
    result, _ = run
    assert result["audit"]["rows_in"] == {"orders": 21, "customers": 8, "logs": 22}
    assert len(result["master"]) == 21


def test_unmapped_customer(run):
    result, _ = run
    assert result["audit"]["unmapped_customers"] == [
        {"customer_id": "CUST_999", "order_id": "ORD_1008"}
    ]
    row = result["master"].set_index("order_id").loc["ORD_1008"]
    assert (
        row["full_name"],
        row["account_tier"],
        row["order_value"],
        row["status"],
    ) == ("Unknown Customer", "Unknown", 150.00, "Delayed")
    assert not row["customer_matched"]


def test_orphans_both_directions(run):
    result, _ = run
    assert result["audit"]["orphan_logs"] == ["ORD_9999"]
    assert result["audit"]["orders_without_logs"] == []


def test_imputations(run):
    assert run[0]["audit"]["imputations"] == {"status": ["ORD_1004"]}


def test_slash_dates_and_total_value(run):
    result, _ = run
    assert result["audit"]["slash_format_dates"] == 7
    assert result["master"]["order_value"].sum() == pytest.approx(3545.69)


def test_status_counts(run):
    counts = run[0]["master"]["status"].value_counts().to_dict()
    assert counts == {
        "Failed Delivery": 8,
        "Delivered": 6,
        "In Transit": 4,
        "Delayed": 2,
        "Pending Verification": 1,
    }


def test_failed_value_by_tier(run):
    failed = run[0]["value_by_tier"]["Failed Delivery"]
    assert failed.sum() == pytest.approx(2390.50)
    assert failed["VIP"] == pytest.approx(1695.50)
    assert failed["Gold"] == pytest.approx(210.00)
    assert failed["Standard"] == pytest.approx(485.00)


def test_value_at_risk(run):
    assert run[0]["value_by_tier"].to_numpy().sum() == pytest.approx(2740.70)


def test_escalations(run):
    esc = run[0]["escalations"]
    assert len(esc) == 7
    assert esc["escalation_priority"].value_counts().to_dict() == {"P1": 5, "P2": 2}
    assert set(esc.loc[esc["escalation_priority"] == "P2", "order_id"]) == {
        "ORD_1003",
        "ORD_1010",
    }
    assert esc.loc[
        esc["escalation_priority"] == "P1", "order_value"
    ].sum() == pytest.approx(1695.50)


def test_low_value_failure_not_escalated(run):
    # ORD_1021: failed, Standard tier, £35 - below the P2 threshold, so no priority.
    master = run[0]["master"].set_index("order_id")
    assert master.loc["ORD_1021", "status"] == "Failed Delivery"
    assert pd.isna(master.loc["ORD_1021", "escalation_priority"])
    assert "ORD_1021" not in set(run[0]["escalations"]["order_id"])


def test_dpd_failures(run):
    assert run[0]["status_by_carrier"].loc["DPD", "Failed Delivery"] == 5


def test_failed_by_hub(run):
    hubs = run[0]["failed_by_hub"]["Failed Delivery"]
    assert hubs.sum() == 8
    assert hubs.index[0] == "Manchester Hub" and hubs["Manchester Hub"] == 4


# ---------------------------------------------------------------- specific fixes
def test_dates_are_day_first_not_month_first(run):
    dates = run[0]["master"].set_index("order_id")["order_date"]
    assert dates["ORD_1002"] == "2026-10-01"  # 01/10/2026, not 10 January
    assert dates["ORD_1020"] == "2026-10-13"


def test_tier_labels_canonical(run):
    tiers = run[0]["master"].set_index("customer_id")["account_tier"]
    assert tiers["CUST_501"].iloc[0] == "VIP"  # .str.title() would give "Vip"
    assert tiers["CUST_503"].iloc[0] == "Gold"
    assert tiers["CUST_504"].iloc[0] == "Standard"  # stored as " standard "
    assert set(tiers) == {"VIP", "Gold", "Standard", "Unknown"}


def test_postcodes_normalised(run):
    postcodes = run[0]["master"]["shipping_postcode"]
    assert (postcodes == postcodes.str.strip().str.upper()).all()


def test_hash_normalises_and_is_keyed():
    assert p.hash_email(" ALICE.SMITH@Email.COM ", "k") == p.hash_email(
        "alice.smith@email.com", "k"
    )
    assert p.hash_email("alice.smith@email.com", "k") != p.hash_email(
        "alice.smith@email.com", "other"
    )


def test_no_personal_data_in_outputs(run):
    _, out = run
    master_cols = set(pd.read_csv(out / p.MASTER_FILE).columns)
    assert not {"email", "phone"} & master_cols
    payload = json.loads((out / p.PAYLOAD_FILE).read_text(encoding="utf-8"))
    assert payload["escalation_count"] == 7
    for record in payload["escalations"]:
        assert not {"email", "email_hash", "phone", "full_name"} & set(record)
        assert record["delivery_notes"] and record["last_checkpoint"]
    for text in [
        (out / f).read_text(encoding="utf-8") for f in (p.MASTER_FILE, p.PAYLOAD_FILE)
    ]:
        assert "@" not in text and "07700" not in text


def test_outputs_and_audit_written(run):
    _, out = run
    for name in run[0]["audit"]["outputs"]:
        assert (out / name).exists(), name
    entry = read_audit(out)[-1]
    assert entry["status"] == "success" and entry["escalations"] == {
        "total": 7,
        "P1": 5,
        "P2": 2,
    }


# ---------------------------------------------------------------- failure paths
def test_missing_column_stops_run_and_is_audited(sandbox):
    df = pd.read_csv(sandbox / p.ORDERS_FILE, dtype=str).drop(columns=["order_value"])
    df.to_csv(sandbox / p.ORDERS_FILE, index=False)
    with pytest.raises(p.PipelineError, match="missing required columns"):
        p.run_pipeline(sandbox, sandbox, verbose=False)
    entry = read_audit(sandbox)[-1]
    assert entry["status"] == "error" and entry["stage"] == "extract"
    assert not (sandbox / p.PAYLOAD_FILE).exists()


def test_duplicate_log_key_rejected(sandbox):
    logs = json.loads((sandbox / p.LOGS_FILE).read_text(encoding="utf-8"))
    logs.append(dict(logs[0]))
    (sandbox / p.LOGS_FILE).write_text(json.dumps(logs), encoding="utf-8")
    with pytest.raises(p.PipelineError, match="duplicate order_id"):
        p.run_pipeline(sandbox, sandbox, verbose=False)


def test_blank_customer_id_rejected(sandbox):
    df = pd.read_csv(sandbox / p.ORDERS_FILE, dtype=str)
    df.loc[0, "customer_id"] = "   "
    df.to_csv(sandbox / p.ORDERS_FILE, index=False)
    with pytest.raises(p.PipelineError, match="blank customer_id"):
        p.run_pipeline(sandbox, sandbox, verbose=False)


def test_unparseable_date_rejected(sandbox):
    df = pd.read_csv(sandbox / p.ORDERS_FILE, dtype=str)
    df.loc[0, "order_date"] = "31/02/2026"
    df.to_csv(sandbox / p.ORDERS_FILE, index=False)
    with pytest.raises(p.PipelineError, match="unparseable order_date"):
        p.run_pipeline(sandbox, sandbox, verbose=False)


def test_blank_tier_rejected(sandbox):
    # NOT NULL still allows an empty string, so the pipeline must catch it.
    with sqlite3.connect(sandbox / p.CRM_FILE) as conn:
        conn.execute(
            "UPDATE customers SET account_tier = '' WHERE customer_id = 'CUST_502'"
        )
    conn.close()
    with pytest.raises(p.PipelineError, match="missing or unrecognised account_tier"):
        p.run_pipeline(sandbox, sandbox, verbose=False)


def test_null_tier_blocked_by_crm_schema(sandbox):
    with sqlite3.connect(sandbox / p.CRM_FILE) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
            conn.execute(
                "UPDATE customers SET account_tier = NULL WHERE customer_id = 'CUST_502'"
            )
    conn.close()


def test_unrecognised_tier_rejected(sandbox):
    with sqlite3.connect(sandbox / p.CRM_FILE) as conn:
        conn.execute(
            "UPDATE customers SET account_tier = 'Platinum' WHERE customer_id = 'CUST_502'"
        )
    conn.close()  # the context manager commits but does not close (matters on Windows)
    with pytest.raises(p.PipelineError, match="unrecognised account_tier"):
        p.run_pipeline(sandbox, sandbox, verbose=False)


def test_missing_crm_database(sandbox):
    (sandbox / p.CRM_FILE).unlink()
    with pytest.raises(p.PipelineError, match="CRM database not found"):
        p.run_pipeline(sandbox, sandbox, verbose=False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-p", "no:cacheprovider"]))
