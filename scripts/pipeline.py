# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "pandas>=3.0.6",
#     "matplotlib>=3.10",
# ]
# ///
"""Failed-delivery escalation pipeline.

Extracts orders (CSV), customers (SQLite) and delivery logs (JSON), cleans and
merges them into one master dataset, draws two charts, and exports a JSON
payload of high-priority escalations for Power Automate. Every run appends one
line to pipeline_audit.jsonl, whether it succeeds or fails.

Reads from ../input, writes to ../output.

Run:  uv run scripts/pipeline.py
"""

import hashlib
import hmac
import json
import sqlite3
import sys
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_DIR / "input"
OUTPUT_DIR = PROJECT_DIR / "output"

ORDERS_FILE = "orders.csv"
CRM_FILE = "crm_database.db"
LOGS_FILE = "delivery_logs.json"

MASTER_FILE = "combined_master_dataset.csv"
PAYLOAD_FILE = "high_priority_escalations.json"
PAYLOAD_CSV_FILE = "high_priority_escalations.csv"
AUDIT_FILE = "pipeline_audit.jsonl"
CHART_TIER_FILE = "chart1_value_at_risk_by_tier.png"
CHART_CARRIER_FILE = "chart2_status_by_carrier.png"

REQUIRED_COLUMNS = {
    "orders": {
        "order_id",
        "customer_id",
        "order_date",
        "order_value",
        "shipping_postcode",
    },
    "customers": {"customer_id", "full_name", "email", "account_tier"},
    "logs": {"order_id", "carrier", "status", "last_checkpoint", "delivery_notes"},
}

# Data minimisation: only the CRM columns the pipeline needs; phone is never read.
CRM_QUERY = "SELECT customer_id, full_name, email, account_tier FROM customers"

# json_normalize names nested fields by their dotted path; shortened for the flat outputs.
JSON_FLATTEN_RENAME = {
    "tracking_details.last_checkpoint": "last_checkpoint",
    "tracking_details.delivery_notes": "delivery_notes",
}

TIER_MAP = {"vip": "VIP", "gold": "Gold", "standard": "Standard"}
TIER_ORDER = ["VIP", "Gold", "Standard", "Unknown"]
UNKNOWN_TIER = "Unknown"
UNKNOWN_NAME = "Unknown Customer"

DELIVERED = "Delivered"
IN_TRANSIT = "In Transit"
FAILED = "Failed Delivery"
DELAYED = "Delayed"
PENDING = "Pending Verification"
NO_TRACKING = "No Tracking Record"
VALID_STATUSES = {DELIVERED, IN_TRANSIT, FAILED, DELAYED, PENDING}
AT_RISK_STATUSES = [FAILED, DELAYED, PENDING]

P2_VALUE_THRESHOLD = 100.00

# Secret key for the keyed email hash (HMAC). Hard-coded for this proof of concept;
# production would load it from a secrets vault such as Azure Key Vault, stored
# separately from the data as ICO pseudonymisation guidance recommends.
HASH_KEY = "demo-key"


class PipelineError(Exception):
    """Raised when input data fails a validation check; carries the stage for the audit log.

    Python convention is to raise an exception for bad external data; assert is
    reserved for internal "this should be impossible" checks.
    """

    def __init__(self, stage, message):
        super().__init__(f"[{stage}] {message}")
        self.stage = stage


def check(condition, stage, message):
    """Guard clause: stop the run with a PipelineError if the condition is False."""
    if not condition:
        raise PipelineError(stage, message)


# =====================================================================
# STAGE 1: EXTRACT AND VALIDATE
# =====================================================================
def check_not_blank(df, source, column):
    check(
        df[column].notna().all() and (df[column].str.strip() != "").all(),
        "extract",
        f"{source}: blank {column} values found",
    )


def validate_schema(df, source, key):
    missing = REQUIRED_COLUMNS[source] - set(df.columns)
    check(
        not missing, "extract", f"{source}: missing required columns {sorted(missing)}"
    )
    check_not_blank(df, source, key)
    dupes = df.loc[df[key].duplicated(), key].tolist()
    check(not dupes, "extract", f"{source}: duplicate {key} values {dupes}")


def extract(input_dir):
    input_dir = Path(input_dir)
    try:
        # dtype=str stops pandas guessing types, so cleaning always starts from text.
        orders = pd.read_csv(input_dir / ORDERS_FILE, dtype=str)

        crm_path = input_dir / CRM_FILE
        # sqlite3.connect() on a missing file silently creates an empty database.
        check(crm_path.exists(), "extract", f"CRM database not found: {crm_path}")

        # closing() guarantees conn.close() runs (sqlite3's own "with" only commits).
        with closing(sqlite3.connect(crm_path)) as conn:
            customers = pd.read_sql_query(CRM_QUERY, conn)

        with open(input_dir / LOGS_FILE, encoding="utf-8") as f:
            raw_logs = json.load(f)
        logs = pd.json_normalize(raw_logs).rename(columns=JSON_FLATTEN_RENAME)
    except PipelineError:
        raise
    except Exception as exc:
        raise PipelineError("extract", f"{type(exc).__name__}: {exc}") from exc

    validate_schema(orders, "orders", "order_id")
    validate_schema(customers, "customers", "customer_id")
    validate_schema(logs, "logs", "order_id")
    check_not_blank(orders, "orders", "customer_id")

    return orders, customers, logs


# =====================================================================
# STAGE 2: CLEAN AND NORMALISE
# =====================================================================
def parse_dates(series):
    """Parse ISO (YYYY-MM-DD) and UK (DD/MM/YYYY) dates in two explicit passes.

    format="mixed" guesses month-first, so 01/10/2026 would become 10 January.
    Fixing each format removes the guesswork.
    """
    text = series.str.strip()
    iso = pd.to_datetime(text, format="%Y-%m-%d", errors="coerce")
    uk = pd.to_datetime(text, format="%d/%m/%Y", errors="coerce")
    parsed = iso.fillna(uk)
    bad = series[parsed.isna()].tolist()
    check(not bad, "clean", f"unparseable order_date values {bad}")
    return parsed, int(uk.notna().sum())


def hash_email(email, key):
    """Normalise first so ' ALICE@X.COM ' and 'alice@x.com' produce the same token.

    A keyed HMAC (not plain SHA-256) stops anyone without the key from
    reversing the token by hashing guessed addresses.
    """
    if pd.isna(email):
        return None
    normalised = str(email).strip().lower()
    # HMAC = SHA-256 combined with a secret key; hexdigest() returns it as 64 hex characters.
    return hmac.new(key.encode(), normalised.encode(), hashlib.sha256).hexdigest()


def clean_orders(orders):
    df = orders.copy()
    df["order_id"] = df["order_id"].str.strip()
    df["customer_id"] = df["customer_id"].str.strip()

    value_text = (
        df["order_value"]
        .str.replace("£", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    df["order_value"] = pd.to_numeric(value_text, errors="coerce")
    bad_values = orders.loc[df["order_value"].isna(), "order_value"].tolist()
    check(not bad_values, "clean", f"non-numeric order_value values {bad_values}")

    # \s+ = any run of whitespace (spaces, tabs), collapsed to a single space.
    df["shipping_postcode"] = (
        df["shipping_postcode"]
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
    )

    df["order_date"], slash_dates = parse_dates(df["order_date"])
    return df, slash_dates


def clean_customers(customers):
    df = customers.copy()
    df["customer_id"] = df["customer_id"].str.strip()

    # Lookup table maps any casing to the official label; missing or unknown values become NaN.
    # account_tier is NOT NULL in the CRM, so a gap here is a data error.
    df["account_tier"] = df["account_tier"].str.strip().str.lower().map(TIER_MAP)
    bad_tiers = customers.loc[df["account_tier"].isna(), "account_tier"].tolist()
    check(
        not bad_tiers,
        "clean",
        f"missing or unrecognised account_tier values {bad_tiers}",
    )

    df["email_hash"] = df["email"].map(lambda e: hash_email(e, HASH_KEY))
    # Data minimisation: the raw email never leaves this stage.
    df = df.drop(columns=["email"])
    return df


def clean_logs(logs):
    df = logs.copy()
    df["order_id"] = df["order_id"].str.strip()
    df["carrier"] = df["carrier"].str.strip()
    df["status"] = df["status"].str.strip()
    imputed_ids = df.loc[
        df["status"].isna() | (df["status"] == ""), "order_id"
    ].tolist()
    df.loc[df["order_id"].isin(imputed_ids), "status"] = PENDING
    unexpected = sorted(set(df["status"]) - VALID_STATUSES)
    check(not unexpected, "clean", f"unexpected delivery status values {unexpected}")
    return df, imputed_ids


def clean(orders, customers, logs):
    orders_c, slash_dates = clean_orders(orders)
    customers_c = clean_customers(customers)
    logs_c, imputed_statuses = clean_logs(logs)

    check(
        orders_c.notna().all().all(),
        "clean",
        "orders still contain missing values after cleaning",
    )
    check(
        customers_c.notna().all().all(),
        "clean",
        "customers still contain missing values after cleaning",
    )
    check(
        logs_c.notna().all().all(),
        "clean",
        "delivery logs still contain missing values after cleaning",
    )

    stats = {
        "slash_format_dates": slash_dates,
        "imputed_status_orders": imputed_statuses,
    }
    return orders_c, customers_c, logs_c, stats


# =====================================================================
# STAGE 3: RELATIONAL MERGE
# =====================================================================
def merge(orders, customers, logs):
    # Orphans in both directions: a left join from orders would silently drop ORD_9999.
    orphan_logs = sorted(set(logs["order_id"]) - set(orders["order_id"]))
    orders_without_logs = sorted(set(orders["order_id"]) - set(logs["order_id"]))

    try:
        # validate= makes pandas raise if a key is unexpectedly duplicated (row fan-out).
        merged = orders.merge(
            logs,
            on="order_id",
            how="left",
            validate="one_to_one",
            indicator="log_match",
        )
        merged = merged.merge(
            customers,
            on="customer_id",
            how="left",
            validate="many_to_one",
            indicator="crm_match",
        )
    except pd.errors.MergeError as exc:
        raise PipelineError("merge", str(exc)) from exc

    # Audit control: a left join with validate= should never change the row count.
    check(
        len(merged) == len(orders),
        "merge",
        f"expected {len(orders)} rows after merge, got {len(merged)}",
    )

    # indicator adds a column saying where each row came from: "both" (matched) or
    # "left_only" (no match). Safer than isna(), as a real customer could have a blank field.
    merged["customer_matched"] = merged.pop("crm_match").eq("both")
    merged["tracking_matched"] = merged.pop("log_match").eq("both")

    # .loc[rows, columns] = values; ~ means NOT, so this targets the unmatched rows only.
    merged.loc[~merged["customer_matched"], ["full_name", "account_tier"]] = [
        UNKNOWN_NAME,
        UNKNOWN_TIER,
    ]
    merged.loc[
        ~merged["tracking_matched"],
        ["carrier", "status", "last_checkpoint", "delivery_notes"],
    ] = ["Unknown", NO_TRACKING, "Unknown", "No delivery log received"]

    unmapped = merged.loc[
        ~merged["customer_matched"], ["customer_id", "order_id"]
    ].to_dict("records")
    stats = {
        "rows_merged": len(merged),
        "unmapped_customers": unmapped,
        "orphan_logs": orphan_logs,
        "orders_without_logs": orders_without_logs,
    }
    return merged, stats


# =====================================================================
# STAGE 4: AGGREGATE AND VISUALISE
# =====================================================================
STATUS_COLOURS = {
    FAILED: "#c0392b",
    DELAYED: "#f0a35e",
    PENDING: "#f7d58b",
    IN_TRANSIT: "#5dade2",
    DELIVERED: "#58b368",
    NO_TRACKING: "#aab7b8",
}


def money(v):
    return f"£{v:,.2f}"


def count(v):
    return f"{int(v)}"


def stacked_bar(ax, table, value_fmt):
    """Draw one bar per table row, with one coloured layer per column stacked on top."""
    # bottom = where the next layer starts on each bar; every bar begins at 0.
    bottom = pd.Series(0.0, index=table.index)
    for status in table.columns:
        values = table[status]
        bars = ax.bar(
            table.index,
            values,
            bottom=bottom,
            label=status,
            color=STATUS_COLOURS[status],
            edgecolor="white",
            linewidth=0.8,
        )
        ax.bar_label(
            bars,
            labels=[value_fmt(v) if v > 0 else "" for v in values],
            label_type="center",
            fontsize=8,
        )
        bottom += values  # the next layer starts on top of this one
    # bottom now holds each bar's total; label it just above the top layer.
    ax.bar_label(
        bars, labels=[value_fmt(v) for v in bottom], padding=3, fontweight="bold"
    )
    ax.set_ylim(0, bottom.max() * 1.12)  # headroom so the total labels clear the title


def aggregate(merged):
    at_risk = merged[merged["status"].isin(AT_RISK_STATUSES)]
    value_by_tier = at_risk.pivot_table(
        index="account_tier",
        columns="status",
        values="order_value",
        aggfunc="sum",
        fill_value=0.0,
    ).reindex(index=TIER_ORDER, columns=AT_RISK_STATUSES, fill_value=0.0)

    status_order = [s for s in STATUS_COLOURS if s in set(merged["status"])]
    status_by_carrier = pd.crosstab(merged["carrier"], merged["status"]).reindex(
        columns=status_order, fill_value=0
    )
    status_by_carrier = status_by_carrier.loc[
        status_by_carrier.sum(axis=1).sort_values(ascending=False).index
    ]
    return value_by_tier, status_by_carrier


def draw_chart(table, value_fmt, title, xlabel, ylabel, path):
    fig, ax = plt.subplots(figsize=(8, 5))  # fig = whole image, ax = the plot area
    stacked_bar(ax, table, value_fmt)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    # Legend just outside the right edge of the plot so it never covers bars.
    ax.legend(title="Delivery status", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    fig.savefig(path, dpi=150)  # 8x5 inches at 150 dpi = 1200x750 pixels
    plt.close(fig)
    return path


def visualise(value_by_tier, status_by_carrier, out_dir):
    plt.style.use("seaborn-v0_8-whitegrid")
    out_dir = Path(out_dir)
    return [
        draw_chart(
            value_by_tier,
            money,
            "Chart 1: Order value at risk by customer tier",
            "Account tier",
            "Order value (£)",
            out_dir / CHART_TIER_FILE,
        ),
        draw_chart(
            status_by_carrier,
            count,
            "Chart 2: Delivery status counts by carrier",
            "Carrier",
            "Number of orders",
            out_dir / CHART_CARRIER_FILE,
        ),
    ]


# =====================================================================
# STAGE 5: ESCALATE, EXPORT AND AUDIT
# =====================================================================
def assign_priority(merged):
    """Return "P1", "P2" or NA for each order.

    P1: failed delivery for a VIP customer, at any order value.
    P2: failed delivery for any other customer (including Unknown) worth >= £100.
    """
    failed = merged["status"].eq(FAILED)
    vip = merged["account_tier"].eq("VIP")
    p1 = failed & vip
    p2 = failed & ~vip & merged["order_value"].ge(P2_VALUE_THRESHOLD)
    priority = pd.Series(pd.NA, index=merged.index, dtype="string")
    priority[p1] = "P1"
    priority[p2] = "P2"
    return priority


MASTER_COLUMNS = [
    "order_id",
    "order_date",
    "order_value",
    "shipping_postcode",
    "customer_id",
    "customer_matched",
    "full_name",
    "account_tier",
    "email_hash",
    "carrier",
    "status",
    "last_checkpoint",
    "delivery_notes",
    "tracking_matched",
    "escalation_priority",
]

# The payload identifies customers only by customer_id and email_hash; the flow looks up
# contact details inside the M365 tenant, so no personal data travels in the file.
PAYLOAD_COLUMNS = [
    "order_id",
    "escalation_priority",
    "customer_id",
    "email_hash",
    "account_tier",
    "customer_matched",
    "order_date",
    "order_value",
    "carrier",
    "status",
    "last_checkpoint",
    "delivery_notes",
]


def export(merged, out_dir, run_id, generated_at):
    out_dir = Path(out_dir)
    master = merged.copy()
    master["escalation_priority"] = assign_priority(master)
    master["order_date"] = master["order_date"].dt.strftime("%Y-%m-%d")
    master["order_value"] = master["order_value"].round(2)
    master = master[MASTER_COLUMNS]
    master.to_csv(out_dir / MASTER_FILE, index=False)

    escalations = master[master["escalation_priority"].notna()].sort_values(
        ["escalation_priority", "order_value"], ascending=[True, False]
    )[PAYLOAD_COLUMNS]
    records = escalations.to_dict("records")
    payload = {
        "run_id": run_id,
        "generated_at": generated_at,
        "escalation_count": len(records),
        "escalations": records,
    }
    with open(out_dir / PAYLOAD_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    escalations.to_csv(out_dir / PAYLOAD_CSV_FILE, index=False)

    return master, escalations


def write_audit(out_dir, entry):
    with open(Path(out_dir) / AUDIT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


# =====================================================================
# ORCHESTRATION
# =====================================================================
def print_stage(title):
    print(f"\n{'=' * 64}\n{title}\n{'=' * 64}")


def run_pipeline(input_dir=INPUT_DIR, out_dir=OUTPUT_DIR, verbose=True):
    input_dir = Path(input_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    say = print if verbose else (lambda *a, **k: None)

    run_id = uuid.uuid4().hex[:8]
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    audit = {"run_id": run_id, "timestamp": started, "status": "running"}

    try:
        if verbose:
            print_stage("STAGE 1: EXTRACT AND VALIDATE")
        orders, customers, logs = extract(input_dir)
        audit["rows_in"] = {
            "orders": len(orders),
            "customers": len(customers),
            "logs": len(logs),
        }
        say(
            f"Rows read  orders={len(orders)}  customers={len(customers)}  logs={len(logs)}"
        )
        say("Schema checks passed (required columns, non-blank and unique keys)")
        say("CRM query reads only required columns (phone never extracted)")
        say("JSON tracking_details flattened by json_normalize:")
        for nested, flat in JSON_FLATTEN_RENAME.items():
            say(f"  {nested} -> {flat}")

        if verbose:
            print_stage("STAGE 2: CLEAN AND NORMALISE")
        orders_c, customers_c, logs_c, clean_stats = clean(orders, customers, logs)
        say(
            f"DD/MM/YYYY dates parsed explicitly: {clean_stats['slash_format_dates']} of {len(orders_c)}"
        )
        say(f"Status imputed to '{PENDING}': {clean_stats['imputed_status_orders']}")
        say("Account tiers normalised to VIP / Gold / Standard (all recognised)")
        say("Emails normalised and HMAC-SHA-256 hashed; raw email dropped")

        if verbose:
            print_stage("STAGE 3: RELATIONAL MERGE")
        merged, merge_stats = merge(orders_c, customers_c, logs_c)
        say(f"Merged rows: {merge_stats['rows_merged']} (orders in: {len(orders_c)})")
        say(f"Unmapped customers: {merge_stats['unmapped_customers']}")
        say(f"Orphan delivery logs (no matching order): {merge_stats['orphan_logs']}")
        say(f"Orders without a delivery log: {merge_stats['orders_without_logs']}")

        if verbose:
            print_stage("STAGE 4: AGGREGATE AND VISUALISE")
        value_by_tier, status_by_carrier = aggregate(merged)
        chart_paths = visualise(value_by_tier, status_by_carrier, out_dir)
        say("Value at risk by tier (GBP):")
        say(value_by_tier.assign(Total=value_by_tier.sum(axis=1)).round(2).to_string())
        say("\nStatus counts by carrier:")
        say(status_by_carrier.to_string())
        say(f"\nCharts saved: {[p.name for p in chart_paths]}")

        if verbose:
            print_stage("STAGE 5: ESCALATE, EXPORT AND AUDIT")
        master, escalations = export(merged, out_dir, run_id, started)
        priority_counts = escalations["escalation_priority"].value_counts().to_dict()
        say(f"Escalations: {len(escalations)}  {priority_counts}")
        say(
            escalations[
                [
                    "order_id",
                    "escalation_priority",
                    "account_tier",
                    "order_value",
                    "carrier",
                    "delivery_notes",
                ]
            ].to_string(index=False)
        )
        say(f"\nOutputs: {MASTER_FILE}, {PAYLOAD_FILE}, {PAYLOAD_CSV_FILE}")

        audit.update(
            {
                "status": "success",
                "rows_merged": merge_stats["rows_merged"],
                "unmapped_customers": merge_stats["unmapped_customers"],
                "orphan_logs": merge_stats["orphan_logs"],
                "orders_without_logs": merge_stats["orders_without_logs"],
                "slash_format_dates": clean_stats["slash_format_dates"],
                "imputations": {"status": clean_stats["imputed_status_orders"]},
                "total_order_value": round(float(master["order_value"].sum()), 2),
                "escalations": {"total": len(escalations), **priority_counts},
                "outputs": [
                    MASTER_FILE,
                    PAYLOAD_FILE,
                    PAYLOAD_CSV_FILE,
                    CHART_TIER_FILE,
                    CHART_CARRIER_FILE,
                ],
            }
        )
        return {
            "master": master,
            "escalations": escalations,
            "value_by_tier": value_by_tier,
            "status_by_carrier": status_by_carrier,
            "audit": audit,
        }

    except PipelineError as exc:
        audit.update({"status": "error", "stage": exc.stage, "error": str(exc)})
        raise
    except Exception as exc:
        audit.update(
            {
                "status": "error",
                "stage": "unexpected",
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        raise
    finally:
        audit["finished"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        write_audit(out_dir, audit)
        if verbose:
            print(
                f"\nAudit entry appended to {AUDIT_FILE} (run {run_id}, status={audit['status']})"
            )


def main():
    try:
        run_pipeline()
    except Exception as exc:
        print(f"\nPIPELINE FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
