from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceSpec:
    prefix: str
    kind: str
    raw_output_name: str
    action_type: str | None = None


SOURCES: tuple[SourceSpec, ...] = (
    SourceSpec(prefix="01 ", kind="xlsx", raw_output_name="01_ls_profile_raw.csv"),
    SourceSpec(prefix="02 ", kind="xlsx", raw_output_name="02_balance_raw.csv"),
    SourceSpec(prefix="03 ", kind="csv", raw_output_name="03_payment_raw.csv"),
    SourceSpec(prefix="04 ", kind="xlsx", raw_output_name="04_autodial_raw.csv", action_type="autodial"),
    SourceSpec(prefix="05 ", kind="xlsx", raw_output_name="05_email_raw.csv", action_type="email"),
    SourceSpec(prefix="06 ", kind="xlsx", raw_output_name="06_sms_raw.csv", action_type="sms"),
    SourceSpec(prefix="07 ", kind="xlsx", raw_output_name="07_operator_call_raw.csv", action_type="operator_call"),
    SourceSpec(prefix="08 ", kind="xlsx", raw_output_name="08_claim_raw.csv", action_type="claim"),
    SourceSpec(prefix="09 ", kind="xlsx", raw_output_name="09_field_visit_raw.csv", action_type="field_visit"),
    SourceSpec(prefix="10 ", kind="xlsx", raw_output_name="10_restriction_notice_raw.csv", action_type="restriction_notice"),
    SourceSpec(prefix="11 ", kind="xlsx", raw_output_name="11_restriction_raw.csv", action_type="restriction"),
    SourceSpec(prefix="12 ", kind="xlsx", raw_output_name="12_court_order_request_raw.csv", action_type="court_order_request"),
    SourceSpec(prefix="13 ", kind="xlsx", raw_output_name="13_court_order_received_raw.csv", action_type="court_order_received"),
    SourceSpec(prefix="14 ", kind="xlsx", raw_output_name="14_action_limits_raw.csv"),
)


ACTION_TYPES: tuple[str, ...] = (
    "autodial",
    "email",
    "sms",
    "operator_call",
    "claim",
    "field_visit",
    "restriction_notice",
    "restriction",
    "court_order_request",
    "court_order_received",
)


ACTION_TYPE_BY_PREFIX: dict[str, str] = {
    src.prefix.strip(): src.action_type
    for src in SOURCES
    if src.action_type is not None
}


RAW_DIRNAME = "raw"
NORMALIZED_DIRNAME = "normalized"
MART_DIRNAME = "mart"


OUTPUT_TABLES_NORMALIZED: tuple[str, ...] = (
    "dim_ls_profile",
    "fact_balance_monthly",
    "fact_payment_txn",
    "fact_action_event",
    "dim_action_limit",
    "dq_issues",
)


OUTPUT_TABLES_MART: tuple[str, ...] = (
    "mart_ls_month",
)
