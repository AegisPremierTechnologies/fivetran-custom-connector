import json


def _extract_supporter_row(s: dict) -> dict:
    account = s.get("account") or {}
    return {
        "id": s.get("id"),
        "created_at": s.get("created_at"),
        "livemode": s.get("livemode"),
        "email": s.get("email"),
        "first_name": s.get("first_name"),
        "last_name": s.get("last_name"),
        "phone": s.get("phone"),
        "title": s.get("title"),
        "language": s.get("language"),
        "account_id": account.get("id"),
        "account_code": account.get("code"),
        "account_name": account.get("name"),
        "address": json.dumps(s.get("address")) if s.get("address") else None,
        "employer": json.dumps(s.get("employer")) if s.get("employer") else None,
    }


def _extract_donation_row(d: dict) -> dict:
    supporter = d.get("supporter") or {}
    campaign = d.get("campaign") or {}
    designation = d.get("designation") or {}
    recurring_plan = d.get("recurring_plan") or {}
    account = d.get("account") or {}

    return {
        "id": d.get("id"),
        "created_at": d.get("created_at"),
        "livemode": d.get("livemode"),
        "status": d.get("status"),
        "amount": d.get("amount"),
        "amount_in_default_currency": d.get("amount_in_default_currency"),
        "currency": d.get("currency"),
        "installment": d.get("installment"),
        "succeeded_at": d.get("succeeded_at"),
        "failed_at": d.get("failed_at"),
        "refunded_at": d.get("refunded_at"),
        "source": d.get("source"),
        "supporter_id": supporter.get("id"),
        "campaign_id": campaign.get("id"),
        "designation_id": designation.get("id"),
        "recurring_plan_id": recurring_plan.get("id"),
        "account_id": account.get("id"),
        "campaign": json.dumps(d.get("campaign")) if d.get("campaign") else None,
        "designation": (
            json.dumps(d.get("designation")) if d.get("designation") else None
        ),
        "element": json.dumps(d.get("element")) if d.get("element") else None,
        "platform_fee": (
            json.dumps(d.get("platform_fee")) if d.get("platform_fee") else None
        ),
        "processing_fee": (
            json.dumps(d.get("processing_fee")) if d.get("processing_fee") else None
        ),
        "payout": json.dumps(d.get("payout")) if d.get("payout") else None,
        "payment": json.dumps(d.get("payment")) if d.get("payment") else None,
        "device": json.dumps(d.get("device")) if d.get("device") else None,
        "utm": json.dumps(d.get("utm")) if d.get("utm") else None,
        "fundraiser": json.dumps(d.get("fundraiser")) if d.get("fundraiser") else None,
        "tribute": json.dumps(d.get("tribute")) if d.get("tribute") else None,
        "custom_fields": (
            json.dumps(d.get("custom_fields")) if d.get("custom_fields") else None
        ),
        "questions": json.dumps(d.get("questions")) if d.get("questions") else None,
        "consent": json.dumps(d.get("consent")) if d.get("consent") else None,
        "url": d.get("url"),
        "on_behalf_of": d.get("on_behalf_of"),
        "receipt_id": d.get("receipt_id"),
        "anonymous": d.get("anonymous"),
    }


def _extract_recurring_plan_row(r: dict) -> dict:
    supporter = r.get("supporter") or {}
    campaign = r.get("campaign") or {}
    return {
        "id": r.get("id"),
        "created_at": r.get("created_at"),
        "livemode": r.get("livemode"),
        "status": r.get("status"),
        "frequency": r.get("frequency"),
        "amount": r.get("amount"),
        "currency": r.get("currency"),
        "supporter_id": supporter.get("id"),
        "campaign_id": campaign.get("id"),
        "supporter": json.dumps(r.get("supporter")) if r.get("supporter") else None,
        "campaign": json.dumps(r.get("campaign")) if r.get("campaign") else None,
        # Add more fields with conservative mapping as seen necessary
    }


def _extract_event_row(e: dict) -> dict:
    return {
        "account": json.dumps(e.get("account")) if e.get("account") else None,
        "created_at": e.get("created_at"),
        "donation": e.get("donation"),
        "id": e.get("id"),
        "livemode": e.get("livemode"),
        "recurring_plan": e.get("recurring_plan"),
        "supporter": e.get("supporter"),
        "type": e.get("type"),
    }
