from typing import Dict, List


def schema(configuration: Dict) -> List[Dict]:
    """
    Minimal yet useful schemas for supporters, donations, and events.
    We keep types broadly compatible (mostly STRING) to avoid brittle mappings initially.
    """
    return [
        {
            "table": "supporters",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "created_at": "STRING",
                "livemode": "BOOLEAN",
                "email": "STRING",
                "first_name": "STRING",
                "last_name": "STRING",
                "phone": "STRING",
                "title": "STRING",
                "language": "STRING",
                # Flatten some useful references
                "account_id": "STRING",
                "account_code": "STRING",
                "account_name": "STRING",
                # Preserve nested objects as JSON for completeness
                "address": "STRING",
                "employer": "STRING",
            },
        },
        {
            "table": "donations",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "created_at": "STRING",
                "livemode": "BOOLEAN",
                "status": "STRING",
                "amount": "STRING",
                "amount_in_default_currency": "STRING",
                "currency": "STRING",
                "installment": "STRING",
                "succeeded_at": "STRING",
                "failed_at": "STRING",
                "refunded_at": "STRING",
                "source": "STRING",
                # References
                "supporter_id": "STRING",
                "campaign_id": "STRING",
                "designation_id": "STRING",
                "recurring_plan_id": "STRING",
                "account_id": "STRING",
                # Nested blobs
                "supporter": "STRING",
                "campaign": "STRING",
                "designation": "STRING",
                "element": "STRING",
                "platform_fee": "STRING",
                "processing_fee": "STRING",
                "payout": "STRING",
                "payment": "STRING",
                "device": "STRING",
                "utm": "STRING",
                "fundraiser": "STRING",
                "tribute": "STRING",
                "custom_fields": "STRING",
                "questions": "STRING",
                "consent": "STRING",
                "url": "STRING",
                "on_behalf_of": "STRING",
                "receipt_id": "STRING",
                "anonymous": "BOOLEAN",
            },
        },
        {
            "table": "events",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "created_at": "STRING",
                "type": "STRING",
                "livemode": "BOOLEAN",
                # Related object IDs
                "donation": "STRING",
                "recurring_plan": "STRING",
                "supporter": "STRING",
                # Account info (flatten a bit)
                "account_id": "STRING",
                "account_code": "STRING",
                "account_name": "STRING",
            },
        },
    ]
