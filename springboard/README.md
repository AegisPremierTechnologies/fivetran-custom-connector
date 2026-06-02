# Springboard MongoDB Connector

Syncs nonprofit fundraising data from a Springboard (Jackson River) MongoDB data warehouse into Fivetran. The source database is `kqed-staging` and contains 12 `sb_*` collections.

The connector supports two sync modes:
- **Historical sync**: walks each collection by `_id` order with checkpoints every 1000 documents
- **Incremental sync**: queries `dw_updated_at.date > last_sync_time` per collection

## Collections

| Table | Description |
|---|---|
| `sb_action` | User actions (currently empty) |
| `sb_cart_abandonment` | Abandoned donation cart records |
| `sb_contact` | Donor/contact profiles |
| `sb_donation` | Donation transaction records (primary transaction table) |
| `sb_event_log` | System events (sustainer lifecycle, payment events) |
| `sb_form` | Fundraising form definitions and configurations |
| `sb_line_item` | Donation line items (premiums, products) |
| `sb_message` | Messages (currently empty) |
| `sb_payment` | Payment transaction records (linked to donations) |
| `sb_submission` | Form submission raw data |
| `sb_sync_failure_counts` | Internal sync failure tracking (no incremental, full replace) |
| `sb_vendor_mapping` | External system ID mappings (e.g., Salesforce) |

## Table Relationships

`sb_donation` is the central fact table. Most joins radiate outward from it.

```sql
-- Donations to donors
sb_donation.contact_id = sb_contact.contact_id

-- Donations to line items
sb_donation.donation_id = sb_line_item.donation_id

-- Donations to payments
sb_donation.donation_id = sb_payment.donation_id

-- Donations to form submissions
sb_donation.submission_id = sb_submission.submission_id

-- Donations to form definitions
sb_donation.form_id = sb_form.form_id

-- Submissions to form definitions
sb_submission.form_id = sb_form.form_id

-- Submissions to contacts
sb_submission.contact_id = sb_contact.contact_id

-- Vendor mappings to contacts (filter by dw_type)
sb_vendor_mapping.dw_id = sb_contact.contact_id  -- WHERE dw_type = 'Contact'
```

## JSON Columns

Several columns store complex nested data as JSON strings. Parse them with `JSON_PARSE()` (Snowflake), `JSON_EXTRACT()` (BigQuery), or equivalent in your destination.

| Table | JSON Columns |
|---|---|
| `sb_form` | `body`, `field_no_gift_message`, `field_image`, `fr_premiums_premium`, `fr_premiums_bonusgift`, `fr_drives_drive` |
| `sb_event_log` | `event_data` |
| `sb_donation` | `onetime_gift_amounts`, `recurring_gift_amounts` |

## Sync Behavior

| Mode | Mechanism | Checkpoint |
|---|---|---|
| Historical | Walks by `_id` order | Every 1000 documents |
| Incremental | Filters `dw_updated_at.date > last_sync_time` | Per collection |
| `sb_sync_failure_counts` | Full replace (no `dw_updated_at` field) | N/A |

All other collections use `dw_updated_at` for incremental sync. The `sb_sync_failure_counts` table is always fully replaced because it lacks this timestamp field.

## Configuration

The connector requires a single secret:

| Key | Description |
|---|---|
| `connection_string` | MongoDB connection URI for the `kqed-staging` database |
