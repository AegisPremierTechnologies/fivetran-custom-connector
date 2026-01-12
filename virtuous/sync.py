"""Sync logic for Virtuous connector with pagination."""

from typing import Optional

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api import query_gifts, query_contacts
from models import (
    format_gift,
    format_contact,
    format_individual,
    format_address,
    format_contact_method,
)


# Pagination configuration
PAGE_SIZE = 1000  # Max allowed by Virtuous API


def sync_gifts(
    configuration: dict,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
):
    """Sync all gifts with pagination. Yields upsert operations.

    Args:
        configuration: Connector configuration
        modified_since: ISO datetime string for incremental sync start (None for full sync)
        modified_until: ISO datetime string for incremental sync end (debug mode)
    """
    skip = 0
    total_synced = 0

    while True:
        response = query_gifts(
            configuration,
            skip=skip,
            take=PAGE_SIZE,
            modified_since=modified_since,
            modified_until=modified_until,
        )

        # Handle response structure - may be {"list": [...]} or just [...]
        gifts = (
            response.get("list", response) if isinstance(response, dict) else response
        )

        if not gifts:
            log.info(f"No more gifts to sync. Total synced: {total_synced}")
            break

        for i, gift in enumerate(gifts):
            # Log first record for debugging
            yield op.upsert(
                table="gifts",
                data=format_gift(gift, debug=(total_synced == 0 and i == 0)),
            )
            total_synced += 1

        log.info(f"Synced {len(gifts)} gifts (total: {total_synced})")

        # Check if we've reached the end
        if len(gifts) < PAGE_SIZE:
            log.info(f"Reached end of gifts. Total synced: {total_synced}")
            break

        skip += PAGE_SIZE


def sync_contacts(
    configuration: dict,
    modified_since: Optional[str] = None,
    modified_until: Optional[str] = None,
):
    """Sync all contacts and related entities with pagination.

    Yields upsert operations for:
    - contacts: Main contact record
    - individuals: People within the contact (from contactIndividuals)
    - addresses: Contact addresses (from address field)
    - contact_methods: Phone, email, etc. (from individual.contactMethods)

    Args:
        configuration: Connector configuration
        modified_since: ISO datetime string for incremental sync start (None for full sync)
        modified_until: ISO datetime string for incremental sync end (debug mode)
    """
    skip = 0
    total_contacts = 0
    total_individuals = 0
    total_addresses = 0
    total_methods = 0

    while True:
        response = query_contacts(
            configuration,
            skip=skip,
            take=PAGE_SIZE,
            modified_since=modified_since,
            modified_until=modified_until,
        )

        # Handle response structure - may be {"list": [...]} or just [...]
        contacts = (
            response.get("list", response) if isinstance(response, dict) else response
        )

        if not contacts:
            log.info(f"No more contacts to sync. Total synced: {total_contacts}")
            break

        for i, contact in enumerate(contacts):
            contact_id = str(contact.get("id", ""))
            debug = total_contacts == 0 and i == 0

            # 1. Yield the main contact record
            yield op.upsert(
                table="contacts",
                data=format_contact(contact, debug=debug),
            )
            total_contacts += 1

            # 2. Yield address(es) - API returns single "address" object
            address = contact.get("address")
            if address and address.get("id"):
                yield op.upsert(
                    table="addresses",
                    data=format_address(address, contact_id),
                )
                total_addresses += 1

            # 3. Yield individuals and their contact methods
            individuals = contact.get("contactIndividuals", [])
            for individual in individuals:
                individual_id = str(individual.get("id", ""))

                yield op.upsert(
                    table="individuals",
                    data=format_individual(individual, contact_id),
                )
                total_individuals += 1

                # 4. Yield contact methods for this individual
                methods = individual.get("contactMethods", [])
                for method in methods:
                    yield op.upsert(
                        table="contact_methods",
                        data=format_contact_method(method, contact_id, individual_id),
                    )
                    total_methods += 1

        log.info(
            f"Synced batch: {len(contacts)} contacts, "
            f"{total_individuals} individuals, {total_addresses} addresses, "
            f"{total_methods} contact methods"
        )

        # Check if we've reached the end
        if len(contacts) < PAGE_SIZE:
            log.info(
                f"Reached end. Totals: {total_contacts} contacts, "
                f"{total_individuals} individuals, {total_addresses} addresses, "
                f"{total_methods} contact methods"
            )
            break

        skip += PAGE_SIZE
