"""
Lead Extraction Engine -- Base Ingestor.
Abstract base class for all data source ingestors.
"""

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime

from database import get_engine_connection

logger = logging.getLogger(__name__)


class BaseIngestor(ABC):

    @property
    @abstractmethod
    def source_name(self) -> str:
        ...

    @abstractmethod
    def extract(self) -> tuple[list[dict], list[dict]]:
        ...

    def run(self) -> dict:
        run_id = self._start_run()
        logger.info("=" * 60)
        logger.info("Starting extraction: %s", self.source_name)
        logger.info("=" * 60)
        try:
            companies, contacts = self.extract()
            inserted_companies, source_id_to_raw_id = self._store_companies(companies)
            self._link_contacts_to_raw_companies(contacts, source_id_to_raw_id)
            inserted_contacts = self._store_contacts(contacts)
            summary = {
                "source": self.source_name,
                "companies_found": len(companies),
                "contacts_found": len(contacts),
                "companies_new": inserted_companies,
                "contacts_new": inserted_contacts,
                "status": "success",
            }
            self._complete_run(run_id, summary)
            logger.info(
                "Extraction complete: %d companies (%d inserted), %d contacts (%d inserted)",
                len(companies), inserted_companies, len(contacts), inserted_contacts,
            )
            return summary
        except Exception as e:
            logger.error("Extraction failed for %s: %s", self.source_name, e)
            self._fail_run(run_id, str(e))
            return {"source": self.source_name, "status": "failed", "error": str(e)}

    def _link_contacts_to_raw_companies(self, contacts, source_id_to_raw_id):
        """Resolve raw_company_id from a contact's optional `_co_key` (= a company's source_id).

        Ingestors that link contacts to companies set `_co_key` on each contact dict;
        ingestors that already populate `raw_company_id`, or that match downstream by
        email-domain (e.g. RMD), are unaffected.
        """
        if not source_id_to_raw_id:
            return
        for contact in contacts:
            if contact.get("raw_company_id"):
                continue
            key = contact.get("_co_key")
            if key and key in source_id_to_raw_id:
                contact["raw_company_id"] = source_id_to_raw_id[key]

    def _store_companies(self, companies: list[dict]) -> tuple[int, dict]:
        if not companies:
            return 0, {}
        inserted = 0
        source_id_to_raw_id: dict = {}
        with get_engine_connection() as conn:
            cursor = conn.cursor()
            for company in companies:
                try:
                    cursor.execute("SAVEPOINT row_sp")
                    cursor.execute(
                        """INSERT INTO raw_companies
                            (company_name, company_domain, company_type, company_size,
                             country, state, address, about, services, tech_stack,
                             source, source_id, website_url, raw_data)
                        VALUES (%(company_name)s, %(company_domain)s, %(company_type)s,
                             %(company_size)s, %(country)s, %(state)s, %(address)s,
                             %(about)s, %(services)s, %(tech_stack)s, %(source)s,
                             %(source_id)s, %(website_url)s, %(raw_data)s)
                        RETURNING id""",
                        {
                            "company_name": company.get("company_name", ""),
                            "company_domain": company.get("company_domain"),
                            "company_type": company.get("company_type"),
                            "company_size": company.get("company_size"),
                            "country": company.get("country", "USA"),
                            "state": company.get("state"),
                            "address": company.get("address"),
                            "about": company.get("about"),
                            "services": company.get("services"),
                            "tech_stack": company.get("tech_stack"),
                            "source": self.source_name,
                            "source_id": company.get("source_id"),
                            "website_url": company.get("website_url"),
                            "raw_data": json.dumps(company.get("raw_data", {}), default=str),
                        },
                    )
                    raw_id = cursor.fetchone()["id"]
                    cursor.execute("RELEASE SAVEPOINT row_sp")
                    inserted += 1
                    src_id = company.get("source_id")
                    if src_id:
                        source_id_to_raw_id[src_id] = raw_id
                except Exception as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT row_sp")
                    logger.debug("Skipped company '%s': %s", company.get("company_name", "?"), e)
            conn.commit()
            cursor.close()
        return inserted, source_id_to_raw_id

    def _store_contacts(self, contacts: list[dict]) -> int:
        if not contacts:
            return 0
        inserted = 0
        with get_engine_connection() as conn:
            cursor = conn.cursor()
            for contact in contacts:
                try:
                    cursor.execute("SAVEPOINT row_sp")
                    cursor.execute(
                        """INSERT INTO raw_contacts
                            (raw_company_id, full_name, job_title, seniority,
                             email, email_verified, email_confidence, phone,
                             linkedin_url, source, raw_data)
                        VALUES (%(raw_company_id)s, %(full_name)s, %(job_title)s,
                             %(seniority)s, %(email)s, %(email_verified)s,
                             %(email_confidence)s, %(phone)s, %(linkedin_url)s,
                             %(source)s, %(raw_data)s)""",
                        {
                            "raw_company_id": contact.get("raw_company_id"),
                            "full_name": contact.get("full_name"),
                            "job_title": contact.get("job_title"),
                            "seniority": contact.get("seniority"),
                            "email": contact.get("email"),
                            "email_verified": contact.get("email_verified", False),
                            "email_confidence": contact.get("email_confidence", 0),
                            "phone": contact.get("phone"),
                            "linkedin_url": contact.get("linkedin_url"),
                            "source": self.source_name,
                            "raw_data": json.dumps(contact.get("raw_data", {}), default=str),
                        },
                    )
                    cursor.execute("RELEASE SAVEPOINT row_sp")
                    inserted += 1
                except Exception as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT row_sp")
                    logger.debug("Skipped contact '%s': %s", contact.get("email", "?"), e)
            conn.commit()
            cursor.close()
        return inserted

    def _start_run(self) -> int:
        with get_engine_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO extraction_runs (source, status, started_at) VALUES (%s, 'running', %s) RETURNING id",
                (self.source_name, datetime.utcnow()),
            )
            run_id = cursor.fetchone()["id"]
            conn.commit()
            cursor.close()
        return run_id

    def _complete_run(self, run_id: int, summary: dict):
        with get_engine_connection() as conn:
            cursor = conn.cursor()
            co_found = summary.get("companies_found", 0)
            co_new = summary.get("companies_new", 0)
            ct_found = summary.get("contacts_found", 0)
            ct_new = summary.get("contacts_new", 0)
            cursor.execute(
                """UPDATE extraction_runs SET status='success',
                       companies_found=%s, companies_new=%s,
                       contacts_found=%s, contacts_new=%s,
                       records_found=%s, records_new=%s,
                       completed_at=%s
                   WHERE id=%s""",
                (co_found, co_new, ct_found, ct_new,
                 co_found + ct_found, co_new + ct_new,
                 datetime.utcnow(), run_id),
            )
            conn.commit()
            cursor.close()

    def _fail_run(self, run_id: int, error_message: str):
        with get_engine_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE extraction_runs SET status='failed', error=%s, completed_at=%s WHERE id=%s",
                (error_message, datetime.utcnow(), run_id),
            )
            conn.commit()
            cursor.close()
