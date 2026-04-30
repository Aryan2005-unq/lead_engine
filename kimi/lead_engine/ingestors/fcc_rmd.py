"""RMD Ingestor - reads from existing crwm_db."""
import logging
from ingestors.base import BaseIngestor
from database import get_rmd_connection
from utils import extract_domain, normalize_email

logger = logging.getLogger(__name__)

class RMDIngestor(BaseIngestor):
    @property
    def source_name(self): return "RMD"

    def extract(self):
        companies, contacts, seen = [], [], {}
        try:
            with get_rmd_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, company_name, email, phone, verify_status FROM leads WHERE email IS NOT NULL AND email != '' ORDER BY id")
                for row in cursor.fetchall():
                    email = normalize_email(row["email"])
                    if not row["company_name"] or not email: continue
                    domain = extract_domain(email)
                    key = domain or row["company_name"].lower().strip()
                    if key not in seen:
                        companies.append({"company_name":row["company_name"].strip(),"company_domain":domain,"company_type":"Interconnected VoIP","country":"USA","source_id":f"rmd_{row['id']}","raw_data":{"rmd_id":row["id"]}})
                        seen[key] = True
                    is_verified = row["verify_status"] and row["verify_status"].lower() in ("active","verified","valid")
                    contacts.append({"full_name":None,"email":email,"email_verified":is_verified,"email_confidence":90 if is_verified else 50,"phone":row["phone"],"raw_data":{"rmd_id":row["id"],"company_name":row["company_name"]}})
                cursor.close()
        except Exception as e: logger.error("Failed to read RMD: %s", e); raise
        logger.info("Extracted %d companies and %d contacts from RMD", len(companies), len(contacts))
        return companies, contacts
