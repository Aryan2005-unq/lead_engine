import os
import time
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

PROSPEO_API_KEY = os.environ.get("PROSPEO_API_KEY")
DB_HOST = "localhost"
DB_PORT = os.environ.get("ENGINE_DB_PORT", "5433")
DB_NAME = "lead_engine_db"
DB_USER = "crwm_user"
DB_PASS = "crwm_password"

headers = {
    "X-KEY": PROSPEO_API_KEY,
    "Content-Type": "application/json"
}

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def enrich_crtc_batch(limit=10):
    print(f"[STEP 1] Fetching up to {limit} un-enriched CRTC companies...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Fetch un-enriched companies
    cur.execute("""
        SELECT id, company_name 
        FROM companies 
        WHERE sources::text ILIKE '%crtc%' 
          AND enriched = FALSE
          AND length(company_name) > 3
          AND company_name NOT ILIKE '%nouvelles%'
          AND company_name NOT ILIKE '%0 - 9%'
        LIMIT %s;
    """, (limit,))
    companies = cur.fetchall()
    
    if not companies:
        print("[SUCCESS] All CRTC companies in the database have been processed!")
        cur.close()
        conn.close()
        return

    print(f"[START] Processing batch of {len(companies)} companies...")

    for idx, company in enumerate(companies, 1):
        company_id, company_name = company
        
        # Clean company name to improve Prospeo match rate
        clean_name = company_name.replace(" Inc.", "").replace(" G.P.", "").replace(" Canada", "").replace(" Limited Partnership", "").replace("Services", "").split(",")[0].strip()
        
        print(f"\n[{idx}/{len(companies)}] Processing {clean_name}...")

        search_url = "https://api.prospeo.io/search-person"
        search_payload = {
            "filters": {
                "company": {
                    "names": {
                        "include": [clean_name]
                    }
                }
            },
            "page": 1
        }
        
        try:
            # Rate limit protection
            time.sleep(3) 
            search_res = requests.post(search_url, headers=headers, json=search_payload, timeout=15)
            
            if search_res.status_code == 429:
                print("   [STOP] Daily Rate Limit Exceeded. Stopping script.")
                break

            if search_res.status_code != 200:
                print(f"   [API Error] Search failed: {search_res.status_code}")
                # Still mark as enriched so we don't loop forever on broken API entries
                cur.execute("UPDATE companies SET enriched = TRUE WHERE id = %s", (company_id,))
                conn.commit()
                continue
                
            data = search_res.json()
            people = data.get("results", [])
            
            if not people:
                print(f"   [No Match] No people found at {clean_name} on Prospeo.")
                # Mark as enriched so we don't try this company again
                cur.execute("UPDATE companies SET enriched = TRUE WHERE id = %s", (company_id,))
                conn.commit()
                continue
                
            top_person = people[0]
            person_data = top_person.get("person", top_person)
            linkedin_url = person_data.get("linkedin_url")
            full_name = person_data.get("full_name", "Unknown")
            
            if not linkedin_url:
                print(f"   [Failed] {full_name} has no LinkedIn URL.")
                cur.execute("UPDATE companies SET enriched = TRUE WHERE id = %s", (company_id,))
                conn.commit()
                continue
                
            print(f"   [ENRICH] Found {full_name}. Fetching email...")
            
            enrich_url = "https://api.prospeo.io/enrich-person"
            enrich_payload = {
                "data": {
                    "linkedin_url": linkedin_url
                },
                "only_verified_email": False
            }
            
            time.sleep(3) 
            enrich_res = requests.post(enrich_url, headers=headers, json=enrich_payload, timeout=15)
            
            if enrich_res.status_code == 200:
                profile = enrich_res.json().get("response", {})
                if profile:
                    email_obj = profile.get("email", {})
                    email = email_obj.get("email") if isinstance(email_obj, dict) else None
                    
                    if email:
                        print(f"   [SUCCESS] Email Found: {email}")
                        cur.execute("""
                            INSERT INTO contacts (company_id, full_name, linkedin_url, email, sources, enriched)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (email) DO NOTHING
                        """, (company_id, full_name, linkedin_url, email, json.dumps(['prospeo_enriched']), True))
                    else:
                        print("   [Failed] No verified email found.")
                
            # REGARDLESS of email result, mark the company as "Processed"
            cur.execute("UPDATE companies SET enriched = TRUE WHERE id = %s", (company_id,))
            conn.commit()

        except Exception as e:
            print(f"   [Unexpected Error] {e}")

    cur.close()
    conn.close()
    print("\n[COMPLETE] Batch finished.")

if __name__ == "__main__":
    # You can increase this number if you upgrade your Prospeo plan
    enrich_crtc_batch(limit=20)
