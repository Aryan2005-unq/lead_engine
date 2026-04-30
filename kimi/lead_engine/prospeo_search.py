import os
import requests
import json
import psycopg2
import time
from dotenv import load_dotenv

load_dotenv()
PROSPEO_API_KEY = os.getenv("PROSPEO_API_KEY")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("ENGINE_DB_HOST", "localhost"),
        port=os.getenv("ENGINE_DB_PORT", "5433"),
        dbname=os.getenv("ENGINE_DB_NAME", "lead_engine_db"),
        user=os.getenv("ENGINE_DB_USER", "crwm_user"),
        password=os.getenv("ENGINE_DB_PASSWORD", "crwm_password")
    )

def enrich_one_db_contact():
    if not PROSPEO_API_KEY or PROSPEO_API_KEY == "your_actual_key_here":
        print("[Error] Valid PROSPEO_API_KEY not found in .env")
        return

    headers = {
        "X-KEY": PROSPEO_API_KEY,
        "Content-Type": "application/json"
    }

    print("[STEP 1] Fetching one contact from the Database...")
    
    conn = get_db_connection()
    print("[STEP 1] Fetching a CRTC company from the Database...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Fetch 1 CRTC company with a domain
    cur.execute("""
        SELECT id, company_name, company_domain
        FROM companies 
        WHERE sources::text ILIKE '%crtc%' 
          AND company_name ILIKE '%Rogers%'
        LIMIT 1;
    """)
    company = cur.fetchone()
    
    if not company:
        print("[SUCCESS] No CRTC companies found in the database!")
        cur.close()
        conn.close()
        return
        
    company_id, company_name, domain = company

    print(f"[TARGET] Found CRTC Company: {company_name}")
    print(f"[STEP 2] Searching for people at {company_name} via Prospeo API...")

    search_url = "https://api.prospeo.io/search-person"
    search_payload = {
        "filters": {
            "company": {
                "include": ["rogers.com"]
            }
        },
        "page": 1
    }
    
    try:
        search_res = requests.post(search_url, headers=headers, json=search_payload, timeout=15)
        if search_res.status_code != 200:
            print(f"   [API Error] {search_res.status_code} - {search_res.text}")
            return
            
        data = search_res.json()
        people = data.get("response", {}).get("data", [])
        
        if not people:
            print(f"   [Failed] No people found at {domain} on Prospeo.")
            return
            
        top_person = people[0]
        linkedin_url = top_person.get("linkedin_url")
        full_name = top_person.get("full_name", "Unknown")
        
        print(f"   [SUCCESS] Found {len(people)} people! Top result: {full_name}")
        
        if not linkedin_url:
            print(f"   [Failed] Top person {full_name} does not have a LinkedIn URL to enrich.")
            return
            
        print(f"[STEP 3] Enriching {full_name} to get email...")
        
        enrich_url = "https://api.prospeo.io/enrich-person"
        enrich_payload = {
            "data": {
                "linkedin_url": linkedin_url
            },
            "only_verified_email": False
        }
        
        # Sleep slightly to avoid rate limit
        time.sleep(1)
        enrich_res = requests.post(enrich_url, headers=headers, json=enrich_payload, timeout=15)
        
        if enrich_res.status_code == 200:
            profile = enrich_res.json().get("response", {})
            
            if not profile:
                print(f"   [Failed] Prospeo returned NO_MATCH for {full_name}")
            else:
                email_obj = profile.get("email", {})
                email = email_obj.get("email") if isinstance(email_obj, dict) else None
                
                if email:
                    print(f"   [SUCCESS] Found Verified Email: {email}")
                else:
                    print(f"   [Failed] Found profile but NO verified email.")
        else:
            print(f"   [API Error] {enrich_res.status_code} - {enrich_res.text}")

    except Exception as e:
        print(f"\n[Unexpected Error] {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    enrich_one_db_contact()
