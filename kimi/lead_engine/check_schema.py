import psycopg2
import json

conn=psycopg2.connect(host='localhost', port='5433', dbname='lead_engine_db', user='crwm_user', password='crwm_password')
cur=conn.cursor()
cur.execute("""
    SELECT id, company_name 
    FROM companies 
    WHERE sources::text ILIKE '%crtc%' 
      AND (
          company_name ILIKE '%Bell%' OR
          company_name ILIKE '%Rogers%' OR
          company_name ILIKE '%Telus%' OR
          company_name ILIKE '%Videotron%' OR
          company_name ILIKE '%Cogeco%' OR
          company_name ILIKE '%Shaw%' OR
          company_name ILIKE '%SaskTel%' OR
          company_name ILIKE '%Xplornet%' OR
          company_name ILIKE '%Telesat%' OR
          company_name ILIKE '%Quebecor%' OR
          company_name ILIKE '%Bragg%' OR
          company_name ILIKE '%Distributel%'
      )
    LIMIT 10;
""")
res = [{"id": row[0], "company_name": row[1].encode('ascii', 'ignore').decode('ascii')} for row in cur.fetchall()]
print(json.dumps(res))
cur.close()
conn.close()
