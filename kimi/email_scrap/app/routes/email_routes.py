import os
import glob
import csv
from fastapi import APIRouter, HTTPException, Depends, Request
from app.database import get_db_connection
from pydantic import BaseModel
from typing import List

router = APIRouter()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")

from typing import List, Optional


class ToggleRequest(BaseModel):
    email: str
    is_ticked: bool

class AssignLeadsRequest(BaseModel):
    employee_id: int
    count: int
    company_name: Optional[str] = ""

class DistributeAllRequest(BaseModel):
    employee_company_id: int
    count: int
    company_name: Optional[str] = ""



def find_latest_file(pattern, directory):
    if not os.path.exists(directory):
        return None
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)

async def get_emails_data():
    """Helper to fetch emails directly from the leads database table (live)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch all leads from the database
        cursor.execute("""
            SELECT l.id, l.company_name, l.email, l.phone, l.verify_status,
                   COALESCE(ve.is_ticked, FALSE) as is_ticked
            FROM leads l
            LEFT JOIN verified_emails ve ON l.email = ve.email
            ORDER BY l.id DESC
        """)
        rows = cursor.fetchall()

        emails_data = []
        seen_emails = set()
        for row in rows:
            email = row['email']
            if email and email not in seen_emails:
                seen_emails.add(email)
                emails_data.append({
                    "id": row['id'],
                    "company_name": row['company_name'] or "N/A",
                    "email": email,
                    "is_verified": bool(row['verify_status'] and 'Active' in row['verify_status']),
                    "phone": row['phone'] or "N/A",
                    "address": "N/A",
                    "contact_name": "N/A",
                    "is_ticked": bool(row['is_ticked']),
                    "verify_status": row['verify_status'] or "Unknown"
                })

        cursor.close()
        conn.close()
        return emails_data
    except Exception as e:
        print(f"Database fetch error in get_emails_data: {e}")
        return []

@router.get("/api/emails")
async def get_emails(req: Request):
    """View endpoint returning emails filtered by role."""
    user_id = req.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    emails_data = await get_emails_data()
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT role FROM users WHERE id = %s", (user_id,))
        user_row = cursor.fetchone()
        
        if user_row and user_row['role'] != 'admin':
            # 🛡️ Member Filter: Only show emails assigned to them
            cursor.execute("SELECT email FROM assigned_leads WHERE assigned_user_id = %s", (user_id,))
            assigned_rows = cursor.fetchall()
            assigned_emails = { row['email'] for row in assigned_rows }
            emails_data = [ item for item in emails_data if item['email'] in assigned_emails ]
            
        cursor.close()
        conn.close()
        return emails_data
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/emails/toggle")
async def toggle_email(data: ToggleRequest):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO verified_emails (email, is_ticked) 
            VALUES (%s, %s)
            ON CONFLICT (email) DO UPDATE 
            SET is_ticked = EXCLUDED.is_ticked, updated_at = CURRENT_TIMESTAMP
        """, (data.email, data.is_ticked))
        conn.commit()
        cursor.close()
        conn.close()
        return {"success": True}
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/leads/assign")
async def assign_leads(req: Request, data: AssignLeadsRequest):
    """📊 Allocate available leads from CSV nodes to fixed staff."""
    user_id = req.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT role FROM users WHERE id = %s", (user_id,))
        current_user = cursor.fetchone()
        if not current_user or current_user['role'] != 'admin':
            raise HTTPException(status_code=403, detail="Only admins can distribute leads")
            
        emails = await get_emails_data()
        matched_emails = [ item['email'] for item in emails if not data.company_name or item['company_name'] == data.company_name ]
        if not matched_emails:
            raise HTTPException(status_code=404, detail="No leads found" + (f" for company '{data.company_name}'" if data.company_name else ""))

            
        cursor.execute("SELECT email FROM assigned_leads")
        assigned_rows = cursor.fetchall()
        assigned_emails = { row['email'] for row in assigned_rows }
        
        available_emails = [ e for e in matched_emails if e not in assigned_emails ]
        if len(available_emails) == 0:
            raise HTTPException(status_code=400, detail="All leads for this company are already assigned")
            
        to_assign = available_emails[:data.count]
        values = [ (e, data.employee_id) for e in to_assign ]
        cursor.executemany("INSERT INTO assigned_leads (email, assigned_user_id) VALUES (%s, %s)", values)
        conn.commit()
        return {"success": True, "message": f"Successfully assigned {len(to_assign)} leads"}
    finally:
        cursor.close()
        conn.close()

@router.post("/api/leads/distribute-all")
async def distribute_leads_all(req: Request, data: DistributeAllRequest):
    """🔄 Equally and uniquely distribute available leads across all employees of a company."""
    user_id = req.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT role FROM users WHERE id = %s", (user_id,))
        current_user = cursor.fetchone()
        if not current_user or current_user['role'] != 'admin':
            raise HTTPException(status_code=403, detail="Only admins can distribute leads")
            
        if data.employee_company_id == 0:
            # 🌐 ALL Companies Mode
            from app.services.distribution_service import distribute_leads_service
            
            cursor.execute("SELECT id, name FROM companies")
            companies = cursor.fetchall()

            companies_with_members = []
            for comp in companies:
                 cursor.execute("SELECT COUNT(*) as cnt FROM users WHERE company_id = %s", (comp["id"],))
                 if cursor.fetchone()["cnt"] > 0:
                     companies_with_members.append(comp)

            if not companies_with_members:
                 raise HTTPException(status_code=404, detail="No active companies with members found")

            total_count = data.count
            num_companies = len(companies_with_members)
            base_count = total_count // num_companies
            remainder = total_count % num_companies
            
            allocs = []
            for i, comp in enumerate(companies_with_members):
                 count = base_count + (1 if i < remainder else 0)
                 allocs.append({
                     "company_id": comp["id"], 
                     "lead_count": count
                 })

            result = await distribute_leads_service(allocs)
            return result

        # 1. Fetch Employees of specific target company (Members only)
        cursor.execute("SELECT id FROM users WHERE company_id = %s AND (role != 'admin' OR role IS NULL)", (data.employee_company_id,))
        employees = cursor.fetchall()
        if not employees:
            raise HTTPException(status_code=404, detail="No employees found for this company")

            
        # 2. Fetch available emails
        emails = await get_emails_data()
        matched_emails = [item['email'] for item in emails if not data.company_name or item['company_name'] == data.company_name]
        if not matched_emails:
            raise HTTPException(status_code=404, detail="No leads found" + (f" for company '{data.company_name}'" if data.company_name else ""))

            
        # 3. Filter unassigned
        cursor.execute("SELECT email FROM assigned_leads")
        assigned_emails = { row['email'] for row in cursor.fetchall() }
        available_emails = [e for e in matched_emails if e not in assigned_emails]
        if not available_emails:
            raise HTTPException(status_code=400, detail="All leads for this company are already assigned")
            
        # 4. Take top N
        to_assign = available_emails[:data.count]
        
        # 5. Iterative equal distribute
        values = []
        num_employees = len(employees)
        for i, email in enumerate(to_assign):
            emp = employees[i % num_employees]
            values.append((email, emp['id']))
            
        cursor.executemany("INSERT INTO assigned_leads (email, assigned_user_id) VALUES (%s, %s)", values)
        conn.commit()
        
        return {
            "success": True, 
            "message": f"Distributed {len(to_assign)} leads equally across {num_employees} employees"
        }
    finally:
        cursor.close()
        conn.close()

@router.post("/api/emails/delete-all")
async def delete_all_leads(req: Request):
    """🛡️ Admin-only: Clear all lead data from the database securely."""
    user_id = req.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verify role
        cursor.execute("SELECT role FROM users WHERE id = %s", (user_id,))
        user_row = cursor.fetchone()
        
        if not user_row or user_row['role'] != 'admin':
            raise HTTPException(status_code=403, detail="Forbidden: Only Admins can clear data")
            
        # Truncate tables with cascade resets
        cursor.execute("TRUNCATE leads RESTART IDENTITY CASCADE;")
        cursor.execute("TRUNCATE assigned_leads RESTART IDENTITY CASCADE;")
        cursor.execute("TRUNCATE verified_emails RESTART IDENTITY CASCADE;")
        
        conn.commit()
        return {"success": True, "message": "All leads and assignments cleared successfully."}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database clearing error: {str(e)}")
    finally:
        cursor.close()
        conn.close()
