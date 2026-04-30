"""
Service logic for the Lead Distribution Engine - Full Multi-Level Round Robin
Modified to support Continuous Cyclic Shifts with state persistence on JSON setups.
"""
from app.database import get_db_connection
from fastapi import HTTPException
import asyncio
import json
import os

CYCLE_STATE_FILE = "cycle_state.json"

distribution_status = {
    "is_running": False,
    "lead_count_per_company": 400, # Total Target Batch
    "interval_seconds": 60, # 1 Week is 604800, using 60 for customizable/testing interval
    "error": None
}

def load_cycle_state():
    if os.path.exists(CYCLE_STATE_FILE):
        try:
            with open(CYCLE_STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {"active_batch": False, "leads": [], "companies_ids": [], "current_week": 1}

def save_cycle_state(state):
    with open(CYCLE_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)


async def distribute_leads_service(company_allocations: list):
    """
    Manual Distribute: Traditional rotational Round-Robin sequential placement across target lists.
    """
    total_requested = sum(alloc["lead_count"] for alloc in company_allocations)
    if total_requested <= 0:
        raise HTTPException(status_code=400, detail="Invalid lead allocation count")

    from app.routes.email_routes import get_emails_data
    emails = await get_emails_data()
    if not emails:
        raise HTTPException(status_code=400, detail="No leads found in source files")

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT email FROM assigned_leads")
        assigned_rows = cursor.fetchall()
        assigned_emails = { row['email'] for row in assigned_rows }
        
        available_leads = [ e for e in emails if e['email'] not in assigned_emails ]
        if total_requested > len(available_leads):
            raise HTTPException(status_code=400, detail=f"Not enough unassigned leads available.")

        available_leads.sort(key=lambda x: x['id'], reverse=True)
        unassigned_leads = available_leads[:total_requested]

        members_cache = {}
        active_member_pointer = {}
        alloc_queues = []

        for alloc in company_allocations:
            cid = alloc["company_id"]
            lead_count = alloc["lead_count"]
            if lead_count <= 0: continue

            cursor.execute("SELECT id FROM users WHERE company_id = %s AND (role != 'admin' OR role IS NULL)", (cid,))
            members = cursor.fetchall()
            if not members: continue

            members_cache[cid] = members
            active_member_pointer[cid] = 0
            alloc_queues.append({"company_id": cid, "lead_count": lead_count, "assigned_count": 0})

        if not alloc_queues:
             raise HTTPException(status_code=400, detail="No companies with members found.")

        assignments = []
        queue_pointer = 0

        for lead in unassigned_leads:
            if not alloc_queues: break
            queue_idx = queue_pointer % len(alloc_queues)
            current_alloc = alloc_queues[queue_idx]
            cid = current_alloc["company_id"]

            members_list = members_cache[cid]
            member = members_list[active_member_pointer[cid] % len(members_list)]

            assignments.append((lead["email"], member["id"]))
            current_alloc["assigned_count"] += 1
            active_member_pointer[cid] += 1
            queue_pointer += 1

            if current_alloc["assigned_count"] >= current_alloc["lead_count"]:
                alloc_queues.pop(queue_idx)
                if len(alloc_queues) > 0: queue_pointer = queue_idx % len(alloc_queues)

        if assignments:
             cursor.executemany("INSERT INTO assigned_leads (email, assigned_user_id) VALUES (%s, %s)", assignments)
             conn.commit()

        return {"success": True, "message": f"Successfully distributed {len(assignments)} leads."}
    finally:
        cursor.close()
        conn.close()


async def get_member_leads(member_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT email FROM assigned_leads WHERE assigned_user_id = %s", (member_id,))
        leads = cursor.fetchall()
        return [{"email": lead["email"]} for lead in leads]
    finally:
        cursor.close()
        conn.close()

# ============================================
# Autonomous Cyclic Processes
# ============================================

async def distribute_cyclic_batch(company_id: int, leads: list):
    """Allocates a list of leads exclusively to one Company's internal members Round-Robin"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM users WHERE company_id = %s AND (role != 'admin' OR role IS NULL)", (company_id,))
        members = cursor.fetchall()
        if not members: return 0

        cursor.execute("SELECT email FROM assigned_leads WHERE assigned_user_id IN (SELECT id FROM users WHERE company_id = %s)", (company_id,))
        company_assigned = { r['email'] for r in cursor.fetchall() }

        unassigned_leads = [ e for e in leads if e not in company_assigned ]
        
        assignments = []
        for i, lead in enumerate(unassigned_leads):
            member = members[i % len(members)]
            assignments.append((lead, member["id"]))

        if assignments:
             cursor.executemany("INSERT INTO assigned_leads (email, assigned_user_id) VALUES (%s, %s)", assignments)
             conn.commit()
        return len(assignments)
    finally:
         cursor.close()
         conn.close()


async def start_autonomous_distribution(count: int = 400, interval: int = 604800):
    if distribution_status["is_running"]:
        return {"success": True, "message": "Autonomous mode is already running."}
        
    # Clear previous state so starting afresh always pulls a new batch immediately
    if os.path.exists(CYCLE_STATE_FILE):
        try:
            os.remove(CYCLE_STATE_FILE)
        except:
            pass
            
    distribution_status["is_running"] = True
    distribution_status["lead_count_per_company"] = count # Serves as total_batch size in Cyclic mode
    distribution_status["interval_seconds"] = interval
    distribution_status["task"] = asyncio.create_task(_distribution_worker_loop())
    return {"success": True, "message": "Autonomous Cyclic Rotation Started."}



async def stop_autonomous_distribution():
    distribution_status["is_running"] = False
    
    # Cancel the sleeping task if it exists so it doesn't linger
    if "task" in distribution_status and distribution_status["task"]:
        distribution_status["task"].cancel()
        distribution_status["task"] = None
        
    # Clear state so the next start is completely fresh
    if os.path.exists(CYCLE_STATE_FILE):
        try:
            os.remove(CYCLE_STATE_FILE)
        except:
            pass
            
    return {"success": True, "message": "Autonomous Cyclic distribution Stopped."}


async def _distribution_worker_loop():
    print("Autonomous Cyclic Distribution Activated.")
    
    while distribution_status["is_running"]:
        try:
            state = load_cycle_state()
            conn = get_db_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT id FROM companies")
                companies = [c['id'] for c in cursor.fetchall()]

                active_companies = []
                for cid in companies:
                    cursor.execute("SELECT COUNT(*) as cnt FROM users WHERE company_id = %s AND (role != 'admin' OR role IS NULL)", (cid,))
                    if cursor.fetchone()["cnt"] > 0:
                        active_companies.append(cid)

                num_c = len(active_companies)
                if num_c == 0:
                     print("No active companies with members found. Skipping cycle.")
                     await asyncio.sleep(float(distribution_status["interval_seconds"]))
                     continue

                # 1. Check if we need to initialize a NEW BATCH
                if not state.get("active_batch") or len(state.get("companies_ids", [])) != num_c:
                    print("Initializing New Cyclic Batch of leads.")
                    from app.routes.email_routes import get_emails_data
                    emails = await get_emails_data()

                    cursor.execute("SELECT email FROM assigned_leads")
                    already_assigned_globally = { r['email'] for r in cursor.fetchall() }
                    
                    available_emails = [ e['email'] for e in emails if e['email'] not in already_assigned_globally ]
                    total_req = distribution_status.get("lead_count_per_company", 400)
                    
                    if len(available_emails) < total_req:
                         print("Not enough fresh leads for new batch. Proceeding with available.")
                         total_req = len(available_emails)

                    batch_leads = available_emails[:total_req]
                    
                    state = {
                        "active_batch": True if batch_leads else False,
                        "leads": batch_leads,
                        "companies_ids": active_companies,
                        "current_week": 1
                    }
                    save_cycle_state(state)

                if state["active_batch"] and state["leads"]:
                    # 2. Process Cyclic Allocation for the Current Week
                    total_leads = len(state["leads"])
                    bucket_size = total_leads // num_c
                    curr_week = state["current_week"]

                    print(f"Executing Cycle Week {curr_week}/{num_c} for {total_leads} leads")

                    assigned_total = 0
                    for i, cid in enumerate(state["companies_ids"]):
                        # Calculate bucket offset accurately
                        # Week 1: i gets Bucket i
                        # Week 2: i gets Bucket (i+1)%C
                        bucket_idx = (i + curr_week - 1) % num_c
                        start_idx = bucket_idx * bucket_size
                        end_idx = start_idx + bucket_size if (bucket_idx < num_c - 1) else total_leads
                        bucket_leads = state["leads"][start_idx:end_idx]

                        cnt = await distribute_cyclic_batch(cid, bucket_leads)
                        assigned_total += cnt

                    print(f"Cycle Week {curr_week} Complete. Total Assignments added: {assigned_total}")

                    # Advance Week index
                    state["current_week"] += 1
                    if state["current_week"] > num_c:
                         print("Cyclic Batch fully distributed to all companies. Closing batch.")
                         state["active_batch"] = False # Triggers new batch pull next cycle trigger line.

                    save_cycle_state(state)

            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            print(f"Autonomous Loop iteration crashed: {e}")

        await asyncio.sleep(float(distribution_status.get("interval_seconds", 60)))
    
    print("Autonomous Cyclic Distribution Deactivated.")
