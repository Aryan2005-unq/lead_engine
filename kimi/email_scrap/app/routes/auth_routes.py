from fastapi import APIRouter, Request, HTTPException, status
from app.models import LoginRequest
from app.auth import authenticate_user
from app.database import init_db
from app.logging_system import ActivityLogger, get_client_ip

router = APIRouter()

@router.post("/api/login")
async def login(request: Request, login_data: LoginRequest):
    ip_address = get_client_ip(request)
    try:
        print(f"=== LOGIN ATTEMPT ===")
        print(f"Email: {login_data.email}")
        print(f"Password length: {len(login_data.password)}")
        
        # Verify database is accessible
        try:
            from app.database import get_db_connection
            test_conn = get_db_connection()
            test_conn.close()
            print("Database connection: OK")
        except Exception as db_error:
            print(f"Database connection error: {db_error}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database connection failed: {str(db_error)}"
            )
        
        user = authenticate_user(login_data.email, login_data.password)
        if not user:
            print(f"Login failed for: {login_data.email} - authentication returned None")
            ActivityLogger.log_activity(
                user_id=None,
                user_email=login_data.email,
                action="login",
                resource_type="user",
                status="error",
                error_message="Invalid email or password",
                ip_address=ip_address
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password"
            )
        
        request.session["user_id"] = user["id"]
        request.session["user_email"] = user["email"]
        print(f"Login successful for: {login_data.email} (ID: {user['id']})")
        print(f"Session set: user_id={user['id']}, user_email={user['email']}")
        
        ActivityLogger.log_activity(
            user_id=user["id"],
            user_email=user["email"],
            action="login",
            resource_type="user",
            status="success",
            ip_address=ip_address
        )
        
        return {"success": True, "message": "Login successful"}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Login error for {login_data.email}: {e}")
        print(f"Traceback: {error_trace}")
        
        ActivityLogger.log_activity(
            user_id=None,
            user_email=login_data.email,
            action="login",
            resource_type="user",
            status="error",
            error_message=str(e),
            ip_address=ip_address
        )
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred during login: {str(e)}"
        )

@router.post("/api/logout")
async def logout(request: Request):
    user_id = request.session.get("user_id")
    user_email = request.session.get("user_email")
    ip_address = get_client_ip(request)
    
    ActivityLogger.log_activity(
        user_id=user_id,
        user_email=user_email,
        action="logout",
        resource_type="user",
        status="success",
        ip_address=ip_address
    )
    
    request.session.clear()
    return {"success": True, "message": "Logged out"}

@router.get("/api/check-auth")
async def check_auth(request: Request):
    user_id = request.session.get("user_id")
    if user_id:
        from app.database import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT u.name, u.role, u.profile_picture, c.name as company_name 
            FROM users u 
            LEFT JOIN companies c ON u.company_id = c.id 
            WHERE u.id = %s
        """, (user_id,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return {
            "authenticated": True, 
            "email": request.session.get("user_email"),
            "name": user["name"] if user and user["name"] else request.session.get("user_email").split('@')[0],
            "role": user["role"] if user else "member",
            "company_name": user["company_name"] if user and user["company_name"] else "N/A",
            "profile_picture": user["profile_picture"] if user else None
        }

    return {"authenticated": False}

@router.post("/api/login/google")
async def login_google(request: Request):
    """
    🔏 Authenticate with Google ID Token payload
    """
    ip_address = get_client_ip(request)
    try:
        data = await request.json()
        token = data.get("token")
        company_id = data.get("company_id") # Optional signup

        if not token:
            raise HTTPException(status_code=400, detail="Missing Google Token")

        # 1. Fetch info from Google Token endpoint info (Zero dependencies client)
        import requests
        resp = requests.get(f"https://oauth2.googleapis.com/tokeninfo?id_token={token}")
        if resp.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid Google Token")
            
        payload = resp.json()
        email = payload.get("email")
        name = payload.get("name")
        picture = payload.get("picture")

        # 2. Query and UPSERT user
        from app.database import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, email, role, company_id FROM users WHERE email = %s", (email,))
        user = cursor.fetchone()

        if not user:
            # Create user on first login
            cursor.execute("""
                INSERT INTO users (name, email, profile_picture, company_id, role)
                VALUES (%s, %s, %s, %s, %s)
            """, (name, email, picture, company_id or 1, "member"))
            conn.commit()
            cursor.execute("SELECT id, email, role, company_id FROM users WHERE email = %s", (email,))
            user = cursor.fetchone()

        request.session["user_id"] = user["id"]
        request.session["user_email"] = user["email"]

        ActivityLogger.log_activity(
            user_id=user["id"],
            user_email=user["email"],
            action="login_google",
            resource_type="user",
            status="success",
            ip_address=ip_address
        )

        cursor.close()
        conn.close()
        
        return {"success": True, "message": "Google Authentication Successful"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/companies")
async def list_companies():
    """🏢 Provide company reference nodes"""
    from app.database import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, code FROM companies")
    columns = [desc[0] for desc in cursor.description]

    results = cursor.fetchall()

    cursor.close()
    conn.close()
    return [dict(zip(columns, r)) for r in results]


from pydantic import BaseModel

class CreateEmployeeRequest(BaseModel):
    name: str
    email: str
    password: str
    company_id: int
    role: str = "member"

@router.post("/api/company/users")
async def add_employee(request: Request, emp_data: CreateEmployeeRequest):
    """👨‍💼 Register secondary workplace operators managers."""
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    from app.database import get_db_connection, get_password_hash
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT role, company_id FROM users WHERE id = %s", (user_id,))
        current_user = cursor.fetchone()
        
        if not current_user or current_user['role'] != 'admin':
            raise HTTPException(status_code=403, detail="Only admins can manage employees")
            
        cursor.execute("SELECT id FROM users WHERE email = %s", (emp_data.email,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")
            
        hashed_pw = get_password_hash(emp_data.password)
        cursor.execute(
            "INSERT INTO users (name, email, password_hash, role, company_id) VALUES (%s, %s, %s, %s, %s)",
            (emp_data.name, emp_data.email, hashed_pw, emp_data.role, emp_data.company_id)
        )
        conn.commit()
        return {"success": True, "message": "Employee created successfully"}
    finally:
        cursor.close()
        conn.close()

@router.get("/api/company/users")
async def list_employees(request: Request):
    """📋 List managers matching tenant context directly."""
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    from app.database import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT company_id FROM users WHERE id = %s", (user_id,))
        current_user = cursor.fetchone()
        
        # Admin from DialPhone (comp 1) lists all. Others list their branch.
        if current_user['company_id'] == 1:
             cursor.execute("""
                SELECT u.id, u.name, u.email, u.role, u.profile_picture, c.code as company_code 
                FROM users u 
                LEFT JOIN companies c ON u.company_id = c.id
             """)
        else:
             cursor.execute("""
                SELECT u.id, u.name, u.email, u.role, u.profile_picture, c.code as company_code  
                FROM users u 
                LEFT JOIN companies c ON u.company_id = c.id
                WHERE u.company_id = %s
             """, (current_user['company_id'],))
             
        # Convert to dictionary using cursor description titles to fix JSON serialization
        columns = [desc[0] for desc in cursor.description]
        users = cursor.fetchall()
        return [dict(zip(columns, u)) for u in users]

    finally:
        cursor.close()
        conn.close()

class KickEmployeeRequest(BaseModel):
    employee_id: int

@router.post("/api/company/users/kick")
async def kick_employee(request: Request, data: KickEmployeeRequest):
    """⚔️ Remove/Kick workplace operator from tenant roster setups."""
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    from app.database import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT role, company_id FROM users WHERE id = %s", (user_id,))
        current_user = cursor.fetchone()
        
        if not current_user or current_user['role'] != 'admin':
            raise HTTPException(status_code=403, detail="Only admins can manage employees")
            
        # Verify targeted delete safety locks
        if data.employee_id == user_id:
             raise HTTPException(status_code=400, detail="You cannot kick yourself out")
             
        cursor.execute("DELETE FROM users WHERE id = %s", (data.employee_id,))
        conn.commit()
        return {"success": True, "message": "Employee kicked out successfully"}
    finally:
        cursor.close()
        conn.close()
