from fastapi import Request, HTTPException, status
from app.database import get_db_connection, verify_password, fetch_one_dict

def get_user_by_email(email: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            user = fetch_one_dict(cursor, "SELECT * FROM users WHERE email = %s", (email,))
            return user
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"Error getting user by email: {e}")
        import traceback
        traceback.print_exc()
        return None

def authenticate_user(email: str, password: str):
    try:
        user = get_user_by_email(email)
        if not user:
            print(f"User not found: {email}")
            return None
        
        print(f"Found user: {user['email']}, ID: {user['id']}")
        
        if not user.get("password_hash"):
            print(f"No password hash found for user: {email}")
            return None
        
        print(f"Attempting to verify password. Hash length: {len(user['password_hash'])}")
        
        try:
            is_valid = verify_password(password, user["password_hash"])
            if not is_valid:
                print(f"Password verification failed for: {email}")
                return None
        except Exception as e:
            print(f"Error during password verification for {email}: {e}")
            import traceback
            traceback.print_exc()
            return None
        
        print(f"Password verification successful for: {email}")
        return {"id": user["id"], "email": user["email"]}
    except Exception as e:
        print(f"Error in authenticate_user: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_current_user(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, email, role FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=401, detail="User session invalid")
        return user
    finally:
        cursor.close()
        conn.close()


