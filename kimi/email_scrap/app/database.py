import psycopg2
import psycopg2.extras
from passlib.context import CryptContext
import os
from dotenv import load_dotenv
import bcrypt

load_dotenv()

# Database configuration from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))  # 5432 for PostgreSQL
DB_NAME = os.getenv("DB_NAME", "crwm_db")
DB_USER = os.getenv("DB_USER", "crwm_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "crwm_password")

def get_db_connection():
    """Create and return a PostgreSQL database connection"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=psycopg2.extras.DictCursor
    )

def init_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Set statement timeout for safety (2 seconds in milliseconds)
        cursor.execute("SET statement_timeout = 2000")
        conn.commit()
        
        try:
            # Create companies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    code VARCHAR(10) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create users table with extended columns
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password_hash TEXT,
                    profile_picture TEXT,
                    company_id INT,
                    role VARCHAR(20) DEFAULT 'member',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE SET NULL
                )
            """)
            
            # 🔄 Ensure existing 'users' table has extended columns (Safely)
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'users'")
            # DictCursor rows are indexable by number
            existing_columns = [row[0] for row in cursor.fetchall()]
            
            if "name" not in existing_columns:
                cursor.execute("ALTER TABLE users ADD COLUMN name VARCHAR(255)")
            if "profile_picture" not in existing_columns:
                cursor.execute("ALTER TABLE users ADD COLUMN profile_picture TEXT")
            if "company_id" not in existing_columns:
                cursor.execute("ALTER TABLE users ADD COLUMN company_id INT")
            if "role" not in existing_columns:
                cursor.execute("ALTER TABLE users ADD COLUMN role VARCHAR(20) DEFAULT 'member'")

            # Safely add constraint
            cursor.execute("""
                SELECT constraint_name FROM information_schema.table_constraints 
                WHERE table_name='users' AND constraint_name='fk_company'
            """)
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE users ADD CONSTRAINT fk_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE SET NULL")
            conn.commit()


            # Create activity_logs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS activity_logs (
                    id SERIAL PRIMARY KEY,
                    user_id INT,
                    user_email VARCHAR(255),
                    action VARCHAR(100) NOT NULL,
                    resource_type VARCHAR(100),
                    resource_id VARCHAR(255),
                    details JSON,
                    status VARCHAR(20) DEFAULT 'success',
                    error_message TEXT,
                    ip_address VARCHAR(45),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
                )
            """)
            conn.commit()
            
            # Create Index Creation inside PostgreSQL standard
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON activity_logs (user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_action ON activity_logs (action)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_resource_type ON activity_logs (resource_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON activity_logs (created_at)")
            conn.commit()

            # Create staging table for incremental scraper runs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS staging_fcc_listings (
                    id SERIAL PRIMARY KEY,
                    frn VARCHAR(100) UNIQUE NOT NULL,
                    business_name VARCHAR(255),
                    sys_id VARCHAR(100),
                    attachment_link TEXT,
                    other_data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

            # Create leads table for distribution setup

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leads (
                    id SERIAL PRIMARY KEY,
                    company_name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    phone VARCHAR(50),
                    verify_status VARCHAR(50),
                    assigned_company_id INT,
                    assigned_user_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (assigned_company_id) REFERENCES companies(id) ON DELETE SET NULL,
                    FOREIGN KEY (assigned_user_id) REFERENCES users(id) ON DELETE SET NULL
                )
            """)

            # Create verified_emails table for tickmarks 
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS verified_emails (
                    id SERIAL PRIMARY KEY,
                    company_name VARCHAR(255),
                    email VARCHAR(255) UNIQUE NOT NULL,
                    is_ticked BOOLEAN DEFAULT FALSE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            
            # Create assigned_leads mapping table for distribution
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assigned_leads (
                    email VARCHAR(255) PRIMARY KEY,
                    assigned_user_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (assigned_user_id) REFERENCES users(id) ON DELETE CASCADE
                )
            """)
            conn.commit()
            
            # Seed Default Companies if empty
            cursor.execute("SELECT COUNT(*) FROM companies")
            coef = cursor.fetchone()
            if coef[0] == 0:
                companies_seed = [("DP", "DP"), ("MCC", "MCC"), ("VC", "VC"), ("ST", "ST")]
                cursor.executemany("INSERT INTO companies (name, code) VALUES (%s, %s)", companies_seed)
                conn.commit()
            
            # Force-update or create Default Admin User
            cursor.execute("SELECT id FROM users WHERE email = %s", ("admin@example.com",))
            user = cursor.fetchone()
            
            salt = bcrypt.gensalt()
            default_password = bcrypt.hashpw("1234".encode('utf-8'), salt).decode('utf-8')
            
            if not user:
                cursor.execute(
                    "INSERT INTO users (name, email, password_hash, role, company_id) VALUES (%s, %s, %s, %s, %s)",
                    ("Admin User", "admin@example.com", default_password, "admin", 1)
                )
                print("Default admin user created: admin@example.com / 1234")
            else:
                cursor.execute(
                    "UPDATE users SET name = 'Admin User', password_hash = %s, role = 'admin', company_id = 1 WHERE email = %s",
                    (default_password, "admin@example.com")
                )
                print("Default admin user name/credentials reset: admin@example.com / 1234")
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"Error initializing database: {e}")
        raise

def get_db():
    conn = get_db_connection()
    return conn

def verify_password(plain_password, hashed_password):
    try:
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    except Exception as e:
        print(f"Password verification error: {e}")
        return False

def get_password_hash(password):
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

def fetch_one_dict(cursor, query, params=None):
    cursor.execute(query, params)
    row = cursor.fetchone()
    return row if row else None
