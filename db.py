import os
import psycopg2
import psycopg2.extras
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class Database:
    def __init__(self):
        self.conn = None
        self.connect()
    
    def connect(self):
        try:
            dbname = os.getenv('DATABASE_NAME', 'neondb')
            user = os.getenv('DATABASE_USER', 'neondb_owner')
            password = os.getenv('DATABASE_PASSWORD', 'npg_hCANMIw4u1Db')
            host = os.getenv('DATABASE_HOST', 'ep-red-night-aeq96bnr.c-2.us-east-2.aws.neon.tech')
            
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                sslmode="require"
            )
            print("Database connection established")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def close(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed")
    
    def execute_query(self, query, params=None):
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params or ())
                try:
                    return cur.fetchall()
                except:
                    return []
        except Exception as e:
            print(f"Error executing query: {e}")
            self.conn.rollback()
            raise
    
    def execute_update(self, query, params=None):
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params or ())
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error executing update: {e}")
            self.conn.rollback()
            raise
    
    def execute_insert(self, query, params=None):
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params or ())
                # Handle ON CONFLICT DO NOTHING case
                result = cur.fetchone()
                if result is None:
                    return None
                inserted_id = result['id']
            self.conn.commit()
            return {"id": inserted_id}
        except Exception as e:
            print(f"Error executing insert: {e}")
            self.conn.rollback()
            raise

db = Database()

def create_tables():
    """Create all necessary tables"""
    try:
        db.execute_update("""
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        db.execute_update("""
        CREATE TABLE IF NOT EXISTS job_categories (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE NOT NULL,
            slug VARCHAR(100) UNIQUE NOT NULL
        )
        """)
        
        db.execute_update("""
        CREATE TABLE IF NOT EXISTS job_locations (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100) NOT NULL,
            country VARCHAR(100) NOT NULL,
            remote BOOLEAN DEFAULT FALSE,
            UNIQUE (city, country)
        )
        """)
        
        db.execute_update("""
        CREATE TABLE IF NOT EXISTS jobs (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            description TEXT NOT NULL,
            requirements TEXT,
            job_type VARCHAR(50) DEFAULT 'full-time',
            salary_min DECIMAL(10, 2),
            salary_max DECIMAL(10, 2),
            salary_currency VARCHAR(10) DEFAULT 'KSh',
            skills_required JSONB,
            company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
            category_id INTEGER REFERENCES job_categories(id) ON DELETE SET NULL,
            location_id INTEGER REFERENCES job_locations(id) ON DELETE SET NULL,
            application_email VARCHAR(255),
            application_url VARCHAR(500),
            posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
        """)
        
        db.execute_update("""
        CREATE TABLE IF NOT EXISTS job_applications (
            id SERIAL PRIMARY KEY,
            job_id INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
            applicant_name VARCHAR(255) NOT NULL,
            applicant_email VARCHAR(255) NOT NULL,
            resume_url TEXT,
            cover_letter TEXT,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        print("All tables created successfully")
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise

def parse_location(location_str):
    """Parse location string to city, country, remote"""
    parts = [p.strip() for p in location_str.split(',') if p.strip()]
    if not parts:
        return "Remote", "Kenya", True
    city = parts[0]
    country = parts[1] if len(parts) > 1 else "Kenya"
    remote = city.lower() == 'remote'
    return city, country, remote

def get_or_create_company(company_name):
    """Get or create a company"""
    result = db.execute_query("SELECT id FROM companies WHERE name = %s", (company_name,))
    if result:
        return result[0]['id']
    return db.execute_insert("INSERT INTO companies (name) VALUES (%s) RETURNING id", (company_name,))['id']

def get_or_create_location(location_str):
    """Get or create a location"""
    city, country, remote = parse_location(location_str)
    result = db.execute_query("SELECT id FROM job_locations WHERE city = %s AND country = %s", (city, country))
    if result:
        return result[0]['id']
    return db.execute_insert(
        "INSERT INTO job_locations (city, country, remote) VALUES (%s, %s, %s) RETURNING id",
        (city, country, remote)
    )['id']

def get_or_create_category(category_name='General'):
    """Get or create a category"""
    slug = category_name.lower().replace(' ', '-')
    result = db.execute_query("SELECT id FROM job_categories WHERE name = %s", (category_name,))
    if result:
        return result[0]['id']
    return db.execute_insert(
        "INSERT INTO job_categories (name, slug) VALUES (%s, %s) RETURNING id",
        (category_name, slug)
    )['id']

def create_job(job_data):
    """Create a new job"""
    company_id = get_or_create_company(job_data['company'])
    location_id = get_or_create_location(job_data['location'])
    category_id = get_or_create_category(job_data.get('category', 'General'))
    
    tags_str = job_data.get('tags', '')
    tags_list = [tag.strip() for tag in tags_str.split(',') if tag.strip()]
    
    query = """
    INSERT INTO jobs (
        title, description, requirements, job_type, salary_min, salary_max, 
        salary_currency, skills_required, company_id, category_id, location_id,
        application_email, application_url, posted_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    """
    params = (
        job_data['title'],
        job_data['description'],
        job_data['requirements'],
        job_data.get('type', 'full-time'),
        job_data.get('salary_min'),
        job_data.get('salary_max'),
        job_data.get('salary_currency', 'KSh'),
        json.dumps(tags_list),
        company_id,
        category_id,
        location_id,
        job_data.get('application_email', ''),
        job_data.get('application_url', ''),
        datetime.now()
    )
    return db.execute_insert(query, params)

def get_jobs(page=1, limit=10, search=None):
    """Get jobs with pagination and search"""
    offset = (page - 1) * limit
    query = """
    SELECT j.*, c.name as company, cat.name as category, l.city, l.country, l.remote
    FROM jobs j
    JOIN companies c ON j.company_id = c.id
    LEFT JOIN job_categories cat ON j.category_id = cat.id
    LEFT JOIN job_locations l ON j.location_id = l.id
    WHERE j.is_active = TRUE
    """
    params = []
    if search:
        query += " AND (j.title ILIKE %s OR j.description ILIKE %s)"
        params.extend([f"%{search}%", f"%{search}%"])
    query += " ORDER BY j.posted_at DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    return db.execute_query(query, tuple(params))

def get_job_by_id(job_id):
    """Get a single job by ID"""
    query = """
    SELECT j.*, c.name as company, cat.name as category, l.city, l.country, l.remote
    FROM jobs j
    JOIN companies c ON j.company_id = c.id
    LEFT JOIN job_categories cat ON j.category_id = cat.id
    LEFT JOIN job_locations l ON j.location_id = l.id
    WHERE j.id = %s AND j.is_active = TRUE
    """
    result = db.execute_query(query, (job_id,))
    return result[0] if result else None

def update_job(job_id, job_data):
    """Update a job"""
    if not get_job_by_id(job_id):
        return False
    
    update_fields = {}
    for key in ['title', 'description', 'requirements', 'job_type', 'salary_min', 'salary_max', 
                'salary_currency', 'application_email', 'application_url']:
        if key in job_data:
            update_fields[key] = job_data[key]
    
    if 'type' in job_data:
        update_fields['job_type'] = job_data['type']
    
    if 'tags' in job_data:
        tags_str = job_data['tags']
        tags_list = [tag.strip() for tag in tags_str.split(',') if tag.strip()]
        update_fields['skills_required'] = json.dumps(tags_list)
    
    if 'company' in job_data:
        update_fields['company_id'] = get_or_create_company(job_data['company'])
    
    if 'location' in job_data:
        update_fields['location_id'] = get_or_create_location(job_data['location'])
    
    if 'category' in job_data:
        update_fields['category_id'] = get_or_create_category(job_data['category'])
    
    if not update_fields:
        return True
    
    set_clause = ', '.join(f"{k} = %s" for k in update_fields)
    params = list(update_fields.values()) + [job_id]
    query = f"UPDATE jobs SET {set_clause} WHERE id = %s"
    db.execute_update(query, tuple(params))
    return True

def delete_job(job_id):
    """Soft delete a job"""
    query = "UPDATE jobs SET is_active = FALSE WHERE id = %s"
    return db.execute_update(query, (job_id,))

def get_job_stats():
    """Get job statistics"""
    stats = {}
    total = db.execute_query("SELECT COUNT(*) as count FROM jobs")
    stats["total_jobs"] = total[0]['count'] if total else 0
    
    active = db.execute_query("SELECT COUNT(*) as count FROM jobs WHERE is_active = TRUE")
    stats["active_jobs"] = active[0]['count'] if active else 0
    
    categories = db.execute_query("""
    SELECT cat.name, COUNT(j.id) as count 
    FROM job_categories cat
    LEFT JOIN jobs j ON cat.id = j.category_id AND j.is_active = TRUE
    GROUP BY cat.name
    """)
    stats["categories"] = {cat['name']: cat['count'] for cat in categories}
    
    locations = db.execute_query("""
    SELECT CONCAT(l.city, ', ', l.country) as loc, COUNT(j.id) as count 
    FROM job_locations l
    LEFT JOIN jobs j ON l.id = j.location_id AND j.is_active = TRUE
    GROUP BY loc
    """)
    stats["locations"] = {loc['loc']: loc['count'] for loc in locations}
    
    return stats