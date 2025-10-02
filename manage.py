import os
import sys
import psycopg
from dotenv import load_dotenv

# Import database functions
from db import create_tables, db, create_job, get_or_create_company, get_or_create_location, get_or_create_category

load_dotenv()

def create_database():
    """Create the database if it doesn't exist"""
    dbname = os.getenv('DATABASE_NAME', 'neondb')
    user = os.getenv('DATABASE_USER', 'neondb_owner')
    password = os.getenv('DATABASE_PASSWORD', 'npg_hCANMIw4u1Db')
    host = os.getenv('DATABASE_HOST', 'ep-red-night-aeq96bnr.c-2.us-east-2.aws.neon.tech')
    
    try:
        conn = psycopg.connect(
            dbname="postgres",
            user=user,
            password=password,
            host=host,
            sslmode="require"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{dbname}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f'CREATE DATABASE "{dbname}"')
            print(f"Database '{dbname}' created successfully")
        else:
            print(f"Database '{dbname}' already exists")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating database: {e}")
        sys.exit(1)

def run_migrations():
    """Run database migrations"""
    print("Running database migrations...")
    try:
        create_tables()
        print("Migrations completed successfully")
    except Exception as e:
        print(f"Error running migrations: {e}")
        sys.exit(1)

def seed_data():
    """Seed the database with initial data"""
    print("Seeding database with initial data...")
    
    try:
        db.conn.autocommit = False
        
        companies = [
            {"name": "Tech Innovations Ltd", "description": "Leading tech company in Kenya"},
            {"name": "Digital Solutions Africa", "description": "Providing digital solutions across Africa"},
            {"name": "Kenya Software Developers", "description": "Top software development company"}
        ]
        
        for company in companies:
            result = db.execute_query("SELECT id FROM companies WHERE name = %s", (company["name"],))
            if not result:
                db.execute_insert(
                    "INSERT INTO companies (name, description) VALUES (%s, %s) RETURNING id",
                    (company["name"], company["description"])
                )
                print(f"Added company: {company['name']}")
            else:
                print(f"Company already exists: {company['name']}")
        
        categories = [
            {"name": "Engineering", "slug": "engineering"},
            {"name": "Design", "slug": "design"},
            {"name": "Marketing", "slug": "marketing"},
            {"name": "Sales", "slug": "sales"}
        ]
        
        for category in categories:
            result = db.execute_query("SELECT id FROM job_categories WHERE name = %s", (category["name"],))
            if not result:
                db.execute_insert(
                    "INSERT INTO job_categories (name, slug) VALUES (%s, %s) RETURNING id",
                    (category["name"], category["slug"])
                )
                print(f"Added category: {category['name']}")
            else:
                print(f"Category already exists: {category['name']}")
        
        locations = [
            {"city": "Nairobi", "country": "Kenya", "remote": False},
            {"city": "Mombasa", "country": "Kenya", "remote": False},
            {"city": "Kisumu", "country": "Kenya", "remote": False},
            {"city": "Remote", "country": "Kenya", "remote": True}
        ]
        
        for location in locations:
            result = db.execute_query(
                "SELECT id FROM job_locations WHERE city = %s AND country = %s",
                (location["city"], location["country"])
            )
            if not result:
                db.execute_insert(
                    "INSERT INTO job_locations (city, country, remote) VALUES (%s, %s, %s) RETURNING id",
                    (location["city"], location["country"], location["remote"])
                )
                print(f"Added location: {location['city']}, {location['country']}")
            else:
                print(f"Location already exists: {location['city']}, {location['country']}")
        
        sample_jobs = [
            {
                "title": "Senior Python Developer",
                "company": "Tech Innovations Ltd",
                "location": "Nairobi, Kenya",
                "type": "full-time",
                "description": "We are looking for an experienced Python developer...",
                "requirements": "5+ years of Python experience, Django framework knowledge",
                "salary_min": 80000,
                "salary_max": 120000,
                "salary_currency": "KSh",
                "tags": "Python, Django, PostgreSQL, REST API",
                "application_email": "hr@techinnovations.co.ke",
                "application_url": "",
                "category": "Engineering"
            },
            {
                "title": "UI/UX Designer",
                "company": "Digital Solutions Africa",
                "location": "Remote, Kenya",
                "type": "full-time",
                "description": "Join our design team to create amazing user experiences...",
                "requirements": "3+ years of design experience, proficiency in Figma",
                "salary_min": 60000,
                "salary_max": 90000,
                "salary_currency": "KSh",
                "tags": "UI, UX, Figma, Design Thinking",
                "application_email": "",
                "application_url": "https://apply.example.com/ux-designer",
                "category": "Design"
            }
        ]
        
        for job in sample_jobs:
            try:
                result = create_job(job)
                print(f"Added job: {job['title']} (ID: {result['id']})")
            except Exception as e:
                print(f"Error adding job {job['title']}: {e}")
        
        db.conn.commit()
        print("Database seeding completed successfully")
        
    except Exception as e:
        db.conn.rollback()
        print(f"Error seeding database: {e}")
        sys.exit(1)
    finally:
        db.conn.autocommit = True

def reset_database():
    """Reset the database by dropping all tables and recreating them"""
    print("Resetting database...")
    
    try:
        tables = [
            "job_applications",
            "jobs",
            "job_locations",
            "job_categories",
            "companies"
        ]
        
        for table in tables:
            db.execute_update(f"DROP TABLE IF EXISTS {table} CASCADE")
            print(f"Dropped table: {table}")
        
        create_tables()
        print("Database reset completed successfully")
        
    except Exception as e:
        print(f"Error resetting database: {e}")
        sys.exit(1)

def run_server():
    """Run the FastAPI server"""
    port = int(os.getenv("PORT", 8000))  # Use Render's PORT or default to 8000
    print(f"Starting server on http://0.0.0.0:{port}")
    try:
        import uvicorn
        uvicorn.run("server:app", host="0.0.0.0", port=port, reload=os.getenv('DEBUG', 'False').lower() == 'true')
    except ImportError:
        print("Error: uvicorn not installed. Install with 'pip install uvicorn'")
        sys.exit(1)
    except Exception as e:
        print(f"Error starting server: {e}")
        sys.exit(1)

def main():
    """Main command-line interface"""
    if len(sys.argv) < 2:
        print("Usage: python manage.py [command]")
        print("Commands:")
        print("  createdb   - Create the database")
        print("  migrate    - Run database migrations")
        print("  seed       - Seed the database with sample data")
        print("  reset      - Reset the database")
        print("  start      - Start the FastAPI server")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "createdb":
        create_database()
    elif command == "migrate":
        run_migrations()
    elif command == "seed":
        seed_data()
    elif command == "reset":
        reset_database()
    elif command == "start":
        run_server()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()