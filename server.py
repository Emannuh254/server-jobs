from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, field_validator
from typing import List, Optional
import os
import json
from datetime import datetime
from db import get_jobs, get_job_by_id, create_job, update_job, delete_job, get_job_stats, db
from dotenv import load_dotenv

load_dotenv()

class JobCreate(BaseModel):
    model_config = ConfigDict(extra="allow")
    title: str
    company: str
    location: str
    type: Optional[str] = "full-time"
    description: str
    requirements: str
    salary: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: Optional[str] = "KSh"
    tags: Optional[str] = ""
    application_email: Optional[str] = ""
    application_link: Optional[str] = ""
    application_url: Optional[str] = ""
    category: Optional[str] = "General"

    @field_validator('salary_max')
    @classmethod
    def salary_max_gt_min(cls, v, values):
        if 'salary_min' in values and values['salary_min'] is not None and v is not None:
            if v < values['salary_min']:
                raise ValueError('salary_max must be greater than or equal to salary_min')
        return v

    @field_validator('salary', mode='before')
    @classmethod
    def parse_salary(cls, v, values):
        if v and isinstance(v, str):
            try:
                parts = v.replace('KSh', '').replace(',', '').strip().split('-')
                if len(parts) == 2:
                    values['salary_min'] = float(parts[0])
                    values['salary_max'] = float(parts[1])
                else:
                    values['salary_min'] = float(parts[0])
                    values['salary_max'] = float(parts[0])
            except (ValueError, IndexError):
                raise ValueError('Invalid salary format. Use "min-max" or single value.')
        return v

    @field_validator('application_url', mode='before')
    @classmethod
    def sync_application_link(cls, v, values):
        if 'application_link' in values and values['application_link']:
            return values['application_link']
        return v

class JobUpdate(BaseModel):
    model_config = ConfigDict(extra="allow")
    title: Optional[str] = None
    company: Optional[str] = None
    location: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None
    requirements: Optional[str] = None
    salary: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: Optional[str] = None
    tags: Optional[str] = None
    application_email: Optional[str] = None
    application_link: Optional[str] = None
    application_url: Optional[str] = None
    category: Optional[str] = None

    @field_validator('salary_max')
    @classmethod
    def salary_max_gt_min(cls, v, values):
        if 'salary_min' in values and values['salary_min'] is not None and v is not None:
            if v < values['salary_min']:
                raise ValueError('salary_max must be greater than or equal to salary_min')
        return v

    @field_validator('salary', mode='before')
    @classmethod
    def parse_salary(cls, v, values):
        if v and isinstance(v, str):
            try:
                parts = v.replace('KSh', '').replace(',', '').strip().split('-')
                if len(parts) == 2:
                    values['salary_min'] = float(parts[0])
                    values['salary_max'] = float(parts[1])
                else:
                    values['salary_min'] = float(parts[0])
                    values['salary_max'] = float(parts[0])
            except (ValueError, IndexError):
                raise ValueError('Invalid salary format. Use "min-max" or single value.')
        return v

    @field_validator('application_url', mode='before')
    @classmethod
    def sync_application_link(cls, v, values):
        if 'application_link' in values and values['application_link']:
            return values['application_link']
        return v

class JobResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str
    title: str
    company: str
    location: str
    type: str
    description: str
    requirements: str
    salary: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: str
    tags: List[str]
    date_posted: Optional[str] = None
    application_email: Optional[str] = None
    application_link: Optional[str] = None
    application_url: Optional[str] = None
    category: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    remote: Optional[bool] = None

app = FastAPI(title="Jobs Parlour API", version="1.0.0")

# Configure CORS from .env
cors_origins = os.getenv('CORS_ALLOWED_ORIGINS', 'http://localhost:5500,http://127.0.0.1:5500,https://emannuh254.github.io').split(',')
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def format_job(job_data: dict):
    """Format job data for response"""
    if not job_data:
        return None
    
    skills = job_data.get('skills_required')
    if isinstance(skills, str):
        try:
            tags = json.loads(skills)
        except json.JSONDecodeError:
            tags = [tag.strip() for tag in skills.split(',') if tag.strip()]
    else:
        tags = skills if isinstance(skills, list) else []
    
    location = f"{job_data.get('city', '')}, {job_data.get('country', '')}" if job_data.get('city') and not job_data.get('remote') else "Remote"
    
    salary = None
    if job_data.get('salary_min') and job_data.get('salary_max'):
        if job_data['salary_min'] == job_data['salary_max']:
            salary = f"{job_data['salary_currency']} {job_data['salary_min']:,.0f}"
        else:
            salary = f"{job_data['salary_currency']} {job_data['salary_min']:,.0f}-{job_data['salary_max']:,.0f}"
    
    return JobResponse(
        id=str(job_data['id']),
        title=job_data['title'],
        company=job_data['company'],
        location=location,
        type=job_data['job_type'],
        description=job_data['description'],
        requirements=job_data.get('requirements', ''),
        salary=salary,
        salary_min=job_data.get('salary_min'),
        salary_max=job_data.get('salary_max'),
        salary_currency=job_data.get('salary_currency', 'KSh'),
        tags=tags,
        date_posted=job_data['posted_at'].strftime('%Y-%m-%d %H:%M') if job_data.get('posted_at') else None,
        application_email=job_data.get('application_email', ''),
        application_link=job_data.get('application_url', ''),
        application_url=job_data.get('application_url', ''),
        category=job_data.get('category', ''),
        city=job_data.get('city', ''),
        country=job_data.get('country', ''),
        remote=job_data.get('remote', False)
    )

@app.get("/")
async def root():
    return {"message": "Jobs Parlour API", "version": "1.0.0"}

@app.get("/api/jobs/", response_model=dict)
async def get_jobs_list(page: int = 1, limit: int = 10, search: Optional[str] = None):
    try:
        jobs_data = get_jobs(page=page, limit=limit, search=search)
        total = len(jobs_data)  # Consider using a COUNT query for efficiency
        return {
            "results": [format_job(job) for job in jobs_data],
            "page": page,
            "limit": limit,
            "total": total
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/jobs/{job_id}", response_model=JobResponse)
async def get_job_detail(job_id: int):
    try:
        job_data = get_job_by_id(job_id)
        if not job_data:
            raise HTTPException(status_code=404, detail="Job not found")
        return format_job(job_data)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/jobs/", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
@app.post("/api/jobs/post/", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_new_job(job: JobCreate):
    try:
        job_data = job.model_dump(exclude={'salary', 'application_link'})
        result = create_job(job_data)
        if not result or not result['id']:
            raise ValueError("Failed to create job")
        created_job = get_job_by_id(result['id'])
        return format_job(created_job)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/jobs/{job_id}", response_model=JobResponse)
async def update_job_detail(job_id: int, job: JobUpdate):
    try:
        job_data = job.model_dump(exclude_unset=True, exclude={'salary', 'application_link'})
        success = update_job(job_id, job_data)
        if not success:
            raise HTTPException(status_code=404, detail="Job not found")
        updated_job = get_job_by_id(job_id)
        return format_job(updated_job)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/jobs/{job_id}")
async def delete_job_detail(job_id: int):
    try:
        success = delete_job(job_id)
        if not success:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"message": "Job deleted successfully"}
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/")
async def get_statistics():
    try:
        return get_job_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
async def health_check():
    try:
        db.execute_query("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})