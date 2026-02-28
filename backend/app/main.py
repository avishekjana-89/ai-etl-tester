import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import setup_logging
from app.api import connectors, mappings, testcases, runs

setup_logging()

app = FastAPI(
    title="ETL Testing SaaS",
    description="AI-powered ETL test case generation and execution",
    version="1.0.0",
)

# CORS Configuration
origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173,http://localhost:3000,http://localhost").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(connectors.router, prefix="/api/connectors", tags=["Connectors"])
app.include_router(mappings.router, prefix="/api/mappings", tags=["Mappings"])
app.include_router(testcases.router, prefix="/api/testcases", tags=["Test Cases"])
app.include_router(runs.router, prefix="/api/runs", tags=["Test Runs"])


@app.on_event("startup")
def on_startup():
    pass  # Migrations are handled by entrypoint.sh in Docker.
          # For local dev: run `venv/bin/alembic upgrade head` before starting uvicorn.


@app.get("/api/health")
def health_check():
    return {"status": "ok", "version": "1.0.0"}
