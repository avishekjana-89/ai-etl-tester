from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Connector, MappingDocument, TestCase, TestRun, TestResult
from app.connectors.registry import create_connected_connector
from app.engine.executor import execute_test_case

router = APIRouter()


from typing import Optional

class RunRequest(BaseModel):
    mapping_document_id: int
    source_connector_override: Optional[int] = None
    target_connector_override: Optional[int] = None
    parameters: Optional[dict[str, str]] = None


@router.post("/")
def create_run(data: RunRequest, db: Session = Depends(get_db)):
    doc = db.query(MappingDocument).filter(MappingDocument.id == data.mapping_document_id).first()
    if not doc:
        raise HTTPException(status_code=404, detail="Mapping document not found")

    test_cases = db.query(TestCase).filter(
        TestCase.mapping_document_id == doc.id,
        TestCase.is_active == True
    ).all()
    if not test_cases:
        raise HTTPException(status_code=400, detail="No active test cases found. Generate them first.")

    # Create test run
    run = TestRun(
        mapping_document_id=doc.id,
        total_cases=len(test_cases),
        status="running",
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    # Connect to source and target
    src_conn = None
    tgt_conn = None
    try:
        # Pre-flight: detect missing (deleted) connectors before running tests
        effective_src_id = data.source_connector_override or doc.source_connector_id
        effective_tgt_id = data.target_connector_override or doc.target_connector_id

        # Source missing check: only fail if any test case actually needs source SQL
        if effective_src_id is None:
            has_source_sql = any(tc.source_sql for tc in test_cases)
            if has_source_sql:
                run.status = "failed"
                run.completed_at = datetime.now(timezone.utc)
                db.commit()
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Source connector is missing — it may have been deleted. "
                        "Please open ⚙️ Run Settings and select a source connector override before executing."
                    ),
                )

        # Target missing check: always required
        if effective_tgt_id is None:
            run.status = "failed"
            run.completed_at = datetime.now(timezone.utc)
            db.commit()
            raise HTTPException(
                status_code=400,
                detail=(
                    "Target connector is missing — it may have been deleted. "
                    "Please open ⚙️ Run Settings and select a target connector override before executing."
                ),
            )

        # Source Connection logic
        src_id = effective_src_id
        if src_id:
            src_model = db.query(Connector).filter(Connector.id == src_id).first()
            if src_model:
                config = src_model.get_config()
                config["type"] = src_model.type
                src_conn = create_connected_connector(src_model.type, config)

        # Target Connection logic
        tgt_id = effective_tgt_id
        if tgt_id:
            tgt_model = db.query(Connector).filter(Connector.id == tgt_id).first()
            if tgt_model:
                config = tgt_model.get_config()
                config["type"] = tgt_model.type
                tgt_conn = create_connected_connector(tgt_model.type, config)

        # Execute each test case
        passed = 0
        failed = 0
        for tc in test_cases:
            result = execute_test_case(tc, src_conn, tgt_conn, data.parameters)
            result.test_run_id = run.id
            db.add(result)

            if result.passed:
                passed += 1
            else:
                failed += 1

        run.passed_cases = passed
        run.failed_cases = failed
        run.status = "completed"
        run.completed_at = datetime.now(timezone.utc)
        db.commit()

        return {
            "run_id": run.id,
            "status": "completed",
            "total": run.total_cases,
            "passed": passed,
            "failed": failed,
        }

    except Exception as e:
        run.status = "failed"
        run.completed_at = datetime.now(timezone.utc)
        db.commit()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if src_conn:
            src_conn.disconnect()
        if tgt_conn:
            tgt_conn.disconnect()


from sqlalchemy.orm import joinedload

@router.get("/{run_id}")
def get_run(run_id: int, db: Session = Depends(get_db)):
    run = db.query(TestRun).options(joinedload(TestRun.mapping_document)).filter(TestRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Test run not found")

    results = db.query(TestResult).options(joinedload(TestResult.test_case)).filter(TestResult.test_run_id == run_id).all()

    return {
        "id": run.id,
        "status": run.status,
        "total_cases": run.total_cases,
        "passed_cases": run.passed_cases,
        "failed_cases": run.failed_cases,
        "mapping_name": run.mapping_document.name if run.mapping_document else "Unknown Mapping",
        "started_at": str(run.started_at),
        "completed_at": str(run.completed_at) if run.completed_at else None,
        "results": [
            {
                "id": r.id,
                "test_case_id": r.test_case_id,
                "test_case_name": r.test_case.name if r.test_case else 'Deleted Test',
                "test_case_type": r.test_case.type if r.test_case else 'unknown',
                "passed": r.passed,
                "source_value": r.source_value,
                "target_value": r.target_value,
                "mismatch_count": r.mismatch_count,
                "mismatch_sample": r.mismatch_sample,
                "error_message": r.error_message,
            }
            for r in results
        ],
    }


@router.get("/")
def list_runs(skip: int = 0, limit: int = 20, db: Session = Depends(get_db)):
    total = db.query(TestRun).count()
    runs = db.query(TestRun).options(joinedload(TestRun.mapping_document)).order_by(TestRun.id.desc()).offset(skip).limit(limit).all()
    return {
        "total": total,
        "runs": [
            {
                "id": r.id,
                "mapping_document_id": r.mapping_document_id,
                "mapping_name": r.mapping_document.name if r.mapping_document else "Unknown Mapping",
                "status": r.status,
                "total_cases": r.total_cases,
                "passed_cases": r.passed_cases,
                "failed_cases": r.failed_cases,
                "started_at": str(r.started_at),
                "completed_at": str(r.completed_at) if r.completed_at else None,
            }
            for r in runs
        ]
    }


@router.delete("/{run_id}")
def delete_run(run_id: int, db: Session = Depends(get_db)):
    run = db.query(TestRun).filter(TestRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Test run not found")
    
    # Results will be deleted automatically if cascade ONDELETE is set, 
    # but let's be explicit to ensure cleanup.
    db.query(TestResult).filter(TestResult.test_run_id == run_id).delete()
    db.delete(run)
    db.commit()
    return {"message": "Test run deleted successfully"}
