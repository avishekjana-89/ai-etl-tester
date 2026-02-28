from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import TestCase

router = APIRouter()


@router.get("/{mapping_document_id}")
def list_test_cases(mapping_document_id: int, db: Session = Depends(get_db)):
    cases = db.query(TestCase).filter(
        TestCase.mapping_document_id == mapping_document_id,
        TestCase.is_active == True
    ).all()
    return [
        {
            "id": tc.id,
            "name": tc.name,
            "type": tc.type,
            "source_sql": tc.source_sql,
            "target_sql": tc.target_sql,
            "description": tc.description,
            "validation_status": tc.validation_status,
        }
        for tc in cases
    ]


class CreateTestCaseRequest(BaseModel):
    mapping_document_id: int
    name: str
    type: str = "custom"
    source_sql: str | None = None
    target_sql: str | None = None
    description: str | None = None

@router.post("/")
def create_test_case(data: CreateTestCaseRequest, db: Session = Depends(get_db)):
    tc = TestCase(
        mapping_document_id=data.mapping_document_id,
        name=data.name,
        type=data.type,
        source_sql=data.source_sql,
        target_sql=data.target_sql,
        description=data.description,
        validation_status="manual",
        is_active=True
    )
    db.add(tc)
    db.commit()
    db.refresh(tc)
    return {
        "id": tc.id,
        "name": tc.name,
        "type": tc.type,
        "source_sql": tc.source_sql,
        "target_sql": tc.target_sql,
        "description": tc.description,
        "validation_status": tc.validation_status,
    }

class UpdateTestCaseRequest(BaseModel):
    source_sql: str | None = None
    target_sql: str | None = None
    validation_status: str | None = None

@router.put("/{test_case_id}")
def update_test_case(test_case_id: int, data: UpdateTestCaseRequest, db: Session = Depends(get_db)):
    tc = db.query(TestCase).filter(TestCase.id == test_case_id, TestCase.is_active == True).first()
    if not tc:
        raise HTTPException(status_code=404, detail="Test case not found")
    if data.source_sql is not None:
        tc.source_sql = data.source_sql if data.source_sql.strip() else None
    if data.target_sql is not None:
        tc.target_sql = data.target_sql
    if data.validation_status is not None:
        tc.validation_status = data.validation_status
    db.commit()
    return {"id": tc.id, "message": "Updated"}

@router.delete("/{test_case_id}")
def delete_test_case(test_case_id: int, db: Session = Depends(get_db)):
    tc = db.query(TestCase).filter(TestCase.id == test_case_id).first()
    if not tc:
        raise HTTPException(status_code=404, detail="Test case not found")
    tc.is_active = False
    db.commit()
    return {"message": "Test case deleted successfully"}
