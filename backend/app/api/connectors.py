import json

from sqlalchemy import or_

from typing import Optional, Any
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from sqlalchemy.orm import Session
import shutil
from pathlib import Path

from app.database import get_db
from app.models import Connector, MappingDocument
from app.config import UPLOAD_DIR
from app.connectors.registry import create_connected_connector, list_supported_types

router = APIRouter()


class ConnectorCreate(BaseModel):
    name: str
    type: str  # "postgresql" | "csv" | "excel" | ...
    config: dict  # {host, port, user, password, database} or {file_path, file_type}


class ConnectorUpdate(BaseModel):
    name: Optional[str] = None
    config: Optional[dict] = None


@router.put("/{connector_id}")
def update_connector(connector_id: int, data: ConnectorUpdate, db: Session = Depends(get_db)):
    connector = db.query(Connector).filter(Connector.id == connector_id).first()
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    
    if data.name is not None:
        connector.name = data.name
    if data.config is not None:
        connector.config = json.dumps(data.config)
        
    db.commit()
    db.refresh(connector)
    return {"id": connector.id, "name": connector.name, "type": connector.type}





@router.get("/types")
def get_supported_types():
    return {"types": list_supported_types()}


@router.get("/")
def list_connectors(db: Session = Depends(get_db)):
    connectors = db.query(Connector).all()
    return [
        {
            "id": c.id, 
            "name": c.name, 
            "type": c.type, 
            "config": c.get_config(),
            "created_at": str(c.created_at)
        }
        for c in connectors
    ]


@router.post("/")
def create_connector(data: ConnectorCreate, db: Session = Depends(get_db)):
    connector = Connector(
        name=data.name,
        type=data.type,
        config=json.dumps(data.config),
    )
    db.add(connector)
    db.commit()
    db.refresh(connector)
    return {"id": connector.id, "name": connector.name, "type": connector.type}


@router.post("/upload")
def upload_connector_file(
    name: str = Form(...),
    type: str = Form(...),
    files: list[UploadFile] = File(...),
    db: Session = Depends(get_db)
):
    # Ensure connectors upload directory exists
    conn_base_dir = UPLOAD_DIR / "connectors"
    conn_base_dir.mkdir(parents=True, exist_ok=True)

    # Create a unique directory for this connector's files
    import uuid
    connector_id_slug = str(uuid.uuid4())[:8]
    conn_upload_dir = conn_base_dir / f"{name.replace(' ', '_')}_{connector_id_slug}"
    conn_upload_dir.mkdir(parents=True, exist_ok=True)

    # Save all files
    for file in files:
        file_path = conn_upload_dir / file.filename
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

    # Create connector
    # If there's only one file, we can still use the directory approach for consistency
    config = {
        "file_path": str(conn_upload_dir.absolute()),
        "file_type": type,
        "is_multi_file": True
    }
    
    connector = Connector(
        name=name,
        type=type,
        config=json.dumps(config),
    )
    db.add(connector)
    db.commit()
    db.refresh(connector)
    
    return {"id": connector.id, "name": connector.name, "type": connector.type}


@router.post("/{connector_id}/test")
def test_connection(connector_id: int, db: Session = Depends(get_db)):
    connector = db.query(Connector).filter(Connector.id == connector_id).first()
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    try:
        config = connector.get_config()
        config["type"] = connector.type
        conn = create_connected_connector(connector.type, config)
        success = conn.test_connection()
        conn.disconnect()
        return {"success": success, "message": "Connection successful" if success else "Connection failed"}
    except Exception as e:
        return {"success": False, "message": str(e)}


@router.get("/{connector_id}/schema")
def get_schema(connector_id: int, db: Session = Depends(get_db)):
    connector = db.query(Connector).filter(Connector.id == connector_id).first()
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    try:
        config = connector.get_config()
        config["type"] = connector.type
        conn = create_connected_connector(connector.type, config)
        tables = conn.get_tables()
        schema = {}
        for table in tables:
            schema[table] = conn.get_columns(table)
        conn.disconnect()
        return {"tables": schema}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{connector_id}/usage")
def get_connector_usage(connector_id: int, db: Session = Depends(get_db)):
    """Return mapping documents that use this connector as source or target."""
    connector = db.query(Connector).filter(Connector.id == connector_id).first()
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    linked = db.query(MappingDocument).filter(
        or_(
            MappingDocument.source_connector_id == connector_id,
            MappingDocument.target_connector_id == connector_id,
        ),
        MappingDocument.is_active == True,
    ).all()
    return {
        "linked_mappings": [
            {
                "id": m.id,
                "name": m.name,
                "role": (
                    "source & target" if m.source_connector_id == connector_id and m.target_connector_id == connector_id
                    else "source" if m.source_connector_id == connector_id
                    else "target"
                ),
            }
            for m in linked
        ]
    }


@router.delete("/{connector_id}")
def delete_connector(connector_id: int, db: Session = Depends(get_db)):
    connector = db.query(Connector).filter(Connector.id == connector_id).first()
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    # Nullify FK references in mapping documents before deleting
    db.query(MappingDocument).filter(
        MappingDocument.source_connector_id == connector_id
    ).update({"source_connector_id": None})
    db.query(MappingDocument).filter(
        MappingDocument.target_connector_id == connector_id
    ).update({"target_connector_id": None})

    db.delete(connector)
    db.commit()
    return {"message": "Deleted"}
