import shutil
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session

from app.database import get_db
from app.config import UPLOAD_DIR
from app.models import Connector, MappingDocument, FieldMapping, TestCase
from app.connectors.registry import create_connected_connector
from app.ai.parser import analyze_mapping_document

router = APIRouter()


def _read_file_content(file_path: str) -> str:
    """Read mapping doc content as text. Supports CSV, TSV, Excel, TXT."""
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext == ".md":
        return path.read_text(encoding="utf-8", errors="ignore")

    if ext in (".csv", ".tsv", ".txt"):
        try:
            import pandas as pd
            import io
            # For .txt, try multiple delimiters
            if ext == ".txt":
                # Try reading with common delimiters
                text = path.read_text(encoding="utf-8", errors="ignore")
                # Auto-detect pipe vs tab vs comma if it looks tabular
                if "|" in text:
                    # Logic to handle pipe-delimited files that might contain '||' in rules
                    # We split by pipes that ARE NOT adjacent to another pipe
                    import re
                    lines = [l.strip().strip("|") for l in text.split("\n") if l.strip()]
                    # Filter out the dash rows (---|---|---)
                    lines = [l for l in lines if not re.match(r'^[ \-\|]+$', l)]
                    
                    data = []
                    for line in lines:
                        # Split by pipe but NOT double-pipe
                        parts = re.split(r'(?<!\|)\|(?!\|)', line)
                        data.append([p.strip() for p in parts])
                    
                    if data:
                        df = pd.DataFrame(data[1:], columns=data[0])
                    else:
                        df = pd.DataFrame()
                else:
                    df = pd.read_csv(io.StringIO(text), sep=None, engine='python', skipinitialspace=True)
            else:
                df = pd.read_csv(file_path, sep=None, engine='python', skipinitialspace=True)
            
            # Clean up whitespace in all string columns and headers
            df.columns = [c.strip() for c in df.columns]
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            
            # Aggressive cleanup: Replace actual NaNs and common "null-like" strings
            df = df.fillna('')
            # Replace case-insensitive 'nan', 'none', 'null' with empty strings in all object columns
            df = df.apply(lambda x: x.replace(['nan', 'NaN', 'None', 'null', 'NULL'], '') if x.dtype == "object" else x)
            
            # Convert to Markdown table
            return df.to_markdown(index=False)
        except Exception as e:
            print(f"Tabular parsing failed, falling back to raw text: {e}")
            return path.read_text(encoding="utf-8", errors="ignore")
    elif ext in (".xlsx", ".xls"):
        import pandas as pd
        df = pd.read_excel(file_path)
        df = df.fillna('')
        df = df.apply(lambda x: x.replace(['nan', 'NaN', 'None', 'null', 'NULL'], '') if x.dtype == "object" else x)
        return df.to_markdown(index=False)
    else:
        return path.read_text(encoding="utf-8", errors="ignore")


@router.get("/")
def list_mapping_documents(db: Session = Depends(get_db)):
    docs = db.query(MappingDocument).filter(MappingDocument.is_active == True).all()
    return [
        {
            "id": d.id,
            "name": d.name,
            "status": d.status,
            "source_connector_id": d.source_connector_id,
            "target_connector_id": d.target_connector_id,
            "created_at": str(d.created_at),
        }
        for d in docs
    ]


@router.post("/upload")
def upload_mapping(
    file: UploadFile = File(...),
    source_connector_id: int = Form(...),
    target_connector_id: int = Form(...),
    db: Session = Depends(get_db),
):
    # Save file
    file_path = UPLOAD_DIR / file.filename
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    doc = MappingDocument(
        name=file.filename,
        file_path=str(file_path),
        source_connector_id=source_connector_id,
        target_connector_id=target_connector_id,
        status="uploaded",
        is_active=True
    )
    db.add(doc)
    db.commit()
    db.refresh(doc)

    return {"id": doc.id, "name": doc.name, "status": doc.status}


@router.post("/{mapping_id}/parse")
async def parse_mapping(mapping_id: int, db: Session = Depends(get_db)):
    """
    Single-stage AI analysis: parses the mapping document and generates
    both field mappings AND test cases with validated SQL in one AI call.
    """
    doc = db.query(MappingDocument).filter(MappingDocument.id == mapping_id, MappingDocument.is_active == True).first()
    if not doc:
        raise HTTPException(status_code=404, detail="Mapping document not found")

    # Update status
    doc.status = "parsing"
    db.commit()

    source_conn = None
    target_conn = None

    try:
        # Read the document content
        content = _read_file_content(doc.file_path)

        # Connect to source and target databases
        source_db_type = "postgresql"
        target_db_type = "mysql"

        if doc.source_connector_id:
            src_conn_model = db.query(Connector).filter(Connector.id == doc.source_connector_id).first()
            if src_conn_model:
                source_db_type = src_conn_model.type
                config = src_conn_model.get_config()
                config["type"] = src_conn_model.type
                source_conn = create_connected_connector(src_conn_model.type, config)

        if doc.target_connector_id:
            tgt_conn_model = db.query(Connector).filter(Connector.id == doc.target_connector_id).first()
            if tgt_conn_model:
                target_db_type = tgt_conn_model.type
                config = tgt_conn_model.get_config()
                config["type"] = tgt_conn_model.type
                target_conn = create_connected_connector(tgt_conn_model.type, config)

        # Delete any previous field mappings and test cases for re-parse
        db.query(TestCase).filter(TestCase.mapping_document_id == doc.id).delete()
        db.query(FieldMapping).filter(FieldMapping.mapping_document_id == doc.id).delete()

        # Single AI call — produces both field mappings and test cases
        result = await analyze_mapping_document(
            document_content=content,
            source_conn=source_conn,
            target_conn=target_conn,
            source_db_type=source_db_type,
            target_db_type=target_db_type,
        )

        # Store field mappings
        field_mappings = result.get("field_mappings", [])
        for m in field_mappings:
            def _to_str(val, default=""):
                if isinstance(val, list):
                    return ", ".join(str(v) for v in val)
                return str(val) if val is not None else default

            def _to_str_or_none(val):
                if isinstance(val, list):
                    return ", ".join(str(v) for v in val)
                return str(val) if val is not None else None

            field_mapping = FieldMapping(
                mapping_document_id=doc.id,
                source_table=_to_str(m.get("source_table")),
                source_column=_to_str(m.get("source_column")),
                target_table=_to_str(m.get("target_table")),
                target_column=_to_str(m.get("target_column")),
                transformation=_to_str(m.get("transformation", "DIRECT"), "DIRECT"),
                source_sql_expr=_to_str_or_none(m.get("source_sql_expr")),
                target_sql_expr=_to_str_or_none(m.get("target_sql_expr")) or _to_str(m.get("target_column")),
                dq_rule=_to_str_or_none(m.get("dq_rule")),
                test_notes=_to_str_or_none(m.get("test_notes")),
                is_key=bool(m.get("is_key", False)),
            )
            db.add(field_mapping)

        # Store test cases
        test_cases = result.get("test_cases", [])
        for tc in test_cases:
            test_case = TestCase(
                mapping_document_id=doc.id,
                name=tc.get("name", "Unnamed Test"),
                type=tc.get("type", "value_match"),
                source_sql=tc.get("source_sql"),
                target_sql=tc.get("target_sql"),
                description=tc.get("description", ""),
                validation_status=tc.get("validation_status", "pending"),
                is_active=True
            )
            db.add(test_case)

        doc.status = "parsed"
        db.commit()

        return {
            "id": doc.id,
            "status": "parsed",
            "field_mappings_count": len(field_mappings),
            "test_cases_count": len(test_cases),
            "valid_tests": sum(1 for tc in test_cases if tc.get("validation_status") == "valid"),
            "invalid_tests": sum(1 for tc in test_cases if tc.get("validation_status") == "invalid"),
        }

    except Exception as e:
        doc.status = "error"
        doc.error_message = str(e)
        db.commit()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if source_conn:
            source_conn.disconnect()
        if target_conn:
            target_conn.disconnect()


@router.get("/{mapping_id}")
def get_mapping(mapping_id: int, db: Session = Depends(get_db)):
    doc = db.query(MappingDocument).filter(MappingDocument.id == mapping_id, MappingDocument.is_active == True).first()
    if not doc:
        raise HTTPException(status_code=404, detail="Mapping document not found")

    field_mappings = db.query(FieldMapping).filter(FieldMapping.mapping_document_id == mapping_id).all()
    return {
        "id": doc.id,
        "name": doc.name,
        "status": doc.status,
        "error_message": doc.error_message,
        "field_mappings": [
            {
                "id": fm.id,
                "source_table": fm.source_table,
                "source_column": fm.source_column,
                "target_table": fm.target_table,
                "target_column": fm.target_column,
                "transformation": fm.transformation,
                "source_sql_expr": fm.source_sql_expr,
                "target_sql_expr": fm.target_sql_expr,
                "dq_rule": fm.dq_rule,
                "test_notes": fm.test_notes,
                "is_key": fm.is_key,
            }
            for fm in field_mappings
        ],
    }


@router.put("/{mapping_id}/fields/{field_id}")
def update_field_mapping(
    mapping_id: int,
    field_id: int,
    data: dict,
    db: Session = Depends(get_db),
):
    doc = db.query(MappingDocument).filter(MappingDocument.id == mapping_id, MappingDocument.is_active == True).first()
    if not doc:
         raise HTTPException(status_code=404, detail="Mapping document not found")

    fm = db.query(FieldMapping).filter(
        FieldMapping.id == field_id,
        FieldMapping.mapping_document_id == mapping_id,
    ).first()
    if not fm:
        raise HTTPException(status_code=404, detail="Field mapping not found")

    for key in ["source_table", "source_column", "target_table", "target_column",
                "transformation", "source_sql_expr", "target_sql_expr", "dq_rule", "test_notes", "is_key"]:
        if key in data:
            setattr(fm, key, data[key])
    db.commit()
    return {"message": "Updated"}


@router.delete("/{mapping_id}")
def delete_mapping(mapping_id: int, db: Session = Depends(get_db)):
    doc = db.query(MappingDocument).filter(MappingDocument.id == mapping_id).first()
    if not doc:
        raise HTTPException(status_code=404, detail="Mapping document not found")
        
    doc.is_active = False

    # Also soft-delete all associated TestCases so they immediately disappear
    # from the TestCases endpoint without wiping historical results/runs.
    test_cases = db.query(TestCase).filter(TestCase.mapping_document_id == mapping_id).all()
    for tc in test_cases:
        tc.is_active = False

    db.commit()
    return {"message": "Mapping document and all active test cases soft-deleted successfully"}
