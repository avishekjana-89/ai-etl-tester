import json
from datetime import datetime, timezone

from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship

from app.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class Connector(Base):
    __tablename__ = "connectors"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False)  # "postgresql" | "csv"
    config = Column(Text, nullable=False)  # JSON string (encrypted in prod)
    created_at = Column(DateTime, default=utcnow)

    def get_config(self) -> dict:
        return json.loads(self.config)


class MappingDocument(Base):
    __tablename__ = "mapping_documents"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    status = Column(String(50), default="uploaded")  # uploaded | parsing | parsed | error
    source_connector_id = Column(Integer, ForeignKey("connectors.id", ondelete="SET NULL"), nullable=True)
    target_connector_id = Column(Integer, ForeignKey("connectors.id", ondelete="SET NULL"), nullable=True)
    error_message = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=utcnow)

    source_connector = relationship("Connector", foreign_keys=[source_connector_id])
    target_connector = relationship("Connector", foreign_keys=[target_connector_id])
    field_mappings = relationship("FieldMapping", back_populates="mapping_document", cascade="all, delete-orphan")
    test_cases = relationship("TestCase", back_populates="mapping_document", cascade="all, delete-orphan")
    test_runs = relationship("TestRun", back_populates="mapping_document", cascade="all, delete-orphan")


class FieldMapping(Base):
    __tablename__ = "field_mappings"

    id = Column(Integer, primary_key=True, index=True)
    mapping_document_id = Column(Integer, ForeignKey("mapping_documents.id"), nullable=False)
    source_table = Column(String(255), nullable=False)
    source_column = Column(String(255), nullable=False)
    target_table = Column(String(255), nullable=False)
    target_column = Column(String(255), nullable=False)
    transformation = Column(String(500), default="DIRECT")  # Human-readable rule
    source_sql_expr = Column(Text, nullable=True)  # SQL expression on source columns
    target_sql_expr = Column(Text, nullable=True)  # SQL expression on target columns
    dq_rule = Column(String(255), nullable=True)  # Data quality rule (NOT NULL, FK EXISTS, etc.)
    test_notes = Column(Text, nullable=True)  # AI-generated test guidance
    is_key = Column(Boolean, default=False)
    created_at = Column(DateTime, default=utcnow)

    mapping_document = relationship("MappingDocument", back_populates="field_mappings")
    test_cases = relationship("TestCase", back_populates="field_mapping", cascade="all, delete-orphan")


class TestCase(Base):
    __tablename__ = "test_cases"

    id = Column(Integer, primary_key=True, index=True)
    mapping_document_id = Column(Integer, ForeignKey("mapping_documents.id"), nullable=False)
    field_mapping_id = Column(Integer, ForeignKey("field_mappings.id"), nullable=True)
    name = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False)  # row_count | null_check | duplicate | value_match
    source_sql = Column(Text, nullable=True)
    target_sql = Column(Text, nullable=True)
    description = Column(Text, nullable=True)  # AI-generated explanation of what this test validates
    validation_status = Column(String(20), default="pending")  # valid | invalid | pending | manual
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=utcnow)

    field_mapping = relationship("FieldMapping", back_populates="test_cases")
    mapping_document = relationship("MappingDocument", back_populates="test_cases")
    results = relationship("TestResult", back_populates="test_case", cascade="all, delete-orphan")


class TestRun(Base):
    __tablename__ = "test_runs"

    id = Column(Integer, primary_key=True, index=True)
    mapping_document_id = Column(Integer, ForeignKey("mapping_documents.id"), nullable=False)
    status = Column(String(50), default="running")  # running | completed | failed
    total_cases = Column(Integer, default=0)
    passed_cases = Column(Integer, default=0)
    failed_cases = Column(Integer, default=0)
    started_at = Column(DateTime, default=utcnow)
    completed_at = Column(DateTime, nullable=True)

    mapping_document = relationship("MappingDocument", back_populates="test_runs")
    results = relationship("TestResult", back_populates="test_run", cascade="all, delete-orphan")


class TestResult(Base):
    __tablename__ = "test_results"

    id = Column(Integer, primary_key=True, index=True)
    test_run_id = Column(Integer, ForeignKey("test_runs.id"), nullable=False)
    test_case_id = Column(Integer, ForeignKey("test_cases.id"), nullable=False)
    passed = Column(Boolean, nullable=False)
    source_value = Column(Text, nullable=True)
    target_value = Column(Text, nullable=True)
    mismatch_count = Column(Integer, default=0)
    mismatch_sample = Column(Text, nullable=True)  # JSON
    error_message = Column(Text, nullable=True)
    executed_at = Column(DateTime, default=utcnow)

    test_run = relationship("TestRun", back_populates="results")
    test_case = relationship("TestCase", back_populates="results")
