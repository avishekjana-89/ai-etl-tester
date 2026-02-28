"""
AI Parser — Bulk Context & Bulk Validation approach.

analyze_mapping_document() fetches schemas for relevant tables upfront,
sends them to the AI in one call, and validates the generated SQL in bulk.
"""

import json
import logging
import re

from app.ai.provider import get_ai_provider
from app.ai.prompts import (
    ETL_ANALYZER_SYSTEM,
    ETL_ANALYZER_USER,
    SQL_VALIDATION_ERROR_USER,
)

ai = get_ai_provider()
logger = logging.getLogger("etl_ai")


def _get_smart_context(conn, document_content: str) -> str:
    """
    Fetch schemas for tables explicitly named (Seeds) + tables that connect them (Bridges).
    """
    if not conn:
        return "No connection available."

    try:
        all_tables = conn.get_tables()
        seed_tables = []
        doc_lower = document_content.lower()

        # Phase 1: Seed Discovery
        for table in all_tables:
            table_basename = table.split(".")[-1].lower()
            if table_basename in doc_lower or table.lower() in doc_lower:
                seed_tables.append(table)

        # Extra Seed Discovery: Look for "table.column" patterns in rules
        # This helps if a table is only mentioned inside a Rule/Transform cell
        rule_tables = re.findall(r'([a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+', document_content)
        for rt in rule_tables:
            rt_lower = rt.lower()
            for t in all_tables:
                if t.lower().endswith(f".{rt_lower}") or t.lower() == rt_lower:
                    if t not in seed_tables:
                        seed_tables.append(t)

        if not seed_tables:
            return "No tables found matching the document content."

        # Collect full schema for seeds and identify bridge opportunities
        relevant_tables = set(seed_tables)
        seed_schemas = {t: conn.get_columns(t) for t in seed_tables}
        
        # Phase 2: Outbound / One-Hop Discovery (FK based)
        for table, cols in seed_schemas.items():
            for col in cols:
                fk = col.get("foreign_key")
                if fk:
                    parts = fk.split(".")
                    if len(parts) >= 2:
                        ref_table = ".".join(parts[:-1])
                        if ref_table in all_tables:
                            relevant_tables.add(ref_table)

        # Phase 3: Bridge Discovery (FK Heuristic)
        if len(seed_tables) >= 2:
            seed_names = set(seed_tables)
            for table in all_tables:
                if table in relevant_tables:
                    continue
                try:
                    cols = conn.get_columns(table)
                    points_to = set()
                    for col in cols:
                        fk = col.get("foreign_key")
                        if fk:
                            parts = fk.split(".")
                            if len(parts) >= 2:
                                ref_table = ".".join(parts[:-1])
                                if ref_table in seed_names:
                                    points_to.add(ref_table)
                    if len(points_to) >= 2:
                        logger.info(f"Discovered bridge table (FK): {table}")
                        relevant_tables.add(table)
                except: continue

        # Phase 4: Column-Name Heuristic (Discovery without FKs)
        # Find tables that have column names matching keys or identity columns of seed tables
        if len(seed_tables) >= 2:
            # Map of column name -> set of seed tables that have it
            # We filter for ID-like/Type-like columns to avoid greedy matches on 'name', 'status', etc.
            id_like_pattern = re.compile(r'.*(_id|_key|_code|_cd|type|code)$', re.IGNORECASE)
            
            key_map = {}
            for t, cols in seed_schemas.items():
                for c in cols:
                    cname = c["name"].lower()
                    if c.get("is_primary_key") or c.get("is_unique") or id_like_pattern.match(cname):
                        key_map.setdefault(cname, set()).add(t)

            for table in all_tables:
                if table in relevant_tables:
                    continue
                try:
                    cols = conn.get_columns(table)
                    col_names = {c["name"].lower() for c in cols}
                    connects = set()
                    for kname, tables in key_map.items():
                        if kname in col_names:
                            # Only count as a match if the column in the bridge table is ALSO ID-like
                            if id_like_pattern.match(kname):
                                connects.update(tables)
                    
                    if len(connects) >= 2:
                        logger.info(f"Discovered bridge table (Heuristic Match): {table} (connects {connects})")
                        relevant_tables.add(table)
                except: continue

        # Phase 5: Inbound Discovery (Who points TO our seeds?)
        # This helps find Fact tables if our seeds are Dimensions.
        if len(seed_tables) > 0:
            seed_names = set(seed_tables)
            for table in all_tables:
                if table in relevant_tables:
                    continue
                try:
                    cols = conn.get_columns(table)
                    for col in cols:
                        fk = col.get("foreign_key")
                        if fk:
                            parts = fk.split(".")
                            if len(parts) >= 2:
                                ref_table = ".".join(parts[:-1])
                                if ref_table in seed_names:
                                    logger.info(f"Discovered relevant inbound table: {table} (points to {ref_table})")
                                    relevant_tables.add(table)
                                    break
                except: continue

        # Final Cleanup: Cap the number of tables to avoid token blow-up
        sorted_tables = sorted(list(relevant_tables))
        if len(sorted_tables) > 15:
            logger.warning(f"Too many relevant tables ({len(sorted_tables)}). Capping to 15.")
            sorted_tables = sorted_tables[:15]

        logger.info(f"Final relevant tables for AI context: {sorted_tables}")

        # Fetch full schema and sample data for set of relevant tables
        context = []
        for table in sorted_tables:
            columns = conn.get_columns(table) if table not in seed_schemas else seed_schemas[table]
            sample_data = conn.get_sample_data(table, limit=3)
            context.append(json.dumps({
                "table": table, 
                "columns": columns,
                "sample_rows": sample_data
            }, indent=2))

        return "\n\n".join(context)

    except Exception as e:
        logger.error(f"Failed to get smart context: {e}")
        return f"Error fetching schema: {e}"


def _bulk_validate_sql(test_cases: list[dict], source_conn, target_conn) -> str | None:
    """
    Run EXPLAIN on all SQL queries.
    Returns a formatted error message string if any fail, or None if all pass.
    """
    errors = []
    
    for i, tc in enumerate(test_cases):
        test_name = tc.get("name", f"Test {i}")
        tc["validation_status"] = "valid"

        if tc.get("source_sql") and source_conn:
            try:
                source_conn.execute_query(f"EXPLAIN {tc['source_sql']}")
            except Exception as e:
                tc["validation_status"] = "invalid"
                errors.append(f"[{test_name}] SOURCE SQL Error: {str(e)}\nQuery: {tc['source_sql']}")

        if tc.get("target_sql") and target_conn:
            try:
                target_conn.execute_query(f"EXPLAIN {tc['target_sql']}")
            except Exception as e:
                tc["validation_status"] = "invalid"
                errors.append(f"[{test_name}] TARGET SQL Error: {str(e)}\nQuery: {tc['target_sql']}")

    if errors:
        return "\n\n".join(errors)
    return None


def _extract_json(content: str) -> dict:
    """Extract JSON from AI response."""
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except:
                pass
        return {"field_mappings": [], "test_cases": []}


async def analyze_mapping_document(
    document_content: str,
    source_conn,
    target_conn,
    source_db_type: str = "postgresql",
    target_db_type: str = "mysql",
) -> dict:
    """
    Bulk Context ETL analysis.
    1. Grabs smart context (only relevant tables)
    2. Calls AI to generate test cases
    3. Bulk validates SQL
    4. If errors, asks AI to fix them (max 2 retries)
    """
    logger.info("Gathering smart context...")
    source_schema = _get_smart_context(source_conn, document_content)
    target_schema = _get_smart_context(target_conn, document_content)

    system_prompt = ETL_ANALYZER_SYSTEM.format(
        source_db_type=source_db_type,
        target_db_type=target_db_type,
    )
    user_prompt = ETL_ANALYZER_USER.format(
        source_db_type=source_db_type,
        target_db_type=target_db_type,
        source_schema=source_schema,
        target_schema=target_schema,
        document_content=document_content,
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    logger.debug(f"AI SYSTEM PROMPT:\n{system_prompt}")
    logger.debug(f"AI USER PROMPT:\n{user_prompt}")

    max_retries = 2
    for attempt in range(max_retries + 1):
        logger.info(f"=== ETL ANALYZER - Attempt {attempt + 1}/{max_retries + 1} ===")
        
        content = await ai.chat(messages, temperature=0.1)
        parsed = _extract_json(content)
        
        test_cases = parsed.get("test_cases", [])
        
        # Bulk Validation
        logger.info(f"Bulk validating {len(test_cases)} test cases...")
        error_msg = _bulk_validate_sql(test_cases, source_conn, target_conn)

        if not error_msg:
            logger.info("✅ All SQL validated successfully!")
            return parsed
        
        # If there are errors and we have retries left, ask AI to fix
        if attempt < max_retries:
            logger.warning(f"SQL validation failed. Asking AI to fix:\n{error_msg}")
            
            # Append AI's response so it knows what it generated
            messages.append({"role": "assistant", "content": content})
            
            # Append the error prompt
            retry_prompt = SQL_VALIDATION_ERROR_USER.format(error_message=error_msg)
            messages.append({"role": "user", "content": retry_prompt})
        else:
            logger.error("❌ Max retries reached. Returning parsed output with invalid SQL.")
            # Status was already marked "invalid" inside _bulk_validate_sql
            return parsed

    return {"field_mappings": [], "test_cases": []}
