"""
Test Executor — runs AI-generated test cases against source/target databases.

Three execution paths:
1. Comparison with key (key_col + val) — merges on common identifier
2. Set comparison (val only) — compares distinct value sets
3. Target-only — checks count = 0
"""

import json
import logging

import pandas as pd

from app.connectors.base import BaseConnector
from app.models import TestCase, TestResult

logger = logging.getLogger("etl_executor")


def execute_test_case(
    test_case: TestCase,
    source_conn: BaseConnector | None,
    target_conn: BaseConnector,
    parameters: dict[str, str] | None = None,
) -> TestResult:
    """Execute a single test case and return the result."""
    try:
        if test_case.source_sql and test_case.target_sql:
            return _execute_comparison(test_case, source_conn, target_conn, parameters)
        elif test_case.target_sql:
            return _execute_target_only(test_case, target_conn, parameters)
        else:
            return TestResult(
                test_case_id=test_case.id,
                passed=False,
                error_message="No SQL queries defined for this test case",
            )
    except Exception as e:
        return TestResult(
            test_case_id=test_case.id,
            passed=False,
            error_message=str(e),
        )


def _apply_parameters(sql: str, params: dict[str, str] | None) -> str:
    """Replace :name placeholders with parameter values."""
    if not params:
        return sql
    
    import re
    result = sql
    for key, val in params.items():
        # Match :key but not within another word
        # Using a simple replacement for now, could be improved with regex
        placeholder = f":{key}"
        result = result.replace(placeholder, str(val))
    return result


def _execute_comparison(tc: TestCase, src: BaseConnector, tgt: BaseConnector, params: dict[str, str] | None) -> TestResult:
    """
    Compare source and target query results.
    ... (omitted summary for brevity)
    """
    src_sql = _apply_parameters(tc.source_sql, params)
    tgt_sql = _apply_parameters(tc.target_sql, params)

    # ─── New: Short-circuit Checksum Logic ──────────────────────────
    # If checksums match at the DB level, we skip fetching data entirely.
    src_checksum = src.get_checksum(src_sql)
    tgt_checksum = tgt.get_checksum(tgt_sql)

    if src_checksum != "fallback-needed" and tgt_checksum != "fallback-needed":
        if src_checksum == tgt_checksum and src_checksum != "empty":
            logger.info(f"Test {tc.id}: Checksum match ({src_checksum}) - Skipping row fetch.")
            return TestResult(
                test_case_id=tc.id,
                passed=True,
                source_value="Checksum Match",
                target_value="Checksum Match",
                mismatch_count=0,
            )
    else:
        logger.debug(f"Test {tc.id}: Checksum fallback triggered (src: {src_checksum}, tgt: {tgt_checksum})")

    src_df = src.execute_query(src_sql)
    tgt_df = tgt.execute_query(tgt_sql)

    src_cols = list(src_df.columns)
    tgt_cols = list(tgt_df.columns)

    # ─── Pattern 1: Single scalar (row_count, aggregate) ──────────────
    if len(src_df) == 1 and len(tgt_df) == 1 and len(src_cols) == 1 and len(tgt_cols) == 1:
        src_val = str(src_df.iloc[0, 0])
        tgt_val = str(tgt_df.iloc[0, 0])
        passed = src_val == tgt_val
        return TestResult(
            test_case_id=tc.id,
            passed=passed,
            source_value=src_val,
            target_value=tgt_val,
            mismatch_count=0 if passed else 1,
        )

    def _normalize_series(series: pd.Series) -> pd.Series:
        """Helper to normalize differences before string comparison."""
        if series is None:
            return pd.Series(dtype=str)
        # 1. Fill NaNs with empty string
        # 2. Convert to string
        # 3. Strip whitespace
        # 4. Remove .0 from floats
        # 5. Clean up string representations of nulls
        return series.fillna('').astype(str).str.strip().str.replace(r'\.0$', '', regex=True).replace(['nan', 'NaN', 'None', 'null', 'NULL'], '')

    # ─── Pattern 2: Key-based comparison (key_col + multiple columns) ───────
    if "key_col" in src_cols and "key_col" in tgt_cols:
        src_df["key_col"] = _normalize_series(src_df["key_col"])
        tgt_df["key_col"] = _normalize_series(tgt_df["key_col"])
        
        # Identify comparison columns (all columns except key_col)
        # We only compare columns present in both datasets
        compare_cols = [c for c in src_cols if c in tgt_cols and c != "key_col"]
        
        if not compare_cols:
            return TestResult(
                test_case_id=tc.id,
                passed=False,
                error_message="Key-based comparison requires at least one value column (e.g., 'val') present in both source and target.",
            )
 
        # Normalize all comparison columns
        for col in compare_cols:
            src_df[col] = _normalize_series(src_df[col])
            tgt_df[col] = _normalize_series(tgt_df[col])
 
        merged = src_df.merge(
            tgt_df,
            on="key_col",
            how="outer",
            suffixes=("_source", "_target"),
            indicator=True,
        )

        # Build mismatch condition: 
        # 1. Row missing in either source or target (_merge != 'both')
        # 2. ANY comparison column has a mismatch
        mismatch_mask = (merged["_merge"] != "both")
        for col in compare_cols:
            mismatch_mask |= (merged[f"{col}_source"] != merged[f"{col}_target"])

        mismatches = merged[mismatch_mask].copy()

        if not mismatches.empty:
            mismatches["mismatch_type"] = mismatches["_merge"].map({
                "left_only": "missing_in_target",
                "right_only": "extra_in_target",
                "both": "value_mismatch",
            })
            mismatches = mismatches.drop(columns=["_merge"])

        mismatch_count = len(mismatches)
        sample = mismatches.head(100).to_json(orient="records") if mismatch_count > 0 else None

        return TestResult(
            test_case_id=tc.id,
            passed=mismatch_count == 0,
            source_value=f"{len(src_df)} rows",
            target_value=f"{len(tgt_df)} rows",
            mismatch_count=mismatch_count,
            mismatch_sample=sample,
        )

    # ─── Pattern 3: Set comparison (val only, or any columns) ─────────
    # Compare distinct value sets
    if "val" in src_cols and "val" in tgt_cols:
        src_vals = set(_normalize_series(src_df["val"]).tolist())
        tgt_vals = set(_normalize_series(tgt_df["val"]).tolist())
    else:
        # Fallback: stringify all rows and compare as sets
        src_vals = set(
            tuple(str(v) for v in row.values) for _, row in src_df.iterrows()
        )
        tgt_vals = set(
            tuple(str(v) for v in row.values) for _, row in tgt_df.iterrows()
        )

    missing = src_vals - tgt_vals
    extra = tgt_vals - src_vals

    mismatches = []
    for v in list(missing)[:50]:
        mismatches.append({"val": str(v), "mismatch_type": "missing_in_target"})
    for v in list(extra)[:50]:
        mismatches.append({"val": str(v), "mismatch_type": "extra_in_target"})

    mismatch_count = len(missing) + len(extra)
    sample = json.dumps(mismatches[:100]) if mismatches else None

    return TestResult(
        test_case_id=tc.id,
        passed=mismatch_count == 0,
        source_value=f"{len(src_vals)} distinct values",
        target_value=f"{len(tgt_vals)} distinct values",
        mismatch_count=mismatch_count,
        mismatch_sample=sample,
    )


def _execute_target_only(tc: TestCase, tgt: BaseConnector, params: dict[str, str] | None) -> TestResult:
    """
    Execute a target-only query (null_check, duplicate_check, dq_check).
    Pass condition: count = 0 (no violations found).
    """
    tgt_sql = _apply_parameters(tc.target_sql, params)
    df = tgt.execute_query(tgt_sql)

    # Single count value
    if len(df.columns) == 1 and len(df) == 1:
        count = int(df.iloc[0, 0])
        return TestResult(
            test_case_id=tc.id,
            passed=count == 0,
            target_value=str(count),
            mismatch_count=count,
        )

    # Set of violation rows
    row_count = len(df)
    sample = df.head(50).to_json(orient="records") if row_count > 0 else None

    return TestResult(
        test_case_id=tc.id,
        passed=row_count == 0,
        target_value=str(row_count),
        mismatch_count=row_count,
        mismatch_sample=sample,
    )
