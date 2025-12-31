from dataclasses import dataclass
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

@dataclass
class DQResult:
    check_name: str
    passed: bool
    failed_rows: int
    details: Optional[str] = None

def check_not_null(df: DataFrame, cols: List[str], check_name: str) -> DQResult:
    cond = None
    for c in cols:
        ccond = col(c).isNull()
        cond = ccond if cond is None else (cond | ccond)

    failed = df.filter(cond).count() if cond is not None else 0
    return DQResult(check_name=check_name, passed=(failed == 0), failed_rows=failed)

def check_unique(df: DataFrame, key_cols: List[str], check_name: str) -> DQResult:
    total = df.count()
    distinct = df.select(*key_cols).distinct().count()
    failed = total - distinct
    return DQResult(check_name=check_name, passed=(failed == 0), failed_rows=failed)

def check_allowed_values(df: DataFrame, col_name: str, allowed: List[str], check_name: str) -> DQResult:
    failed = df.filter(~col(col_name).isin(allowed)).count()
    return DQResult(check_name=check_name, passed=(failed == 0), failed_rows=failed)

def enforce_or_fail(results: List[DQResult]) -> None:
    failed = [r for r in results if not r.passed]
    if failed:
        msg = "Data Quality Failed:\n" + "\n".join(
            [f"- {r.check_name}: failed_rows={r.failed_rows} details={r.details}" for r in failed]
        )
        raise ValueError(msg)
