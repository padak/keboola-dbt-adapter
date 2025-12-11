# Comprehensive Quality Assessment of dbt-keboola Adapter

**Analysis Date:** 2025-12-11
**Adapter Version:** 0.1.0
**Analyzed by:** Claude Code with explore agents

---

## EXECUTIVE SUMMARY

**Overall Rating: 7/10** - Solid alpha version with good architecture, but with critical gaps for production.

| Aspect | Score | Comment |
|--------|-------|---------|
| Architecture | 9/10 | Clean separation of concerns |
| dbt conventions | 9/10 | Excellent compliance with standards |
| DB-API Implementation | 8/10 | Complete, missing iterator protocol |
| Error handling | 6/10 | Good mapping, some silent errors |
| Thread safety | 5/10 | Undocumented |
| Testing | 1/10 | Empty test directories |
| Production readiness | 6/10 | Works, needs hardening |

---

## WHAT'S EXCELLENT

### 1. Clean Architecture and Separation of Concerns

```
connections.py  -> Connection lifecycle, DB-API cursor
impl.py         -> Adapter-specific operations
relation.py     -> Snowflake identifiers
column.py       -> Type system
macros/         -> SQL materializations
```

The architecture closely follows the pattern of official dbt adapters (postgres, snowflake). Each file has clear responsibility - this is a fundamental prerequisite for maintainability.

### 2. Correct Namespace Package Implementation

```python
# dbt/__init__.py, dbt/adapters/__init__.py
__path__ = __import__('pkgutil').extend_path(__path__, __name__)
```

This is **critical** for coexistence with installed dbt-core. Without it, the local package would override the system dbt.

### 3. Robust DB-API 2.0 Emulation

`KeboolaCursor` implements:
- `description` property (7-tuple format per specification)
- `rowcount` for SELECT and DML
- `execute()`, `fetchone()`, `fetchmany()`, `fetchall()`
- Correct list -> tuple conversions

### 4. Snowflake Case-Insensitive Matching

```python
def _is_exactish_match(self, other: "KeboolaRelation") -> bool:
    return (
        self.database.upper() == other.database.upper() and
        self.schema.upper() == other.schema.upper() and
        self.identifier.upper() == other.identifier.upper()
    )
```

Snowflake stores unquoted identifiers as UPPERCASE - the adapter handles this correctly.

### 5. Idempotent Operations

```sql
CREATE OR REPLACE TABLE {{ relation }} AS ({{ sql }})
```

Every `dbt run` can safely be executed repeatedly without manual work.

### 6. Complete Materializations

- **Table**: Fully functional with hooks
- **View**: Handles table -> view replacement
- **Incremental**: 3 strategies (merge, delete+insert, append)

---

## WHAT'S A COMPROMISE

### 1. Simulated Transactions (intentional)

```python
def begin(self) -> None:
    """REST API doesn't support transactions."""
    self._in_transaction = True  # No-op

def commit(self) -> None:
    self._in_transaction = False  # No-op
```

Keboola Query Service REST API doesn't support transactions. The adapter honestly simulates this. This isn't a bug - it's an inherent architectural limitation. However, it means rollback isn't possible if failure occurs mid-operation.

### 2. All Data in Memory

```python
self._data = self._result.data or []  # Everything in RAM
```

For typical dbt usage (< 100K rows) this is OK. For large datasets it will cause OOM.

### 3. No Parameterized Query Support

```python
if bindings:
    logger.warning("Keboola Query Service does not support parameter bindings")
```

The SDK doesn't support it. The adapter honestly logs this.

### 4. Query Cancellation Not Implemented

```python
def cancel(self, connection: Connection) -> None:
    # Note: SDK doesn't provide a way to get current job ID
    logger.debug("Query cancellation not implemented")
```

Long-running queries cannot be interrupted - the SDK doesn't support this.

---

## CRITICAL ISSUES

### 1. NO UNIT TESTS

```
tests/
├── unit/        # EMPTY
└── functional/  # EMPTY
```

**Impact on community:**
- Community contributors cannot trust that their changes don't break anything
- No regression protection
- Professional projects expect >80% coverage
- **BLOCKS acceptance into dbt-labs/dbt-adapters**

### 2. Bug in `KeboolaRelation.matches()`

**Location:** `dbt/adapters/keboola/relation.py:36-59`

```python
def matches(self, database=None, schema=None, identifier=None) -> bool:
    for part in search:
        if self.database and self.database.upper() == part_upper:
            continue  # BUG: any part can match any component!
```

**Failure example:**
```python
relation = KeboolaRelation.create(database="DB", schema="SCH", identifier="events")
relation.matches(database="events")  # Returns True - WRONG!
```

**Fix:**
```python
def matches(self, database=None, schema=None, identifier=None) -> bool:
    if database and (not self.database or self.database.upper() != database.upper()):
        return False
    if schema and (not self.schema or self.schema.upper() != schema.upper()):
        return False
    if identifier and (not self.identifier or self.identifier.upper() != identifier.upper()):
        return False
    return True
```

### 3. Inconsistent Type Checking in `KeboolaColumn`

**Location:** `dbt/adapters/keboola/column.py:56-74`

```python
def is_integer(self) -> bool:
    return self.data_type.upper() in ("INT", "INTEGER", "BIGINT", ...)
    # data_type is already normalized to "NUMBER" - these raw types aren't there!
```

**Impact:**
```python
col = KeboolaColumn(column="age", dtype="BIGINT")
col.is_integer()  # Checks "NUMBER" in ("INT", "INTEGER"...) = False - WRONG!
```

**Fix:** Test against `self.dtype`, not `self.data_type`

### 4. Silent Exception Swallowing

**Location:** `dbt/adapters/keboola/connections.py:327-332`

```python
def close(self) -> None:
    try:
        self._client.close()
    except Exception as e:
        logger.warning(f"Error closing: {e}")  # Exception disappears!
```

**Impact:** Resource cleanup problems remain hidden.

### 5. SQL Injection in Metadata Queries

**Location:** `dbt/include/keboola/macros/adapters.sql:77`

```sql
where "table_name" = '{{ relation.identifier }}'  -- Not escaped!
```

**Proof of concept:**
```
relation.identifier = "test' OR '1'='1"
-- Result: where "table_name" = 'test' OR '1'='1'
```

Risk is MEDIUM (only affects INFORMATION_SCHEMA), but the community will notice.

---

## COMPARISON WITH COMMUNITY STANDARDS

| Requirement | dbt-postgres | dbt-snowflake | dbt-keboola |
|-------------|--------------|---------------|-------------|
| Unit tests | 100+ | 100+ | **0** |
| Functional tests | Yes | Yes | **No** |
| CI/CD pipeline | Yes | Yes | **No** |
| Documentation | Excellent | Excellent | Basic |
| Type hints | Complete | Complete | 85% |
| Snapshot materialization | Yes | Yes | **No** |
| on_schema_change | Yes | Yes | **No** |

---

## WHAT THE COMMUNITY WOULD REJECT

### 1. Absence of Tests (DEALBREAKER)
No serious dbt user will accept an adapter without tests into production.

### 2. Missing Snapshot Materialization
SCD Type 2 patterns are commonly used. Without snapshots, the adapter covers only part of use-cases.

### 3. Thread Safety Not Documented
With `threads: 4` in profiles.yml, subtle race conditions may occur.

### 4. No CI/CD
Pull requests cannot be automatically validated.

### 5. Limited Documentation
Missing:
- Usage examples for all materializations
- Troubleshooting guide
- Performance recommendations
- Known limitations

---

## RECOMMENDATIONS FOR PRODUCTION

### Priority 1 (BLOCKING)

1. **Add unit tests** - minimum 50 tests covering:
   - KeboolaCredentials validation
   - KeboolaCursor execute/fetch flow
   - KeboolaRelation.matches() fixed
   - KeboolaColumn type predicates fixed
   - Exception handling

2. **Fix bug in matches()** - see fix above

3. **Fix KeboolaColumn type methods** - test against `self.dtype`, not `self.data_type`

### Priority 2 (IMPORTANT)

4. **Add CI/CD** (GitHub Actions)
5. **Escape SQL in INFORMATION_SCHEMA queries**
6. **Add retry logic** with exponential backoff
7. **Document thread safety** limitations

### Priority 3 (NICE TO HAVE)

8. Implement Snapshot materialization
9. Add `on_schema_change` macro
10. Add iterator protocol to cursor (`__iter__`, `__next__`)

---

## DETAILED ANALYSIS BY COMPONENT

### connections.py (551 lines)

**Strengths:**
- Complete DB-API 2.0 cursor implementation
- Specific exception mapping (AuthenticationError, JobTimeoutError, QueryServiceError)
- Good logging including query ID

**Weaknesses:**
- No retry logic for transient errors
- Hard-coded timeouts (connect_timeout=10.0, max_retries=3)
- `_last_query_id` is never populated

### impl.py (284 lines)

**Strengths:**
- Correctly inherits from SQLAdapter
- Snowflake-specific functions (date_function, quote)
- Complete list_relations, get_columns implementation

**Weaknesses:**
- Weak credential validation
- Missing cancel() implementation

### relation.py (151 lines)

**Strengths:**
- Correct case-insensitive `_is_exactish_match()`
- Correct quote policy (only identifiers)
- Clean render() methods

**Weaknesses:**
- Bug in matches() - critical issue

### column.py (96 lines)

**Strengths:**
- Complete TYPE_LABELS mapping (28+ types)
- Correct string_type(), numeric_type() methods

**Weaknesses:**
- is_string/is_numeric/is_integer/is_float check normalized types

### macros/ (426 lines total)

**Strengths:**
- Complete table, view, incremental materializations
- Correct CREATE OR REPLACE usage
- 3 incremental strategies

**Weaknesses:**
- SQL injection in INFORMATION_SCHEMA queries
- Missing snapshot materialization
- Missing on_schema_change

---

## CONCLUSION

The dbt-keboola adapter has a **solid architectural foundation** and demonstrates good knowledge of dbt conventions. The code is readable, well-organized, and most key functions are implemented correctly.

**The main barrier to community acceptance is the absence of tests.** This is unacceptable in the dbt ecosystem. The two semantic bugs (matches, type predicates) can be fixed within hours.

| Usage | Rating | Comment |
|-------|--------|---------|
| Internal corporate | 7/10 | Usable with awareness of limitations |
| Open-source community | 5/10 | Needs tests and bug fixes |
| Production-grade | 4/10 | Missing hardening and monitoring |
