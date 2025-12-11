# Komplexni posouzeni kvality dbt-keboola adapteru

**Datum analyzy:** 2025-12-11
**Verze adapteru:** 0.1.0
**Analyzovano:** Claude Code s explore agenty

---

## EXECUTIVE SUMMARY

**Celkove hodnoceni: 7/10** - Solidni alpha verze s dobrou architekturou, ale s kritickymy mezerami pro produkci.

| Aspekt | Skore | Komentar |
|--------|-------|----------|
| Architektura | 9/10 | Ciste rozdeleni zodpovednosti |
| dbt konvence | 9/10 | Vynikajici soulad se standardy |
| Implementace DB-API | 8/10 | Kompletni, chybi iterator protokol |
| Error handling | 6/10 | Dobre mapovani, nektere tiche chyby |
| Thread safety | 5/10 | Nedokumentovano |
| Testovani | 1/10 | Prazdne testovaci adresare |
| Produkcni pripravenost | 6/10 | Funguje, potrebuje zpevneni |

---

## CO JE SKVELE

### 1. Cista architektura a separace zodpovednosti

```
connections.py  -> Zivotni cyklus pripojeni, DB-API kurzor
impl.py         -> Adapter-specificke operace
relation.py     -> Snowflake identifikatory
column.py       -> Typovy system
macros/         -> SQL materializace
```

Architektura presne kopiruje vzor oficialnich dbt adapteru (postgres, snowflake). Kazdy soubor ma jasnou zodpovednost - to je zakladni predpoklad pro udrzitelnost.

### 2. Spravna implementace namespace packages

```python
# dbt/__init__.py, dbt/adapters/__init__.py
__path__ = __import__('pkgutil').extend_path(__path__, __name__)
```

Toto je **kriticke** pro koexistenci s instalovanym dbt-core. Bez toho by lokalni package prekryl systemovy dbt.

### 3. Robustni DB-API 2.0 emulace

`KeboolaCursor` implementuje:
- `description` property (7-tuple format dle specifikace)
- `rowcount` pro SELECT i DML
- `execute()`, `fetchone()`, `fetchmany()`, `fetchall()`
- Spravne prevody list -> tuple

### 4. Snowflake case-insensitive matching

```python
def _is_exactish_match(self, other: "KeboolaRelation") -> bool:
    return (
        self.database.upper() == other.database.upper() and
        self.schema.upper() == other.schema.upper() and
        self.identifier.upper() == other.identifier.upper()
    )
```

Snowflake uklada unquoted identifikatory jako UPPERCASE - adapter to spravne resi.

### 5. Idempotentni operace

```sql
CREATE OR REPLACE TABLE {{ relation }} AS ({{ sql }})
```

Kazdy `dbt run` muze bezpecne probehnout opakovane bez manualni prace.

### 6. Kompletni materializace

- **Table**: Plne funkcni s hooks
- **View**: Osetruje nahrazeni table -> view
- **Incremental**: 3 strategie (merge, delete+insert, append)

---

## CO JE KOMPROMISNI

### 1. Simulovane transakce (zamerne)

```python
def begin(self) -> None:
    """REST API doesn't support transactions."""
    self._in_transaction = True  # No-op

def commit(self) -> None:
    self._in_transaction = False  # No-op
```

REST API Keboola Query Service nepodporuje transakce. Adapter to poctive simuluje. To neni chyba - je to inherentni omezeni architektury. Ale znamena to, ze pri selhani uprostred vicekrokove operace neni rollback mozny.

### 2. Vsechna data v pameti

```python
self._data = self._result.data or []  # Vse do RAM
```

Pro typicke dbt pouziti (< 100K radku) je to OK. Pro velke datasety to zpusobi OOM.

### 3. Zadna podpora parametrizovanych dotazu

```python
if bindings:
    logger.warning("Keboola Query Service does not support parameter bindings")
```

SDK to nepodporuje. Adapter to poctive loguje.

### 4. Zruseni dotazu neni implementovano

```python
def cancel(self, connection: Connection) -> None:
    # Note: SDK doesn't provide a way to get current job ID
    logger.debug("Query cancellation not implemented")
```

Nelze prerusit dlouho bezici dotazy - SDK to nepodporuje.

---

## KRITICKE PROBLEMY

### 1. ZADNE UNIT TESTY

```
tests/
├── unit/        # PRAZDNY
└── functional/  # PRAZDNY
```

**Dopad na komunitu:**
- Komunitni contributori nemohou verit, ze jejich zmeny nic nerozbijeji
- Zadna regresni ochrana
- Profesionalni projekty ocekavaji >80% coverage
- **BLOKUJE prijeti do dbt-labs/dbt-adapters**

### 2. Bug v `KeboolaRelation.matches()`

**Lokace:** `dbt/adapters/keboola/relation.py:36-59`

```python
def matches(self, database=None, schema=None, identifier=None) -> bool:
    for part in search:
        if self.database and self.database.upper() == part_upper:
            continue  # BUG: jakykoli part muze matchovat jakykoli component!
```

**Priklad selhani:**
```python
relation = KeboolaRelation.create(database="DB", schema="SCH", identifier="events")
relation.matches(database="events")  # Vrati True - SPATNE!
```

**Oprava:**
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

### 3. Nekonzistentni type checking v `KeboolaColumn`

**Lokace:** `dbt/adapters/keboola/column.py:56-74`

```python
def is_integer(self) -> bool:
    return self.data_type.upper() in ("INT", "INTEGER", "BIGINT", ...)
    # data_type uz je normalizovano na "NUMBER" - tyhle raw typy tam nejsou!
```

**Dopad:**
```python
col = KeboolaColumn(column="age", dtype="BIGINT")
col.is_integer()  # Kontroluje "NUMBER" in ("INT", "INTEGER"...) = False - SPATNE!
```

**Oprava:** Testovat proti `self.dtype`, ne `self.data_type`

### 4. Tichy swallow vyjimek

**Lokace:** `dbt/adapters/keboola/connections.py:327-332`

```python
def close(self) -> None:
    try:
        self._client.close()
    except Exception as e:
        logger.warning(f"Error closing: {e}")  # Vyjimka zmizi!
```

**Dopad:** Problemy s uvolnovanim prostredku zustanou skryty.

### 5. SQL Injection v metadata dotazech

**Lokace:** `dbt/include/keboola/macros/adapters.sql:77`

```sql
where "table_name" = '{{ relation.identifier }}'  -- Neni escapovano!
```

**Proof of concept:**
```
relation.identifier = "test' OR '1'='1"
-- Vysledek: where "table_name" = 'test' OR '1'='1'
```

Riziko je MEDIUM (jde jen o INFORMATION_SCHEMA), ale komunita si vsimne.

---

## SROVNANI S KOMUNITNIMI STANDARDY

| Pozadavek | dbt-postgres | dbt-snowflake | dbt-keboola |
|-----------|--------------|---------------|-------------|
| Unit testy | 100+ | 100+ | **0** |
| Functional testy | Ano | Ano | **Ne** |
| CI/CD pipeline | Ano | Ano | **Ne** |
| Dokumentace | Vyborne | Vyborne | Zakladni |
| Type hints | Kompletni | Kompletni | 85% |
| Snapshot materialization | Ano | Ano | **Ne** |
| on_schema_change | Ano | Ano | **Ne** |

---

## CO BY KOMUNITA SPATNE PRIJALA

### 1. Absence testu (DEALBREAKER)
Zadny seriozni dbt uzivatel neprijme adapter bez testu do produkce.

### 2. Chybejici Snapshot materialization
SCD Type 2 patterny jsou bezne pouzivane. Bez snapshotu adapter pokryva jen cast use-cases.

### 3. Thread safety neni dokumentovana
Pri `threads: 4` v profiles.yml mohou nastat subtilni race conditions.

### 4. Zadna CI/CD
Pull requesty nelze automaticky validovat.

### 5. Limitovana dokumentace
Chybi:
- Priklady pouziti vsech materializaci
- Troubleshooting guide
- Performance doporuceni
- Known limitations

---

## DOPORUCENI PRO PRODUKCI

### Priorita 1 (BLOKUJICI)

1. **Pridat unit testy** - minimalne 50 testu pokryvajicich:
   - KeboolaCredentials validace
   - KeboolaCursor execute/fetch flow
   - KeboolaRelation.matches() opraveny
   - KeboolaColumn type predicates opravene
   - Exception handling

2. **Opravit bug v matches()** - viz oprava vyse

3. **Opravit KeboolaColumn type methods** - testovat proti `self.dtype`, ne `self.data_type`

### Priorita 2 (DULEZITE)

4. **Pridat CI/CD** (GitHub Actions)
5. **Escapovat SQL v INFORMATION_SCHEMA dotazech**
6. **Pridat retry logiku** s exponential backoff
7. **Dokumentovat thread safety** omezeni

### Priorita 3 (NICE TO HAVE)

8. Implementovat Snapshot materialization
9. Pridat `on_schema_change` macro
10. Pridat iterator protokol do kurzoru (`__iter__`, `__next__`)

---

## DETAILNI ANALYZA PODLE KOMPONENT

### connections.py (551 radku)

**Silne stranky:**
- Kompletni DB-API 2.0 cursor implementace
- Specificke exception mapping (AuthenticationError, JobTimeoutError, QueryServiceError)
- Dobre logovani vcetne query ID

**Slabiny:**
- Zadna retry logika pro transientni chyby
- Hard-coded timeouty (connect_timeout=10.0, max_retries=3)
- `_last_query_id` nikdy neni naplneno

### impl.py (284 radku)

**Silne stranky:**
- Spravne dedi z SQLAdapter
- Snowflake-specificke funkce (date_function, quote)
- Kompletni list_relations, get_columns implementace

**Slabiny:**
- Weak credential validation
- Chybi cancel() implementace

### relation.py (151 radku)

**Silne stranky:**
- Spravna case-insensitive `_is_exactish_match()`
- Korektni quote policy (jen identifikatory)
- Ciste render() metody

**Slabiny:**
- Bug v matches() - kriticky problem

### column.py (96 radku)

**Silne stranky:**
- Kompletni TYPE_LABELS mapovani (28+ typu)
- Spravne string_type(), numeric_type() metody

**Slabiny:**
- is_string/is_numeric/is_integer/is_float kontroluji normalizovane typy

### macros/ (426 radku celkem)

**Silne stranky:**
- Kompletni table, view, incremental materializace
- Spravne CREATE OR REPLACE pouziti
- 3 incremental strategie

**Slabiny:**
- SQL injection v INFORMATION_SCHEMA dotazech
- Chybi snapshot materialization
- Chybi on_schema_change

---

## ZAVER

dbt-keboola adapter ma **solidni architektonicky zaklad** a vykazuje dobrou znalost dbt konvenci. Kod je citelny, dobre organizovany a vetsina klicovych funkci je implementovana spravne.

**Hlavni prekazka pro prijeti komunitou je absence testu.** To je v dbt ekosystemu neprijatelne. Dva semanticke bugy (matches, type predicates) jsou opravitelne behem hodin.

| Pouziti | Hodnoceni | Komentar |
|---------|-----------|----------|
| Interni firemni | 7/10 | Pouzitelne s vedomim omezeni |
| Open-source komunita | 5/10 | Potrebuje testy a opravy bugy |
| Production-grade | 4/10 | Chybi hardening a monitoring |
