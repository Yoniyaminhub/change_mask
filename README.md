https://community.qlik.com/t5/Qlik-Replicate/Column-header-change-mask/m-p/2531061#M15102

# Change Mask → Column Change Flags (Qlik Replicate CT)

Decode Qlik Replicate’s **`change_mask`** (a little-endian, null-trimmed byte array) and output, for every Change Table (CT) row, a **1/0 flag per CT column** indicating whether that column changed.

- **Wide view (default goal):** one row per CT record, each CT column rendered as `0/1` (mirrors CT schema).
- **Tall view (optional):** one row per (CT row × column) → `column_name, changed(0/1)`.

Supported targets: **SQL Server, PostgreSQL, Oracle, Snowflake, Databricks (Spark SQL)**.

---

## How it works (recap)

- Bit **N** in `change_mask` corresponds to **CT column ordinal N** (0-based), including header columns.
- Little-endian: **byte 0 → bits 0..7**, **byte 1 → bits 8..15**, etc.
- `change_mask` may be **null-trimmed** (trailing zero bytes omitted). Logic accounts for this.
- Event semantics (from Replicate docs):  
  INSERT → all inserted columns set; DELETE → only PK/unique set; BEFORE-IMAGE → all clear; UPDATE → only changed columns.

---

## Folder layout

```
change-mask/
├─ sqlserver/change_mask.sql
├─ postgres/change_mask.sql
├─ oracle/change_mask.sql
├─ snowflake/change_mask.sql
└─ databricks/change_mask.sql
```

Each file includes:
- A **bit decoder** helper (UDF/TVF).
- **Tall** query.
- **Wide** query (dynamic SELECT that emits a 0/1 column per CT column).

---

## Prerequisites & placeholders

In each script, replace:

- `<SCHEMA>` – your schema (e.g., `dbo`, `public`, `MY_SCHEMA`).
- `<CT_TABLE>` – your **change table** name.
- `<MASK_COL>` – the **mask** column name (e.g., `header__change_mask`).
- `<HEADER_COLS>` – number of header columns (if you want to **exclude** them).

> Tip: To **discover** the column ordinals the scripts already map system catalogs to `0..N-1` in the correct order.

---

## Quick Start (per platform)

### 1) SQL Server

1) Run `sqlserver/change_mask.sql`. It creates:

- `dbo.fn_change_mask_bits(@mask VARBINARY(128))` (bit ordinals)
- **Tall** query: emits `column_name, changed`
- **Wide** query: dynamic SELECT producing 0/1 per CT column

2) Edit at the top:
```sql
DECLARE @ct SYSNAME = N'<SCHEMA>.<CT_TABLE>';
DECLARE @header_cols INT = <HEADER_COLS>;
-- SET @mask column name inside the wide query section: @mask = N'<MASK_COL>'
```

3) To get **wide flags** (one row per CT row mirroring CT schema), run the **Wide** section.  
   To **exclude headers**, add `WHERE bit_ordinal >= @header_cols` in the tall join or remove those columns from the dynamic list (optional).

> Output: `SELECT r.*, <every_CT_column_as_0_or_1>` (you can remove `r.*` if you want only flags).

---

### 2) PostgreSQL

1) Run `postgres/change_mask.sql`. It uses built-in `get_bit(bytea, offset)`.

2) Set placeholders at the top of each block:
```sql
SET search_path TO <SCHEMA>;
-- or qualify as <SCHEMA>.<CT_TABLE>
```

3) Run **Wide** (DO block) to generate and execute the 0/1 projection per CT column.  
   Run **Tall** if you prefer a `(column_name, changed)` view.

---

### 3) Oracle

1) Run `oracle/change_mask.sql`. It creates:

- `TYPE num_list AS TABLE OF NUMBER;`
- `FUNCTION change_mask_bits(p_mask RAW) RETURN num_list PIPELINED;`

2) Use the **Tall** SELECT to see per-column flags.  
   For **Wide**, run the provided PL/SQL block that generates a dynamic SELECT list; copy the printed SQL and execute it.

> Note: You’ll need `CREATE TYPE/FUNCTION` privileges in the schema (or `AUTHID CURRENT_USER` pattern if preferred).

---

### 4) Snowflake

1) Run `snowflake/change_mask.sql`. It creates:

- `CHANGE_MASK_BITS(mask BINARY) RETURNS ARRAY` (JS UDF)

2) Use the **Tall** query (with `LATERAL FLATTEN`) or build the **Wide** dynamic SELECT using `ARRAY_CONTAINS(k::variant, CHANGE_MASK_BITS(r.<MASK_COL>))` per column.

> Tip: The script includes a helper query that prints the dynamic SELECT; copy it and run with `EXECUTE IMMEDIATE` if desired.

---

### 5) Databricks (Spark SQL)

1) Run `databricks/change_mask.sql`. It creates:

- `change_mask_bits(mask BINARY) RETURNS ARRAY<INT>` (Python UDF)

2) **Tall**: `LATERAL VIEW explode(change_mask_bits(r.<MASK_COL>))` and join to a `cols` CTE drawn from `information_schema.columns`.

3) **Wide**: The script includes a generator that emits the dynamic SELECT text; copy + run (or use `EXECUTE IMMEDIATE` in Databricks SQL).

> Note: In Unity Catalog, use `system.information_schema.columns`; in Hive Metastore, use `information_schema.columns`.

---

## Choosing tall vs. wide

- **Wide (recommended):** Ideal when your downstream expects the full CT shape with 0/1 flags per column (one record per CT row).
- **Tall:** Great for auditing, analytics, or debugging (easy to aggregate, filter, count changed columns, etc.).

---

## Header columns

If you only care about **data columns**, skip the first **`<HEADER_COLS>`** bit positions.  
- Tall: add `WHERE c.bit_ordinal >= <HEADER_COLS>`.  
- Wide: either remove those from the dynamic list, or keep them if you want to visualize header changes too.

---

## Examples

**Example outcome (wide):**
```
ct_row_id | header__operation | id | name | price | updated_at
----------+--------------------+----+------+-------+-----------
12345     | 0                  | 1  | 0    | 1     | 0
```
Meaning: in that CT row, `id` and `price` changed.

**Example outcome (tall):**
```
ct_row_id | column_name | changed
----------+-------------+---------
12345     | id          | 1
12345     | name        | 0
12345     | price       | 1
12345     | updated_at  | 0
```

---

## Performance notes

- The **wide** dynamic SELECT is set-based and generally faster for bulk exports than joining to exploded bit lists.
- Add a **predicate** on CT (time window / commit sequence / partition column) to limit processed rows.
- Consider projecting **only flags** (drop `r.*`) if you don’t need original CT columns to reduce I/O.
- Ensure appropriate **warehouse/cluster** sizes (Snowflake/Databricks) for large CTs.

---

## Edge cases & validation

- **BEFORE-IMAGE** rows → mask is `NULL` or all zeros → all flags are 0.
- **Null-trimmed** masks are normal. Unset trailing bits are implicitly 0.
- To sanity-check, pick a small mask manually (e.g., `0xA1` → bits 0,5,7 set) and confirm the script flags the expected columns by ordinal.

---

## Troubleshooting

- **Wrong columns flagged:** Verify the CT **column order** used in the `cols`/catalog query matches the physical change table. The scripts use standard catalogs: `sys.columns` (SQL Server), `information_schema.columns` (PG/Snowflake/Databricks), `ALL_TAB_COLUMNS` (Oracle).
- **Headers vs data confusion:** Remember bit 0 = first CT column (often a header). Set `<HEADER_COLS>` correctly if you want to exclude them.
- **Permissions:** Creating UDFs/TVFs may require elevated privileges; ask your DBA if creation fails.
- **Databricks catalog differences:** Use the right `information_schema` path for your metastore/Unity Catalog.

---

## License

MIT (or your preference).

---

## Credits

Based on Qlik Replicate documentation for **change tables** and **change masks** (little-endian, column-ordinal mapping).
