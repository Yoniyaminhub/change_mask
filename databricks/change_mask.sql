-- change-mask/databricks/change_mask.sql

-- Function (Python) to decode little-endian mask into an array of bit ordinals
CREATE OR REPLACE FUNCTION change_mask_bits(mask BINARY)
RETURNS ARRAY<INT>
LANGUAGE PYTHON
AS $$
def run(mask):
    if mask is None:
        return []
    out = []
    for i, byte in enumerate(mask):
        for b in range(8):
            if byte & (1 << b):
                out.append(i*8 + b)
    return out
$$;

-- Tall: one row per CT row × column
WITH cols AS (
  SELECT (ordinal_position - 1) AS bit_ordinal, column_name
  FROM system.information_schema.columns  -- Use information_schema.columns if not in Unity Catalog
  WHERE table_schema = '<SCHEMA>' AND table_name = '<CT_TABLE>'
)
SELECT r.*, c.column_name,
       CASE WHEN pos IS NULL THEN 0 ELSE 1 END AS changed
FROM `<SCHEMA>`.`<CT_TABLE>` r
CROSS JOIN cols c
LEFT JOIN LATERAL VIEW explode(change_mask_bits(r.`<MASK_COL>`)) e AS pos
  ON pos = c.bit_ordinal
-- WHERE c.bit_ordinal >= <HEADER_COLS>
ORDER BY c.bit_ordinal;

-- Wide: generate dynamic SELECT text (Databricks SQL supports EXECUTE IMMEDIATE).
WITH exprs AS (
  SELECT concat_ws(', ',
    collect_list(
      CONCAT('CASE WHEN array_contains(change_mask_bits(r.`', '<MASK_COL>', '`), ',
             CAST(ordinal_position-1 AS STRING),
             ') THEN 1 ELSE 0 END AS `', column_name, '`')
    )
  ) AS select_list
  FROM system.information_schema.columns
  WHERE table_schema = '<SCHEMA>' AND table_name = '<CT_TABLE>'
)
SELECT CONCAT('SELECT r.*, ', select_list, ' FROM `<SCHEMA>`.`<CT_TABLE>` r') AS generated_sql
FROM exprs;

-- To run the generated SQL directly (Databricks SQL):
-- EXECUTE IMMEDIATE (SELECT CONCAT('SELECT r.*, ', select_list, ' FROM `<SCHEMA>`.`<CT_TABLE>` r') FROM exprs);
