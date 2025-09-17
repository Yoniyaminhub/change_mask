-- change-mask/snowflake/change_mask.sql

-- JS UDF: decode little-endian mask into array of bit ordinals
CREATE OR REPLACE FUNCTION CHANGE_MASK_BITS(mask BINARY)
RETURNS ARRAY
LANGUAGE JAVASCRIPT
AS
$$
  if (mask === null) return [];
  const arr = [];
  for (let i = 0; i < mask.length; i++) {
    const byte = mask[i];
    for (let b = 0; b < 8; b++) {
      if ((byte & (1 << b)) !== 0) arr.push(i*8 + b);
    }
  }
  return arr;
$$;

-- Tall: one row per CT row × column
WITH cols AS (
  SELECT (ordinal_position - 1) AS bit_ordinal, column_name
  FROM information_schema.columns
  WHERE table_schema = '<SCHEMA>' AND table_name = '<CT_TABLE>'
)
SELECT r.*, c.column_name,
       IFF(f.value IS NULL, 0, 1) AS changed
FROM "<SCHEMA>"."<CT_TABLE>" r
JOIN cols c ON TRUE
LEFT JOIN LATERAL FLATTEN(INPUT => CHANGE_MASK_BITS(r."<MASK_COL>")) f
       ON f.value = c.bit_ordinal
-- WHERE c.bit_ordinal >= <HEADER_COLS>
ORDER BY c.bit_ordinal;

-- Wide: helper that prints the dynamic SELECT text. Copy result and run (or wrap in EXECUTE IMMEDIATE).
WITH exprs AS (
  SELECT LISTAGG(
    'IFF(ARRAY_CONTAINS(' || CAST((ordinal_position-1) AS VARCHAR) ||
    '::variant, CHANGE_MASK_BITS(r."' || '<MASK_COL>' || '")),' ||
    ' 1, 0) AS "' || column_name || '"', ', ') AS select_list
  FROM information_schema.columns
  WHERE table_schema = '<SCHEMA>' AND table_name = '<CT_TABLE>'
)
SELECT 'SELECT r.*, ' || select_list || ' FROM "' || '<SCHEMA>' || '"."' ||
       '<CT_TABLE>' || '" r' AS generated_sql
FROM exprs;
