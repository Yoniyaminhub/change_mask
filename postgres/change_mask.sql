-- change-mask/postgres/change_mask.sql
-- Set your schema/table/mask column:
-- SET search_path TO <SCHEMA>;

-- Tall: one row per CT row × column
WITH cols AS (
  SELECT (ordinal_position - 1) AS bit_ordinal, column_name
  FROM information_schema.columns
  WHERE table_schema = '<SCHEMA>'
    AND table_name  = '<CT_TABLE>'
)
SELECT r.*,
       c.column_name,
       CASE WHEN get_bit(r."<MASK_COL>", c.bit_ordinal) = 1 THEN 1 ELSE 0 END AS changed
FROM "<SCHEMA>"."<CT_TABLE>" r
JOIN cols c ON TRUE
-- WHERE c.bit_ordinal >= <HEADER_COLS>
ORDER BY c.bit_ordinal;

-- Wide: dynamic DO block generates and executes SELECT with 0/1 per column
DO $$
DECLARE
  select_list TEXT := '';
  sql TEXT := '';
BEGIN
  SELECT string_agg(
    format('CASE WHEN get_bit(r.%I, %s) = 1 THEN 1 ELSE 0 END AS %I',
           '<MASK_COL>', (ordinal_position-1), column_name), ', ')
  INTO select_list
  FROM information_schema.columns
  WHERE table_schema = '<SCHEMA>'
    AND table_name  = '<CT_TABLE>';

  sql := format('SELECT r.*, %s FROM %I.%I r', select_list, '<SCHEMA>', '<CT_TABLE>');
  RAISE NOTICE 'Generated SQL:%', chr(10) || sql;
  EXECUTE sql;
END $$;
