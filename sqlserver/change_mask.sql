-- change-mask/sqlserver/change_mask.sql
-- Placeholders to set:
DECLARE @ct SYSNAME = N'<SCHEMA>.<CT_TABLE>';
DECLARE @mask SYSNAME = N'<MASK_COL>';  -- e.g., header__change_mask
DECLARE @header_cols INT = <HEADER_COLS>; -- e.g., 5

/* ==========================================================================
   Helper: decode little-endian change mask into bit ordinals (0-based)
   ========================================================================== */
IF OBJECT_ID('dbo.fn_change_mask_bits') IS NOT NULL
  DROP FUNCTION dbo.fn_change_mask_bits;
GO
CREATE FUNCTION dbo.fn_change_mask_bits (@mask VARBINARY(128))
RETURNS TABLE
AS
RETURN
WITH Tally(n) AS (
  SELECT TOP (8 * ISNULL(DATALENGTH(@mask), 0))
         ROW_NUMBER() OVER (ORDER BY (SELECT 1)) - 1
  FROM sys.all_objects
)
SELECT n AS bit_ordinal
FROM Tally
WHERE (ASCII(SUBSTRING(@mask, (n/8)+1, 1)) & POWER(CAST(2 AS INT), n % 8)) <> 0;
GO

/* ==========================================================================
   Tall view: one row per CT row × column, with 0/1 changed flag
   ========================================================================== */
;WITH cols AS (
  SELECT (c.column_id - 1) AS bit_ordinal, c.name
  FROM sys.columns c
  WHERE c.[object_id] = OBJECT_ID(@ct)
)
SELECT r.*,
       cols.name AS column_name,
       CASE WHEN b.bit_ordinal IS NULL THEN 0 ELSE 1 END AS changed
FROM  (SELECT * FROM  /*+ your predicate here */  ) AS dummy  -- keep placeholder
RIGHT JOIN (SELECT * FROM sys.objects) AS _ignore ON 1=0       -- no-op to keep editor happy
-- Real query:
-- FROM <SCHEMA>.<CT_TABLE> AS r
-- CROSS APPLY (SELECT cols.bit_ordinal, cols.name FROM cols) cols
-- LEFT JOIN dbo.fn_change_mask_bits(r.[<MASK_COL>]) b
--        ON b.bit_ordinal = cols.bit_ordinal
-- WHERE cols.bit_ordinal >= @header_cols  -- optional
-- ORDER BY cols.bit_ordinal;

-- Replace the FROM above with your real CT table like:
-- FROM <SCHEMA>.<CT_TABLE> AS r

/* ==========================================================================
   Wide view: dynamic SELECT with a 0/1 projection per CT column
   ========================================================================== */
DECLARE @sql NVARCHAR(MAX) = N'';
;WITH cols AS (
  SELECT (c.column_id - 1) AS bit_ordinal, QUOTENAME(c.name) AS qname
  FROM sys.columns c
  WHERE c.[object_id] = OBJECT_ID(@ct)
)
SELECT @sql = STRING_AGG(
  CONCAT(
    'CASE WHEN (ASCII(SUBSTRING(r.', @mask, ', (', bit_ordinal, '/8)+1, 1))',
    ' & POWER(CAST(2 AS INT), ', bit_ordinal, ' % 8)) <> 0 THEN 1 ELSE 0 END AS ', qname
  ),
  ','
)
FROM cols;

SET @sql = N'SELECT r.*, ' + @sql + N' FROM ' + @ct + N' r;';
PRINT @sql;  -- Inspect the generated SQL
EXEC sp_executesql @sql;  -- Run it
