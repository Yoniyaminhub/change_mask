-- change-mask/oracle/change_mask.sql

-- Helper types & function (little-endian mask → set of bit ordinals)
CREATE OR REPLACE TYPE num_list AS TABLE OF NUMBER;
/
CREATE OR REPLACE FUNCTION change_mask_bits(p_mask RAW)
  RETURN num_list PIPELINED
AS
  v_len  PLS_INTEGER := NVL(UTL_RAW.LENGTH(p_mask), 0);
  v_hex  VARCHAR2(32767);
  v_byte PLS_INTEGER;
BEGIN
  v_hex := RAWTOHEX(p_mask);
  FOR i IN 0 .. v_len-1 LOOP
    v_byte := TO_NUMBER(SUBSTR(v_hex, i*2+1, 2), 'XX');
    FOR b IN 0 .. 7 LOOP
      IF BITAND(v_byte, POWER(2, b)) <> 0 THEN
        PIPE ROW(i*8 + b);
      END IF;
    END LOOP;
  END LOOP;
  RETURN;
END;
/
-- Tall: one row per CT row × column
WITH cols AS (
  SELECT (COLUMN_ID - 1) AS bit_ordinal, COLUMN_NAME
  FROM ALL_TAB_COLUMNS
  WHERE OWNER = '<SCHEMA>'
    AND TABLE_NAME = '<CT_TABLE>'
)
SELECT r.*, c.COLUMN_NAME,
       CASE WHEN b.COLUMN_VALUE IS NULL THEN 0 ELSE 1 END AS changed
FROM "<SCHEMA>"."<CT_TABLE>" r
CROSS JOIN cols c
LEFT JOIN TABLE(change_mask_bits(r."<MASK_COL>")) b
       ON b.COLUMN_VALUE = c.bit_ordinal
-- WHERE c.bit_ordinal >= <HEADER_COLS>
ORDER BY c.bit_ordinal;

-- Wide: generate dynamic SELECT text (copy output and run)
DECLARE
  v_sql CLOB := EMPTY_CLOB();
BEGIN
  FOR rec IN (
    SELECT (COLUMN_ID - 1) AS bit_ordinal, COLUMN_NAME
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = '<SCHEMA>' AND TABLE_NAME = '<CT_TABLE>'
    ORDER BY COLUMN_ID
  ) LOOP
    v_sql := v_sql ||
      'CASE WHEN BITAND(TO_NUMBER(SUBSTR(RAWTOHEX(r."' || '<MASK_COL>' || '"), ' ||
      '(' || rec.bit_ordinal || '/8)*2+1, 2), ''XX''), POWER(2, MOD(' ||
      rec.bit_ordinal || ',8))) <> 0 THEN 1 ELSE 0 END AS "' ||
      rec.COLUMN_NAME || '",';
  END LOOP;

  v_sql := 'SELECT r.*, ' || RTRIM(v_sql, ',') || ' FROM "' || '<SCHEMA>' ||
           '"."' || '<CT_TABLE>' || '" r';
  DBMS_OUTPUT.PUT_LINE(v_sql);
END;
/
-- Enable serveroutput to see the text:
-- SET SERVEROUTPUT ON
-- Then copy the generated SELECT and execute it.
