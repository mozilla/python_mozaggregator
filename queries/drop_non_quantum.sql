CREATE OR REPLACE FUNCTION try_cast_int(p_in TEXT, p_default INT default null)
RETURNS INT
LANGUAGE plpgsql;
AS $$
BEGIN
  BEGIN
    RETURN $1::INT;
  EXCEPTION 
    WHEN others THEN
       RETURN p_default;
  END;
END;
$$;

CREATE OR REPLACE FUNCTION reverse(TEXT)
RETURNS TEXT
AS $$
SELECT array_to_string(ARRAY(
  SELECT SUBSTRING($1, s.i,1) FROM generate_series(LENGTH($1), 1, -1) AS s(i)
  ), '');
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION drop_tables_before_date(IN _schema TEXT, IN _min_date INT) 
RETURNS void 
LANGUAGE plpgsql
AS
$$
DECLARE
    row record;
BEGIN
    FOR row IN 
        SELECT
            table_schema,
            table_name
        FROM
            information_schema.tables
        WHERE
            table_type = 'BASE TABLE'
        AND
            table_schema = _schema
        AND
            try_cast_int(reverse(split_part(reverse(table_name), '_', 1)), _min_date-1) < _min_date
    LOOP
        EXECUTE 'DROP TABLE ' || quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
        RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
    END LOOP;
END;
$$;

SELECT drop_tables_before_date('public', '20180101');
