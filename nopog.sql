--
-- nopog PostgreSQL schema
-- Multi-table key-value store with microsecond wall-clock timestamps (microseconds since epoch)
--

SET statement_timeout = 0;

-- Enable pg_trgm extension for trigram indexes
CREATE EXTENSION IF NOT EXISTS pg_trgm;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;

-- Create timestamp function returning microseconds since epoch.
-- µs wall clock; equal values are possible within the same microsecond;
-- all ordering and cursors tiebreak by key.
CREATE FUNCTION public.nopog_now() RETURNS bigint
    LANGUAGE sql
    AS $$
    SELECT (extract(epoch from clock_timestamp()) * 1000000)::bigint
$$;

-- Create valid function (only allows single * at end of path)
-- Key charset matches github.com/benitogf/ooo key.IsValid: letters, digits, and
-- '*' may start/end a key; separators '/', '-', '_', '.' are allowed only in the
-- middle (a key cannot start or end with one). Single-character keys are allowed.
-- Using LANGUAGE sql for better performance (no PL/pgSQL overhead)
CREATE FUNCTION public.valid(fkey character varying) RETURNS boolean
    LANGUAGE sql
    IMMUTABLE
    AS $_$
    SELECT 
        -- Valid key characters
        (fkey ~ '^[a-zA-Z\*\d]$|^[a-zA-Z\*\d][a-zA-Z\*\d\/._-]*[a-zA-Z\*\d]$')
        -- No duplicate separators
        AND (array_length(string_to_array(fkey, '//'), 1) - 1 = 0)
        -- No wildcard in middle (wildcard count > 0 AND NOT ends with wildcard)
        AND NOT ((array_length(string_to_array(fkey, '*'), 1) - 1 > 0) AND NOT (fkey LIKE '%*'))
        -- No multiple wildcards
        AND NOT (array_length(string_to_array(fkey, '*'), 1) - 1 > 1)
$_$;

-- Create table pair function (creates keys_<name> and values_<name> tables)
CREATE FUNCTION public.create_table(tname character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
BEGIN
    IF tname !~ '^[a-zA-Z][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid table name: must start with letter and contain only alphanumeric and underscore';
    END IF;

    EXECUTE format('
        CREATE TABLE IF NOT EXISTS public.%I (
            key character varying(800) NOT NULL PRIMARY KEY,
            created bigint NOT NULL,
            updated bigint
        )', keys_table);

    EXECUTE format('
        CREATE TABLE IF NOT EXISTS public.%I (
            key character varying(800) NOT NULL PRIMARY KEY,
            data jsonb NOT NULL
        )', values_table);

    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_created ON public.%I (created DESC)', keys_table, keys_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_key ON public.%I (key)', values_table, values_table);
    
    -- Create GIN index for JSONB containment queries (@>, ?, ?&, ?|)
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_data_gin ON public.%I USING GIN (data)', values_table, values_table);
    
    -- Create SP-GiST index for ^@ prefix matching operator
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_key_spgist ON public.%I USING spgist (key)', keys_table, keys_table);
    
    -- Create composite index for prefix + time range queries (most used)
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_key_created ON public.%I (key text_pattern_ops, created DESC)', keys_table, keys_table);

    EXECUTE format('
        DO $fk$ BEGIN
            ALTER TABLE public.%I ADD CONSTRAINT %I_fkey 
            FOREIGN KEY (key) REFERENCES public.%I(key) ON UPDATE CASCADE ON DELETE CASCADE;
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $fk$', values_table, values_table, keys_table);
END;
$$;

-- Drop table pair function
CREATE FUNCTION public.drop_table(tname character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
BEGIN
    EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', values_table);
    EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', keys_table);
END;
$$;

-- Delete function with table parameter
CREATE FUNCTION public.nopog_del(tname character varying, fkey character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    poswildcard integer := POSITION('*' IN fkey);
    nowildcard bool := poswildcard = 0;
    prefix character varying;
BEGIN
    IF NOT public.valid(fkey) THEN
        RAISE EXCEPTION 'invalid key';
    END IF;

    IF nowildcard THEN
        EXECUTE format('DELETE FROM public.%I WHERE key = $1', keys_table) USING fkey;
        RETURN;
    END IF;

    IF fkey = '*' THEN
        EXECUTE format('DELETE FROM public.%I', keys_table);
        RETURN;
    END IF;

    prefix := substring(fkey from 1 for poswildcard - 1);
    EXECUTE format('DELETE FROM public.%I WHERE key::text ^@ $1::text', keys_table)
        USING prefix;
END;
$$;

-- Get function with table parameter (optimized: filter keys first, then LEFT OUTER JOIN)
CREATE FUNCTION public.nopog_get(tname character varying, fkey character varying) 
    RETURNS TABLE(key character varying(800), created bigint, updated bigint, data jsonb)
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
    wildcardPosition integer := position('*' IN fkey);
    noWildcard bool := wildcardPosition = 0;
    prefix character varying;
BEGIN
    IF NOT public.valid(fkey) THEN
        RAISE EXCEPTION 'invalid key';
    END IF;

    IF fkey = '*' THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data
            FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
            ORDER BY k.created DESC, k.key DESC', keys_table, values_table);
        RETURN;
    END IF;

    IF noWildcard THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data
            FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
            WHERE k.key = $1', keys_table, values_table) USING fkey;
        RETURN;
    END IF;

    prefix := substring(fkey from 1 for wildcardPosition - 1);
    RETURN QUERY EXECUTE format('
        SELECT k.key, k.created, k.updated, v.data
        FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
        WHERE k.key::text ^@ $1::text
        ORDER BY k.created DESC, k.key DESC', keys_table, values_table) USING prefix;
END;
$$;

-- Get with time range function (optimized: filter keys by prefix AND time range first, then LEFT OUTER JOIN)
CREATE FUNCTION public.nopog_get_range(tname character varying, fkey character varying, time_from bigint, time_to bigint, result_limit integer) 
    RETURNS TABLE(key character varying(800), created bigint, updated bigint, data jsonb)
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
    wildcardPosition integer := position('*' IN fkey);
    noWildcard bool := wildcardPosition = 0;
    prefix character varying;
    effective_to bigint;
BEGIN
    IF NOT public.valid(fkey) THEN
        RAISE EXCEPTION 'invalid key';
    END IF;
    
    IF time_to = 0 THEN
        effective_to := public.nopog_now();
    ELSE
        effective_to := time_to;
    END IF;

    IF fkey = '*' THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data
            FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
            WHERE k.created >= $1 AND k.created <= $2
            ORDER BY k.created DESC, k.key DESC
            LIMIT $3', keys_table, values_table) USING time_from, effective_to, result_limit;
        RETURN;
    END IF;

    IF noWildcard THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data
            FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
            WHERE k.key = $1 AND k.created >= $2 AND k.created <= $3
            LIMIT $4', keys_table, values_table) USING fkey, time_from, effective_to, result_limit;
        RETURN;
    END IF;

    prefix := substring(fkey from 1 for wildcardPosition - 1);
    RETURN QUERY EXECUTE format('
        SELECT k.key, k.created, k.updated, v.data
        FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
        WHERE k.key::text ^@ $1::text AND k.created >= $2 AND k.created <= $3
        ORDER BY k.created DESC, k.key DESC
        LIMIT $4', keys_table, values_table) USING prefix, time_from, effective_to, result_limit;
END;
$$;

-- Peek with time range function (keys only, optimized)
CREATE FUNCTION public.nopog_peek_range(tname character varying, fkey character varying, time_from bigint, time_to bigint, result_limit integer) 
    RETURNS TABLE(key character varying(800), created bigint, updated bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    wildcardPosition integer := position('*' IN fkey);
    noWildcard bool := wildcardPosition = 0;
    prefix character varying;
    effective_to bigint;
BEGIN
    IF NOT public.valid(fkey) THEN
        RAISE EXCEPTION 'invalid key';
    END IF;
    
    IF time_to = 0 THEN
        effective_to := public.nopog_now();
    ELSE
        effective_to := time_to;
    END IF;

    IF fkey = '*' THEN
        RETURN QUERY EXECUTE format('
            SELECT key, created, updated FROM public.%I
            WHERE created >= $1 AND created <= $2
            ORDER BY created DESC, key DESC
            LIMIT $3', keys_table) USING time_from, effective_to, result_limit;
        RETURN;
    END IF;

    IF noWildcard THEN
        RETURN QUERY EXECUTE format('
            SELECT key, created, updated FROM public.%I
            WHERE key = $1 AND created >= $2 AND created <= $3
            LIMIT $4', keys_table) USING fkey, time_from, effective_to, result_limit;
        RETURN;
    END IF;

    prefix := substring(fkey from 1 for wildcardPosition - 1);
    RETURN QUERY EXECUTE format('
        SELECT key, created, updated FROM public.%I
        WHERE key::text ^@ $1::text AND created >= $2 AND created <= $3
        ORDER BY created DESC, key DESC
        LIMIT $4', keys_table) USING prefix, time_from, effective_to, result_limit;
END;
$$;

-- Peek function with table parameter (keys only)
CREATE FUNCTION public.nopog_peek(tname character varying, fkey character varying) 
    RETURNS TABLE(key character varying(800), created bigint, updated bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    poswildcard integer := POSITION('*' IN fkey);
    nowildcard bool := poswildcard = 0;
    prefix character varying;
BEGIN
    IF NOT public.valid(fkey) THEN
        RAISE EXCEPTION 'invalid key';
    END IF;

    IF fkey = '*' THEN
        RETURN QUERY EXECUTE format('SELECT key, created, updated FROM public.%I ORDER BY created DESC, key DESC', keys_table);
        RETURN;
    END IF;

    IF nowildcard THEN
        RETURN QUERY EXECUTE format('SELECT key, created, updated FROM public.%I WHERE key = $1', keys_table) USING fkey;
        RETURN;
    END IF;

    prefix := substring(fkey from 1 for poswildcard - 1);
    RETURN QUERY EXECUTE format('
        SELECT key, created, updated FROM public.%I
        WHERE key::text ^@ $1::text
        ORDER BY created DESC, key DESC', keys_table) USING prefix;
END;
$$;

-- Batch set function for bulk inserts (keys and values as arrays)
CREATE FUNCTION public.nopog_set_batch(tname character varying, fkeys character varying[], fvalues character varying[]) RETURNS bigint[]
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
    curtime bigint;
    result bigint[] := '{}';
    i integer;
BEGIN
    IF array_length(fkeys, 1) != array_length(fvalues, 1) THEN
        RAISE EXCEPTION 'keys and values arrays must have same length';
    END IF;
    
    FOR i IN 1..array_length(fkeys, 1) LOOP
        IF NOT public.valid(fkeys[i]) OR position('*' IN fkeys[i]) > 0 THEN
            RAISE EXCEPTION 'invalid key: %', fkeys[i];
        END IF;
        
        curtime := public.nopog_now();
        
        EXECUTE format('
            INSERT INTO public.%I (key, created, updated) VALUES ($1, $2, NULL)
            ON CONFLICT (key) DO UPDATE SET updated = $2
        ', keys_table) USING fkeys[i], curtime;
        
        EXECUTE format('
            INSERT INTO public.%I (key, data) VALUES ($1, $2::json)
            ON CONFLICT (key) DO UPDATE SET data = $2::json
        ', values_table) USING fkeys[i], fvalues[i];
        
        result := array_append(result, curtime);
    END LOOP;
    
    RETURN result;
END;
$$;

-- Set function with table parameter (optimized with upsert)
CREATE FUNCTION public.nopog_set(tname character varying, fkey character varying, fvalue character varying) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
    curtime bigint;
    jvalue json := fvalue::json;
    wildcardPosition integer := position('*' IN fkey);
BEGIN
    IF NOT public.valid(fkey) OR wildcardPosition > 0 THEN
        RAISE EXCEPTION 'invalid key';
    END IF;

    curtime := public.nopog_now();

    -- Upsert into keys table
    EXECUTE format('
        INSERT INTO public.%I (key, created, updated) VALUES ($1, $2, NULL)
        ON CONFLICT (key) DO UPDATE SET updated = $2
    ', keys_table) USING fkey, curtime;
    
    -- Upsert into values table
    EXECUTE format('
        INSERT INTO public.%I (key, data) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET data = $2
    ', values_table) USING fkey, jvalue;

    RETURN curtime;
END;
$$;

-- Query by JSONB containment (uses GIN index)
-- Example: SELECT * FROM nopog_get_by_json('mytable', 'prefix/*', '{"status":"active"}')
CREATE FUNCTION public.nopog_get_by_json(tname character varying, fkey character varying, json_filter jsonb)
    RETURNS TABLE(key character varying(800), created bigint, updated bigint, data jsonb)
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
    wildcardPosition integer := position('*' IN fkey);
    noWildcard bool := wildcardPosition = 0;
    prefix character varying;
BEGIN
    IF NOT public.valid(fkey) THEN
        RAISE EXCEPTION 'invalid key';
    END IF;

    IF fkey = '*' THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data
            FROM public.%I k
            INNER JOIN public.%I v ON v.key = k.key
            WHERE v.data @> $1
            ORDER BY k.created DESC, k.key DESC', keys_table, values_table) USING json_filter;
        RETURN;
    END IF;

    IF noWildcard THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data
            FROM public.%I k
            INNER JOIN public.%I v ON v.key = k.key
            WHERE k.key = $1 AND v.data @> $2', keys_table, values_table) USING fkey, json_filter;
        RETURN;
    END IF;

    prefix := substring(fkey from 1 for wildcardPosition - 1);
    RETURN QUERY EXECUTE format('
        SELECT k.key, k.created, k.updated, v.data
        FROM public.%I k
        INNER JOIN public.%I v ON v.key = k.key
        WHERE k.key::text ^@ $1::text AND v.data @> $2
        ORDER BY k.created DESC, k.key DESC', keys_table, values_table) USING prefix, json_filter;
END;
$$;

-- Query by JSONB field value
-- Example: SELECT * FROM nopog_get_by_field('mytable', 'prefix/*', 'status', 'active')
CREATE FUNCTION public.nopog_get_by_field(tname character varying, fkey character varying, field_name text, field_value text)
    RETURNS TABLE(key character varying(800), created bigint, updated bigint, data jsonb)
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
    wildcardPosition integer := position('*' IN fkey);
    noWildcard bool := wildcardPosition = 0;
    prefix character varying;
BEGIN
    IF NOT public.valid(fkey) THEN
        RAISE EXCEPTION 'invalid key';
    END IF;

    IF fkey = '*' THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data
            FROM public.%I k
            INNER JOIN public.%I v ON v.key = k.key
            WHERE v.data->>$1 = $2
            ORDER BY k.created DESC, k.key DESC', keys_table, values_table) USING field_name, field_value;
        RETURN;
    END IF;

    IF noWildcard THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data
            FROM public.%I k
            INNER JOIN public.%I v ON v.key = k.key
            WHERE k.key = $1 AND v.data->>$2 = $3', keys_table, values_table) USING fkey, field_name, field_value;
        RETURN;
    END IF;

    prefix := substring(fkey from 1 for wildcardPosition - 1);
    RETURN QUERY EXECUTE format('
        SELECT k.key, k.created, k.updated, v.data
        FROM public.%I k
        INNER JOIN public.%I v ON v.key = k.key
        WHERE k.key::text ^@ $1::text AND v.data->>$2 = $3
        ORDER BY k.created DESC, k.key DESC', keys_table, values_table) USING prefix, field_name, field_value;
END;
$$;

-- Set with caller-supplied created/updated timestamps (overwrites both on conflict).
-- µs wall clock; ordering and cursors tiebreak by key.
CREATE FUNCTION public.nopog_set_meta(tname character varying, fkey character varying, fvalue character varying, fcreated bigint, fupdated bigint) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
    jvalue json := fvalue::json;
    wildcardPosition integer := position('*' IN fkey);
BEGIN
    IF NOT public.valid(fkey) OR wildcardPosition > 0 THEN
        RAISE EXCEPTION 'invalid key';
    END IF;

    EXECUTE format('
        INSERT INTO public.%I (key, created, updated) VALUES ($1, $2, $3)
        ON CONFLICT (key) DO UPDATE SET created = EXCLUDED.created, updated = EXCLUDED.updated
    ', keys_table) USING fkey, fcreated, fupdated;

    EXECUTE format('
        INSERT INTO public.%I (key, data) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET data = $2
    ', values_table) USING fkey, jvalue;
END;
$$;

-- Additive batch import: existing rows always win (ON CONFLICT DO NOTHING).
-- Returns the number of rows actually inserted into the keys table.
CREATE FUNCTION public.nopog_import(tname character varying, fkeys character varying[], fcreated bigint[], fupdated bigint[], fvalues character varying[]) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
    inserted integer := 0;
    affected integer;
    i integer;
BEGIN
    IF array_length(fkeys, 1) IS DISTINCT FROM array_length(fcreated, 1)
        OR array_length(fkeys, 1) IS DISTINCT FROM array_length(fupdated, 1)
        OR array_length(fkeys, 1) IS DISTINCT FROM array_length(fvalues, 1) THEN
        RAISE EXCEPTION 'keys, created, updated and values arrays must have same length';
    END IF;

    IF fkeys IS NULL OR array_length(fkeys, 1) IS NULL THEN
        RETURN 0;
    END IF;

    FOR i IN 1..array_length(fkeys, 1) LOOP
        IF NOT public.valid(fkeys[i]) OR position('*' IN fkeys[i]) > 0 THEN
            RAISE EXCEPTION 'invalid key: %', fkeys[i];
        END IF;

        EXECUTE format('
            INSERT INTO public.%I (key, created, updated) VALUES ($1, $2, $3)
            ON CONFLICT (key) DO NOTHING
        ', keys_table) USING fkeys[i], fcreated[i], fupdated[i];
        GET DIAGNOSTICS affected = ROW_COUNT;

        EXECUTE format('
            INSERT INTO public.%I (key, data) VALUES ($1, $2::json)
            ON CONFLICT (key) DO NOTHING
        ', values_table) USING fkeys[i], fvalues[i];

        inserted := inserted + affected;
    END LOOP;

    RETURN inserted;
END;
$$;

-- Keyset pagination scan ordered by (created, key) ascending.
-- Rows strictly after the (cursor_created, cursor_key) cursor, up to result_limit.
CREATE FUNCTION public.nopog_scan(tname character varying, cursor_created bigint, cursor_key character varying, result_limit integer)
    RETURNS TABLE(key character varying(800), created bigint, updated bigint, data jsonb)
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT k.key, k.created, k.updated, v.data
        FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
        WHERE (k.created, k.key) > ($1, $2)
        ORDER BY k.created ASC, k.key ASC
        LIMIT $3', keys_table, values_table) USING cursor_created, cursor_key, result_limit;
END;
$$;

-- Get with time range plus a path-segment equality filter.
-- A key matches when split_part(key, '/', p) = seg_value for ANY p in seg_positions.
CREATE FUNCTION public.nopog_get_range_segment(tname character varying, fkey character varying, time_from bigint, time_to bigint, result_limit integer, seg_positions integer[], seg_value character varying)
    RETURNS TABLE(key character varying(800), created bigint, updated bigint, data jsonb)
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
    wildcardPosition integer := position('*' IN fkey);
    noWildcard bool := wildcardPosition = 0;
    prefix character varying;
    effective_to bigint;
BEGIN
    IF NOT public.valid(fkey) THEN
        RAISE EXCEPTION 'invalid key';
    END IF;

    IF time_to = 0 THEN
        effective_to := public.nopog_now();
    ELSE
        effective_to := time_to;
    END IF;

    IF fkey = '*' THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data
            FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
            WHERE k.created >= $1 AND k.created <= $2
              AND EXISTS (SELECT 1 FROM unnest($4::integer[]) p WHERE split_part(k.key, ''/'', p) = $5)
            ORDER BY k.created DESC, k.key DESC
            LIMIT $3', keys_table, values_table) USING time_from, effective_to, result_limit, seg_positions, seg_value;
        RETURN;
    END IF;

    IF noWildcard THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data
            FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
            WHERE k.key = $1 AND k.created >= $2 AND k.created <= $3
              AND EXISTS (SELECT 1 FROM unnest($5::integer[]) p WHERE split_part(k.key, ''/'', p) = $6)
            ORDER BY k.created DESC, k.key DESC
            LIMIT $4', keys_table, values_table) USING fkey, time_from, effective_to, result_limit, seg_positions, seg_value;
        RETURN;
    END IF;

    prefix := substring(fkey from 1 for wildcardPosition - 1);
    RETURN QUERY EXECUTE format('
        SELECT k.key, k.created, k.updated, v.data
        FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
        WHERE k.key::text ^@ $1::text AND k.created >= $2 AND k.created <= $3
          AND EXISTS (SELECT 1 FROM unnest($5::integer[]) p WHERE split_part(k.key, ''/'', p) = $6)
        ORDER BY k.created DESC, k.key DESC
        LIMIT $4', keys_table, values_table) USING prefix, time_from, effective_to, result_limit, seg_positions, seg_value;
END;
$$;