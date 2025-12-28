-- Enable pg_trgm extension for trigram indexes
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create monotonic clock sequence with cache for high performance
-- CACHE 1000 keeps values in memory, reducing disk I/O and lock contention
CREATE SEQUENCE IF NOT EXISTS public.monotonic_clock_seq CACHE 1000;

-- Drop existing functions to recreate them
DROP FUNCTION IF EXISTS public.valid(character varying);
DROP FUNCTION IF EXISTS public.monotonic_now();
DROP FUNCTION IF EXISTS public.create_table(character varying);
DROP FUNCTION IF EXISTS public.drop_table(character varying);
DROP FUNCTION IF EXISTS public.nopog_del(character varying, character varying);
DROP FUNCTION IF EXISTS public.nopog_get(character varying, character varying);
DROP FUNCTION IF EXISTS public.nopog_peek(character varying, character varying);
DROP FUNCTION IF EXISTS public.nopog_set(character varying, character varying, character varying);
DROP FUNCTION IF EXISTS public.nopog_set_batch(character varying, character varying[], character varying[]);
DROP FUNCTION IF EXISTS public.nopog_get_range(character varying, character varying, bigint, bigint, integer);
DROP FUNCTION IF EXISTS public.nopog_peek_range(character varying, character varying, bigint, bigint, integer);

-- Create monotonic timestamp function using cached sequence
-- Returns microseconds since epoch, guaranteed to be strictly increasing
-- Hybrid approach: uses GREATEST of sequence and current time to maintain time correlation
CREATE OR REPLACE FUNCTION public.monotonic_now() RETURNS bigint
    LANGUAGE sql
    AS $$
    SELECT GREATEST(
        nextval('public.monotonic_clock_seq'),
        (extract(epoch from clock_timestamp()) * 1000000)::bigint
    )
$$;

-- Create valid function (only allows single * at end of path)
-- Using LANGUAGE sql for better performance (no PL/pgSQL overhead)
CREATE OR REPLACE FUNCTION public.valid(fkey character varying) RETURNS boolean
    LANGUAGE sql
    IMMUTABLE
    AS $_$
    SELECT 
        -- Valid key characters
        (fkey ~ '^[a-zA-Z\*\d]$|^[a-zA-Z\*\d][a-zA-Z\*\d\/]+[a-zA-Z\*\d]$')
        -- No duplicate separators
        AND (array_length(string_to_array(fkey, '//'), 1) - 1 = 0)
        -- No wildcard in middle (wildcard count > 0 AND NOT ends with wildcard)
        AND NOT ((array_length(string_to_array(fkey, '*'), 1) - 1 > 0) AND NOT (fkey LIKE '%*'))
        -- No multiple wildcards
        AND NOT (array_length(string_to_array(fkey, '*'), 1) - 1 > 1)
$_$;

-- Create table pair function (creates keys_<name> and values_<name> tables)
CREATE OR REPLACE FUNCTION public.create_table(tname character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys_table text := 'keys_' || tname;
    values_table text := 'values_' || tname;
BEGIN
    -- Validate table name (alphanumeric and underscore only)
    IF tname !~ '^[a-zA-Z][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid table name: must start with letter and contain only alphanumeric and underscore';
    END IF;

    -- Create keys table
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS public.%I (
            key character varying(800) NOT NULL PRIMARY KEY,
            created bigint NOT NULL,
            updated bigint
        )', keys_table);

    -- Create values table
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS public.%I (
            key character varying(800) NOT NULL PRIMARY KEY,
            data json NOT NULL
        )', values_table);

    -- Create indexes
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_created ON public.%I (created DESC)', keys_table, keys_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_key ON public.%I (key)', values_table, values_table);
    
    -- Create SP-GiST index for ^@ prefix matching operator
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_key_spgist ON public.%I USING spgist (key)', keys_table, keys_table);
    
    -- Create composite index for prefix + time range queries (most used)
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_key_created ON public.%I (key text_pattern_ops, created DESC)', keys_table, keys_table);

    -- Add foreign key
    EXECUTE format('
        DO $fk$ BEGIN
            ALTER TABLE public.%I ADD CONSTRAINT %I_fkey 
            FOREIGN KEY (key) REFERENCES public.%I(key) ON UPDATE CASCADE ON DELETE CASCADE;
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $fk$', values_table, values_table, keys_table);
END;
$$;

-- Drop table pair function
CREATE OR REPLACE FUNCTION public.drop_table(tname character varying) RETURNS void
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
CREATE OR REPLACE FUNCTION public.nopog_del(tname character varying, fkey character varying) RETURNS void
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
CREATE OR REPLACE FUNCTION public.nopog_get(tname character varying, fkey character varying) 
    RETURNS TABLE(key character varying(800), created bigint, updated bigint, data json)
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
            ORDER BY k.created DESC', keys_table, values_table);
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
        ORDER BY k.created DESC', keys_table, values_table) USING prefix;
END;
$$;

-- Get with time range function (optimized: filter keys by prefix AND time range first, then LEFT OUTER JOIN)
CREATE OR REPLACE FUNCTION public.nopog_get_range(tname character varying, fkey character varying, time_from bigint, time_to bigint, result_limit integer) 
    RETURNS TABLE(key character varying(800), created bigint, updated bigint, data json)
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
    
    -- Handle to=0 as "now"
    IF time_to = 0 THEN
        effective_to := public.monotonic_now();
    ELSE
        effective_to := time_to;
    END IF;

    IF fkey = '*' THEN
        RETURN QUERY EXECUTE format('
            SELECT k.key, k.created, k.updated, v.data 
            FROM public.%I k LEFT OUTER JOIN public.%I v ON v.key = k.key
            WHERE k.created >= $1 AND k.created <= $2
            ORDER BY k.created DESC
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
        ORDER BY k.created DESC
        LIMIT $4', keys_table, values_table) USING prefix, time_from, effective_to, result_limit;
END;
$$;

-- Peek with time range function (keys only, optimized)
CREATE OR REPLACE FUNCTION public.nopog_peek_range(tname character varying, fkey character varying, time_from bigint, time_to bigint, result_limit integer) 
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
        effective_to := public.monotonic_now();
    ELSE
        effective_to := time_to;
    END IF;

    IF fkey = '*' THEN
        RETURN QUERY EXECUTE format('
            SELECT key, created, updated FROM public.%I
            WHERE created >= $1 AND created <= $2
            ORDER BY created DESC
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
        ORDER BY created DESC
        LIMIT $4', keys_table) USING prefix, time_from, effective_to, result_limit;
END;
$$;

-- Peek function with table parameter (keys only)
CREATE OR REPLACE FUNCTION public.nopog_peek(tname character varying, fkey character varying) 
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
        RETURN QUERY EXECUTE format('SELECT key, created, updated FROM public.%I ORDER BY created DESC', keys_table);
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
        ORDER BY created DESC', keys_table) USING prefix;
END;
$$;

-- Batch set function for bulk inserts (keys and values as arrays)
CREATE OR REPLACE FUNCTION public.nopog_set_batch(tname character varying, fkeys character varying[], fvalues character varying[]) RETURNS bigint[]
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
        
        curtime := public.monotonic_now();
        
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
CREATE OR REPLACE FUNCTION public.nopog_set(tname character varying, fkey character varying, fvalue character varying) RETURNS bigint
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

    curtime := public.monotonic_now();

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
