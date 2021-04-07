--
-- PostgreSQL database dump
--

-- Dumped from database version 10.16 (Ubuntu 10.16-0ubuntu0.18.04.1)
-- Dumped by pg_dump version 13.2

-- Started on 2021-04-01 16:06:03

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 512 (class 1247 OID 16542)
-- Name: entry; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.entry AS (
        key character varying(800) COLLATE pg_catalog."C.UTF-8",
        created timestamp without time zone,
        updated timestamp without time zone,
        data json
);


--
-- TOC entry 215 (class 1255 OID 16543)
-- Name: del(character varying); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.del(fkey character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
        poswildcard integer := POSITION('*' IN fkey);
        /* this is how you count characters in pgsql */
        /* https://stackoverflow.com/a/36376584 */
        countseparators integer := array_length(string_to_array(fkey, '/'), 1) - 1;
        nowildcard bool := poswildcard = 0;
BEGIN
                if NOT public.valid(fkey) then
                        raise notice 'invalid key';
                        return;
                end if;

        if nowildcard then
                DELETE FROM public.keys WHERE public.keys.key = fkey;
            return;
        end if;

                if fkey = '*' then
                        DELETE FROM public.keys;
                        return;
                end if;

        DELETE FROM public.keys
                        where public.keys.key ~ fkey
                                AND array_length(string_to_array(public.keys.key, '/'), 1) - 1 = countSeparators;
END;
$$;


--
-- TOC entry 214 (class 1255 OID 16544)
-- Name: get(character varying); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get(fkey character varying) RETURNS SETOF public.entry
    LANGUAGE plpgsql
    AS $$
/* to return a join we declared a type 'entry' */
/* https://dba.stackexchange.com/a/96140 */
DECLARE
        wildcardPosition integer := position('*' IN fkey);
        /* this is how you count characters in pgsql */
        /* https://stackoverflow.com/a/36376584 */
        countSeparators integer := array_length(string_to_array(fkey, '/'), 1) - 1;
        noWildcard bool := wildcardPosition = 0;
BEGIN
                if NOT (select public.valid(fkey)) then
                                raise notice 'invalid key';
                                return;
                end if;

        if fkey = '*' then
                raise notice 'get all';
                return QUERY SELECT public.keys.key, public.keys.created, public.keys.updated, values.data FROM public.keys
                        INNER JOIN public.values
                        ON public.values.key = public.keys.key
                                                ORDER BY public.keys.created DESC;
                return;
        end if;

        if noWildcard then
                raise notice 'no wildcard';
                /* so... return doens't return, you need to double return so it returns */
                return QUERY SELECT public.keys.key, public.keys.created, public.keys.updated, values.data FROM public.keys
                        INNER JOIN public.values
                        ON public.values.key = public.keys.key
                        WHERE public.keys.key = fkey;
                return;
        end if;

        raise notice 'glob pattern';
        return QUERY SELECT public.keys.key, public.keys.created, public.keys.updated, values.data FROM public.keys
                        INNER JOIN public.values
                        ON public.values.key = public.keys.key
                        WHERE public.keys.key ~ fkey AND array_length(string_to_array(public.keys.key, '/'), 1) - 1 = countSeparators
                                                ORDER BY public.keys.created DESC;
END;
$$;


SET default_tablespace = '';

--
-- TOC entry 197 (class 1259 OID 16545)
-- Name: keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.keys (
    created timestamp without time zone NOT NULL,
    key character varying(800) NOT NULL,
    updated timestamp without time zone
);


--
-- TOC entry 211 (class 1255 OID 16551)
-- Name: peek(character varying); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.peek(fkey character varying) RETURNS SETOF public.keys
    LANGUAGE plpgsql
    AS $$
DECLARE
        poswildcard integer := POSITION('*' IN fkey);
        /* this is how you count characters in pgsql */
        /* https://stackoverflow.com/a/36376584 */
        countwildcards integer := array_length(string_to_array(fkey, '*'), 1) - 1;
        countseparators integer := array_length(string_to_array(fkey, '/'), 1) - 1;
        nowildcard bool := poswildcard = 0;
        wildcardquery text := replace(fkey, '*', '%');
BEGIN
        if nowildcard then
                raise notice 'here';
                /* so... return doens't return, you need to double return so it returns */
        return QUERY SELECT * FROM public.keys WHERE public.keys.key = fkey;
                return;
        end if;
        if countwildcards > 1 then
                raise notice 'no here';
                return;
        end if;
        raise notice 'less here';
        return QUERY SELECT * FROM public.keys where public.keys.key like wildcardquery;
END;
$$;


--
-- TOC entry 212 (class 1255 OID 16552)
-- Name: set(character varying, character varying); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set(fkey character varying, fvalue character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
        curtime timestamp := now();
        jvalue json := fvalue::json;
                wildcardPosition integer := position('*' IN fkey);
BEGIN
        if NOT (select public.valid(fkey)) OR wildcardPosition > 0 then
                raise notice 'invalid key';
                return;
        end if;

    INSERT INTO public.keys (key, created, updated) VALUES (fkey, curtime, NULL)
                ON CONFLICT (key) DO
                UPDATE SET updated = curtime;
    INSERT INTO public.values (key, data) VALUES (fkey, jvalue)
                ON CONFLICT (key) DO
                UPDATE SET data = jvalue;
END;
$$;


--
-- TOC entry 213 (class 1255 OID 16553)
-- Name: valid(character varying); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.valid(fkey character varying) RETURNS boolean
    LANGUAGE plpgsql
    AS $_$
DECLARE
        countDupeSeparators integer := array_length(string_to_array(fkey, '//'), 1) - 1;
        countDupewildcards integer := array_length(string_to_array(fkey, '**'), 1) - 1;
        validKeyCharacters bool := fkey ~ '^[a-zA-Z\*\d]$|^[a-zA-Z\*\d][a-zA-Z\*\d\/]+[a-zA-Z\*\d]$';
BEGIN
        return validKeyCharacters AND countDupeSeparators = 0 AND countDupewildcards = 0;
END;
$_$;


--
-- TOC entry 198 (class 1259 OID 16554)
-- Name: values; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."values" (
    key character varying(800) NOT NULL,
    data json NOT NULL
);


--
-- TOC entry 2798 (class 2606 OID 16561)
-- Name: keys keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.keys
    ADD CONSTRAINT keys_pkey PRIMARY KEY (key);


--
-- TOC entry 2800 (class 2606 OID 16563)
-- Name: keys keys_ukey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.keys
    ADD CONSTRAINT keys_ukey UNIQUE (key);


--
-- TOC entry 2803 (class 2606 OID 16565)
-- Name: values keys_vukey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."values"
    ADD CONSTRAINT keys_vukey UNIQUE (key);


--
-- TOC entry 2801 (class 1259 OID 16566)
-- Name: fki_keys; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fki_keys ON public."values" USING btree (key);


--
-- TOC entry 2804 (class 2606 OID 16567)
-- Name: values keys_vfkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."values"
    ADD CONSTRAINT keys_vfkey FOREIGN KEY (key) REFERENCES public.keys(key) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


-- Completed on 2021-04-01 16:06:03

--
-- PostgreSQL database dump complete
--