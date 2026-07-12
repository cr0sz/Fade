-- create_analyst_role.sql
-- Read-only analyst role for backtest playground (JupyterLab).
-- Run via: psql -v password='<secret>' -f create_analyst_role.sql
--
-- Idempotent: safe to re-run.
-- Does NOT live in migrations/ because the password must be supplied at runtime,
-- not baked into a numbered migration file.

-- ── 1. Create role (idempotent via DO + dynamic SQL with psql variable) ──────
-- psql expands :'password' before sending to the server, so the password
-- never appears in pg_stat_activity / query logs as a literal.
\set ON_ERROR_STOP on

SELECT NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'analyst') AS should_create
\gset

\if :should_create
CREATE ROLE analyst WITH LOGIN PASSWORD :'password';
\else
ALTER ROLE analyst WITH PASSWORD :'password';
\endif

-- ── 2. Connection privileges ──────────────────────────────────────────────────
GRANT CONNECT ON DATABASE mktintel TO analyst;
GRANT USAGE ON SCHEMA public TO analyst;

-- ── 3. Read access on all existing tables ────────────────────────────────────
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;

-- ── 4. Future tables: auto-grant SELECT to analyst ───────────────────────────
ALTER DEFAULT PRIVILEGES FOR ROLE mktintel IN SCHEMA public
  GRANT SELECT ON TABLES TO analyst;

-- ── 5. Sequences: read-only (currval) — no nextval/setval ────────────────────
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO analyst;
ALTER DEFAULT PRIVILEGES FOR ROLE mktintel IN SCHEMA public
  GRANT SELECT ON SEQUENCES TO analyst;

\echo 'analyst role ready (read-only access to mktintel.public)'
