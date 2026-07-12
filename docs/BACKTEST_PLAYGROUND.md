# Backtest Playground — JupyterLab

A lightweight, on-demand JupyterLab environment for ad-hoc analysis and backtesting
against the live market-intelligence database.

**Zero cost at rest** — the service only starts when you explicitly request the
`analysis` profile. Normal `docker compose up -d` never touches it.

---

## Start

```bash
docker compose --profile analysis up -d jupyter
```

Then open in your browser: **http://127.0.0.1:8888**

If you're on a remote server, open an SSH tunnel first:

```bash
ssh -L 8888:127.0.0.1:8888 user@your-server
```

Then navigate to `http://127.0.0.1:8888` locally.

**Token**: set in `.env` as `JUPYTER_TOKEN`. Copy it from there:

```bash
grep JUPYTER_TOKEN /opt/market-intelligence/.env
```

---

## Stop

```bash
docker compose --profile analysis stop jupyter
```

Or to remove the container entirely (notebooks on disk are unaffected):

```bash
docker compose --profile analysis down jupyter
```

---

## Notebooks

Notebooks live in `./notebooks/` and are mounted read-write at
`/home/work/notebooks` inside the container.

The starter notebook `playground_starter.ipynb` covers:
- DB connection helper (read-only `analyst` role)
- Hourly price chart for any symbol
- Trade history: PnL by `close_reason` and `router_action`
- Fade analysis: forward returns after large negative 24h moves
- Funding rates / liquidation events preview
- Vectorized backtest skeleton

> **Note**: `*.ipynb` files are in `.gitignore` — notebook outputs (which may
> contain query results or credentials) are never committed.

---

## Database access — read-only guarantee

The container connects as the `analyst` Postgres role, which has:
- `SELECT` on all current and future tables in `public`
- No `INSERT`, `UPDATE`, `DELETE`, or DDL privileges

This is enforced at the database level, not just in application code.

To verify:

```bash
# SELECT works
docker exec market-intelligence-postgres-1 \
  psql "postgresql://analyst:<password>@localhost/mktintel" \
  -c "SELECT count(*) FROM ticker_quotes"

# INSERT is rejected
docker exec market-intelligence-postgres-1 \
  psql "postgresql://analyst:<password>@localhost/mktintel" \
  -c "INSERT INTO ticker_quotes (symbol, price, change_pct, volume, exchange) \
      VALUES ('TEST', 1.0, 0.0, 0.0, 'CRYPTO')"
# Expected: ERROR: permission denied for table ticker_quotes
```

---

## Re-running analyst role setup

If you need to recreate or rotate the analyst password:

```bash
# 1. Generate a new password, update .env
NEW_PW=$(openssl rand -base64 20 | tr -d '/+=')
# Update ANALYST_DB_PASSWORD in .env manually, then:

# 2. Re-apply the SQL
docker cp sql/create_analyst_role.sql market-intelligence-postgres-1:/tmp/
docker exec market-intelligence-postgres-1 \
  psql -U mktintel -d mktintel \
  -v "password=${NEW_PW}" \
  -f /tmp/create_analyst_role.sql
```

---

## Resource limits

| Setting    | Value  |
|------------|--------|
| Memory     | 1 GB   |
| Port       | 127.0.0.1:8888 (localhost only) |
| Profile    | `analysis` (never auto-started) |
| Image      | `docker/jupyter.Dockerfile` (python:3.11-slim base) |
