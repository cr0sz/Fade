-- Personal finance tracker
CREATE TABLE IF NOT EXISTS personal_transactions (
    id          BIGSERIAL PRIMARY KEY,
    date        DATE NOT NULL,
    amount_usd  NUMERIC(14,2) NOT NULL,
    category    TEXT NOT NULL CHECK (category IN
                ('income','housing','food','transport','subscriptions','trading_deposit','other')),
    description TEXT,
    source      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_personal_tx_date ON personal_transactions(date DESC);

-- Trading journal (auto-generated weekly by Ollama)
CREATE TABLE IF NOT EXISTS trading_journal (
    id           BIGSERIAL PRIMARY KEY,
    week_start   DATE NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    content      TEXT NOT NULL,
    model_used   TEXT NOT NULL,
    trade_count  INTEGER NOT NULL DEFAULT 0,
    total_pnl    NUMERIC(14,2) NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trading_journal_week ON trading_journal(week_start);
