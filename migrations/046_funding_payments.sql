-- Realized funding-fee payments pulled from Binance income history.
-- income < 0 = funding paid (cost); income > 0 = funding received.
-- tran_id is Binance's globally-unique income id — idempotent re-polls via PK.
CREATE TABLE IF NOT EXISTS funding_payments (
    tran_id  BIGINT           PRIMARY KEY,
    symbol   TEXT             NOT NULL,
    income   DOUBLE PRECISION NOT NULL,
    paid_at  TIMESTAMPTZ      NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_funding_payments_paid_at ON funding_payments (paid_at);
