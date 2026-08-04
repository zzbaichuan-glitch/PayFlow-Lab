USE payflow;

CREATE TABLE IF NOT EXISTS payments (
  id VARCHAR(64) NOT NULL,
  gid VARCHAR(128) NULL,
  execution_mode VARCHAR(24) NOT NULL DEFAULT 'distributed',
  order_id VARCHAR(128) NOT NULL,
  account_id VARCHAR(128) NOT NULL,
  amount_cents BIGINT NOT NULL,
  currency CHAR(3) NOT NULL,
  status VARCHAR(24) NOT NULL,
  failure_reason VARCHAR(512) NOT NULL DEFAULT '',
  last_callback_sequence BIGINT NOT NULL DEFAULT 0,
  idempotency_key VARCHAR(128) NOT NULL,
  request_fingerprint CHAR(64) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_payments_gid (gid),
  UNIQUE KEY uk_payments_idempotency (idempotency_key),
  KEY idx_payments_order_id (order_id),
  KEY idx_payments_status_updated (status, updated_at),
  CONSTRAINT chk_payments_amount CHECK (amount_cents > 0),
  CONSTRAINT chk_payments_status CHECK (status IN ('INIT','PROCESSING','SUCCESS','FAILED','CLOSED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS payment_events (
  id BIGINT NOT NULL AUTO_INCREMENT,
  payment_id VARCHAR(64) NOT NULL,
  sequence BIGINT NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  from_status VARCHAR(24) NOT NULL DEFAULT '',
  to_status VARCHAR(24) NOT NULL DEFAULT '',
  message VARCHAR(768) NOT NULL,
  metadata JSON NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_payment_event_sequence (payment_id, sequence),
  KEY idx_payment_events_created (created_at),
  CONSTRAINT fk_payment_events_payment FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS accounts (
  id VARCHAR(128) NOT NULL,
  available_cents BIGINT NOT NULL,
  frozen_cents BIGINT NOT NULL,
  currency CHAR(3) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT chk_accounts_available CHECK (available_cents >= 0),
  CONSTRAINT chk_accounts_frozen CHECK (frozen_cents >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO accounts(id, available_cents, frozen_cents, currency, updated_at)
VALUES ('demo-user', 1000000, 0, 'CNY', UTC_TIMESTAMP(6))
ON DUPLICATE KEY UPDATE id=VALUES(id);

CREATE TABLE IF NOT EXISTS account_reservations (
  id BIGINT NOT NULL AUTO_INCREMENT,
  gid VARCHAR(128) NOT NULL,
  payment_id VARCHAR(64) NOT NULL,
  account_id VARCHAR(128) NOT NULL,
  amount_cents BIGINT NOT NULL,
  status VARCHAR(24) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_account_reservation_gid (gid),
  UNIQUE KEY uk_account_reservation_payment (payment_id),
  KEY idx_account_reservation_account (account_id),
  CONSTRAINT fk_reservation_payment FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE CASCADE,
  CONSTRAINT fk_reservation_account FOREIGN KEY (account_id) REFERENCES accounts(id),
  CONSTRAINT chk_reservation_status CHECK (status IN ('FROZEN','CONFIRMED','CANCELLED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS ledger_entries (
  id BIGINT NOT NULL AUTO_INCREMENT,
  gid VARCHAR(128) NOT NULL,
  payment_id VARCHAR(64) NOT NULL,
  account_id VARCHAR(128) NOT NULL,
  amount_cents BIGINT NOT NULL,
  direction VARCHAR(12) NOT NULL,
  status VARCHAR(24) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_ledger_gid (gid),
  UNIQUE KEY uk_ledger_payment_direction (payment_id, direction),
  KEY idx_ledger_status_created (status, created_at),
  CONSTRAINT fk_ledger_payment FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE CASCADE,
  CONSTRAINT chk_ledger_status CHECK (status IN ('PREPARED','POSTED','CANCELLED')),
  CONSTRAINT chk_ledger_direction CHECK (direction IN ('DEBIT','CREDIT'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
