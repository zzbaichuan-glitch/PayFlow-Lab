CREATE TABLE IF NOT EXISTS dtm.trans_global (
  id BIGINT NOT NULL AUTO_INCREMENT,
  gid VARCHAR(128) NOT NULL,
  trans_type VARCHAR(45) NOT NULL,
  status VARCHAR(12) NOT NULL,
  query_prepared VARCHAR(1024) NOT NULL,
  protocol VARCHAR(45) NOT NULL,
  create_time DATETIME DEFAULT NULL,
  update_time DATETIME DEFAULT NULL,
  finish_time DATETIME DEFAULT NULL,
  rollback_time DATETIME DEFAULT NULL,
  options VARCHAR(1024) DEFAULT '',
  custom_data VARCHAR(1024) DEFAULT '',
  next_cron_interval INT DEFAULT NULL,
  next_cron_time DATETIME DEFAULT NULL,
  owner VARCHAR(128) NOT NULL DEFAULT '',
  ext_data TEXT,
  result VARCHAR(1024) DEFAULT '',
  rollback_reason VARCHAR(1024) DEFAULT '',
  PRIMARY KEY (id),
  UNIQUE KEY uk_dtm_gid (gid),
  KEY idx_dtm_owner (owner),
  KEY idx_dtm_status_cron (status, next_cron_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dtm.trans_branch_op (
  id BIGINT NOT NULL AUTO_INCREMENT,
  gid VARCHAR(128) NOT NULL,
  url VARCHAR(1024) NOT NULL,
  data TEXT,
  bin_data BLOB,
  branch_id VARCHAR(128) NOT NULL,
  op VARCHAR(45) NOT NULL,
  status VARCHAR(45) NOT NULL,
  finish_time DATETIME DEFAULT NULL,
  rollback_time DATETIME DEFAULT NULL,
  create_time DATETIME DEFAULT NULL,
  update_time DATETIME DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_dtm_branch (gid, branch_id, op)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dtm.kv (
  id BIGINT NOT NULL AUTO_INCREMENT,
  cat VARCHAR(45) NOT NULL,
  k VARCHAR(128) NOT NULL,
  v TEXT,
  version BIGINT DEFAULT 1,
  create_time DATETIME DEFAULT NULL,
  update_time DATETIME DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_dtm_kv (cat, k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dtm_barrier.barrier (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  trans_type VARCHAR(45) DEFAULT '',
  gid VARCHAR(128) DEFAULT '',
  branch_id VARCHAR(128) DEFAULT '',
  op VARCHAR(45) DEFAULT '',
  barrier_id VARCHAR(45) DEFAULT '',
  reason VARCHAR(45) DEFAULT '' COMMENT 'the branch type that inserted this row',
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY idx_barrier_create_time (create_time),
  KEY idx_barrier_update_time (update_time),
  UNIQUE KEY uk_barrier (gid, branch_id, op, barrier_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
