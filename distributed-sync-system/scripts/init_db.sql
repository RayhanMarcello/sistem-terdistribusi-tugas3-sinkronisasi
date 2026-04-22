-- =============================================================
-- Distributed Sync System - MySQL Database Schema
-- =============================================================
-- This script initializes all tables for:
--   • Raft consensus log
--   • Distributed lock table
--   • Cache metadata
--   • Audit logs
--   • Performance metrics
--   • Node registry
-- =============================================================

CREATE DATABASE IF NOT EXISTS distributed_sync
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE distributed_sync;

-- -----------------------------------------------
-- Node Registry
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS nodes (
    id          VARCHAR(64) PRIMARY KEY,
    host        VARCHAR(255) NOT NULL,
    grpc_port   INT NOT NULL,
    rest_port   INT,
    region      VARCHAR(32) DEFAULT 'us',
    status      ENUM('active', 'inactive', 'partitioned') DEFAULT 'active',
    role        ENUM('leader', 'follower', 'candidate') DEFAULT 'follower',
    joined_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_region (region)
) ENGINE=InnoDB;

-- -----------------------------------------------
-- Raft Consensus Log
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS raft_log (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    node_id     VARCHAR(64) NOT NULL,
    term        BIGINT NOT NULL,
    log_index   BIGINT NOT NULL,
    command     JSON NOT NULL,
    committed   BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_node_term (node_id, term),
    INDEX idx_log_index (log_index),
    UNIQUE KEY uk_node_index (node_id, log_index)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS raft_state (
    node_id         VARCHAR(64) PRIMARY KEY,
    current_term    BIGINT DEFAULT 0,
    voted_for       VARCHAR(64),
    commit_index    BIGINT DEFAULT 0,
    last_applied    BIGINT DEFAULT 0,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- -----------------------------------------------
-- Distributed Lock Table
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS distributed_locks (
    lock_id         VARCHAR(255) PRIMARY KEY,
    lock_type       ENUM('shared', 'exclusive') NOT NULL,
    owner_node      VARCHAR(64) NOT NULL,
    owner_client    VARCHAR(255) NOT NULL,
    acquired_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at      TIMESTAMP NOT NULL,
    lease_token     VARCHAR(128) NOT NULL,
    INDEX idx_owner (owner_node),
    INDEX idx_expires (expires_at),
    INDEX idx_type (lock_type)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS lock_waiters (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    lock_id         VARCHAR(255) NOT NULL,
    waiter_client   VARCHAR(255) NOT NULL,
    lock_type       ENUM('shared', 'exclusive') NOT NULL,
    requested_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_lock (lock_id),
    INDEX idx_waiter (waiter_client)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS lock_history (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    lock_id         VARCHAR(255) NOT NULL,
    lock_type       ENUM('shared', 'exclusive') NOT NULL,
    owner_client    VARCHAR(255) NOT NULL,
    node_id         VARCHAR(64) NOT NULL,
    action          ENUM('acquired', 'released', 'expired', 'deadlock_broken') NOT NULL,
    duration_ms     BIGINT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_lock_id (lock_id),
    INDEX idx_created (created_at),
    INDEX idx_action (action)
) ENGINE=InnoDB;

-- -----------------------------------------------
-- Cache Metadata (MOESI)
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS cache_entries (
    cache_key       VARCHAR(512) NOT NULL,
    node_id         VARCHAR(64) NOT NULL,
    moesi_state     ENUM('Modified', 'Owned', 'Exclusive', 'Shared', 'Invalid') NOT NULL,
    value           LONGBLOB,
    version         BIGINT DEFAULT 1,
    access_count    BIGINT DEFAULT 0,
    last_access     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cache_key, node_id),
    INDEX idx_state (moesi_state),
    INDEX idx_node (node_id),
    INDEX idx_last_access (last_access)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS cache_stats (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    node_id         VARCHAR(64) NOT NULL,
    hits            BIGINT DEFAULT 0,
    misses          BIGINT DEFAULT 0,
    invalidations   BIGINT DEFAULT 0,
    evictions       BIGINT DEFAULT 0,
    recorded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_node (node_id),
    INDEX idx_recorded (recorded_at)
) ENGINE=InnoDB;

-- -----------------------------------------------
-- Queue Messages
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS queue_messages (
    message_id      VARCHAR(128) PRIMARY KEY,
    topic           VARCHAR(255) NOT NULL,
    partition_key   VARCHAR(255),
    node_id         VARCHAR(64) NOT NULL,
    payload         JSON NOT NULL,
    status          ENUM('pending', 'processing', 'completed', 'failed', 'dead_letter') DEFAULT 'pending',
    retry_count     INT DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at    TIMESTAMP,
    INDEX idx_topic (topic),
    INDEX idx_status (status),
    INDEX idx_node (node_id),
    INDEX idx_created (created_at)
) ENGINE=InnoDB;

-- -----------------------------------------------
-- Audit Logs (Tamper-proof with hash chaining)
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS audit_logs (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    sequence        BIGINT NOT NULL UNIQUE,
    node_id         VARCHAR(64) NOT NULL,
    event_type      VARCHAR(64) NOT NULL,
    actor           VARCHAR(255),
    resource        VARCHAR(512),
    action          VARCHAR(128) NOT NULL,
    details         JSON,
    ip_address      VARCHAR(45),
    prev_hash       VARCHAR(64),
    entry_hash      VARCHAR(64) NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_node (node_id),
    INDEX idx_event_type (event_type),
    INDEX idx_actor (actor),
    INDEX idx_created (created_at)
) ENGINE=InnoDB;

-- -----------------------------------------------
-- Performance Metrics
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS performance_metrics (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    node_id         VARCHAR(64) NOT NULL,
    metric_name     VARCHAR(128) NOT NULL,
    metric_value    DOUBLE NOT NULL,
    labels          JSON,
    recorded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_node_metric (node_id, metric_name),
    INDEX idx_recorded (recorded_at)
) ENGINE=InnoDB;

-- -----------------------------------------------
-- PBFT State (Bonus A)
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS pbft_messages (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    view_number     BIGINT NOT NULL,
    sequence_number BIGINT NOT NULL,
    node_id         VARCHAR(64) NOT NULL,
    phase           ENUM('pre_prepare', 'prepare', 'commit', 'view_change', 'new_view') NOT NULL,
    digest          VARCHAR(64),
    message_data    JSON,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_view_seq (view_number, sequence_number),
    INDEX idx_node (node_id),
    INDEX idx_phase (phase)
) ENGINE=InnoDB;

-- -----------------------------------------------
-- ML Model Metrics (Bonus C)
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS ml_predictions (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    request_id      VARCHAR(128),
    features        JSON NOT NULL,
    predicted_node  VARCHAR(64) NOT NULL,
    actual_node     VARCHAR(64),
    confidence      DOUBLE,
    latency_ms      DOUBLE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_created (created_at)
) ENGINE=InnoDB;

-- -----------------------------------------------
-- RBAC (Bonus D)
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS rbac_users (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    username        VARCHAR(128) UNIQUE NOT NULL,
    password_hash   VARCHAR(255) NOT NULL,
    role            ENUM('admin', 'producer', 'consumer', 'observer') NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_username (username)
) ENGINE=InnoDB;

-- Insert default admin user (password: admin123)
INSERT IGNORE INTO rbac_users (username, password_hash, role)
VALUES ('admin', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQyCGFS/Que/LabHFQJBOG8kq', 'admin');

-- -----------------------------------------------
-- Geo-Distributed regions (Bonus B)
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS geo_regions (
    region_id       VARCHAR(32) PRIMARY KEY,
    display_name    VARCHAR(128) NOT NULL,
    latitude        DOUBLE,
    longitude       DOUBLE,
    is_active       BOOLEAN DEFAULT TRUE
) ENGINE=InnoDB;

INSERT IGNORE INTO geo_regions (region_id, display_name, latitude, longitude) VALUES
    ('us', 'United States', 37.0902, -95.7129),
    ('eu', 'Europe', 51.1657, 10.4515),
    ('asia', 'Asia-Pacific', 35.8617, 104.1954);
