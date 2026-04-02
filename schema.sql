-- Key–value settings: one logical row per `key`; new poll replaces previous row for that key on merge.
-- ENGINE: ReplacingMergeTree(updated_at) keeps the row with the latest updated_at per key.
-- Latest SOL: SELECT toFloat64OrZero(argMax(`value`, updated_at)) FROM bbb.system WHERE `key` = 'sol-price'
CREATE DATABASE IF NOT EXISTS bbb;

drop table if exists bbb.system;
CREATE TABLE IF NOT EXISTS bbb.system
(
    `key` LowCardinality(String),
    `value` String,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (`key`);

-- Migrating from plain MergeTree append-only:
-- CREATE TABLE bbb.system_new AS bbb.system ENGINE = ReplacingMergeTree(updated_at) ORDER BY (`key`);
-- INSERT INTO bbb.system_new SELECT key, value, updated_at FROM bbb.system;
-- EXCHANGE TABLES bbb.system AND bbb.system_new;
-- DROP TABLE bbb.system_old;
