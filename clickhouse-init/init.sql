CREATE DATABASE IF NOT EXISTS reviewsdb;

CREATE TABLE IF NOT EXISTS reviewsdb.reviews (
    id UInt64,
    content String
) ENGINE = MergeTree()
ORDER BY id;
