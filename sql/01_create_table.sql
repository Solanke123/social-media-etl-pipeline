-- External table over the processed TSV in HDFS.
-- Run with: hive --hivevar HDFS_PROCESSED=/social_media/processed -f sql/01_create_table.sql
CREATE DATABASE IF NOT EXISTS social_media_dw;
USE social_media_dw;

CREATE EXTERNAL TABLE IF NOT EXISTS social_media_posts (
  post_id        STRING,
  user_name      STRING,
  post_text      STRING,
  likes          INT,
  comments       INT,
  shares         INT,
  post_timestamp STRING,
  media_file     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:HDFS_PROCESSED}'
TBLPROPERTIES ('skip.header.line.count'='1');

SELECT COUNT(*) AS post_count FROM social_media_posts;
