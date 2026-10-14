-- Per-user engagement. Score = likes + 2*comments + 3*shares
CREATE DATABASE IF NOT EXISTS social_media_dw;
USE social_media_dw;

CREATE TABLE IF NOT EXISTS user_engagement_analytics (
  user_name        STRING,
  total_posts      BIGINT,
  total_likes      BIGINT,
  total_comments   BIGINT,
  total_shares     BIGINT,
  engagement_score BIGINT
)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE user_engagement_analytics
SELECT user_name,
       COUNT(*),
       SUM(likes),
       SUM(comments),
       SUM(shares),
       SUM(likes + 2 * comments + 3 * shares)
FROM social_media_posts
GROUP BY user_name;
