-- Queries used by the report task.
SET hive.cli.print.header=true;
USE social_media_dw;

SELECT * FROM user_engagement_analytics ORDER BY engagement_score DESC;

SELECT post_id, user_name, likes, comments, shares,
       (likes + 2 * comments + 3 * shares) AS engagement_score
FROM social_media_posts
ORDER BY engagement_score DESC
LIMIT 5;
