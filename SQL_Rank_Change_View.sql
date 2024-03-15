WITH ranked_players AS (
  SELECT
    PLAYER_ID,
    PLAYER,
    RANK,
    timestamp,
    LAG(RANK) OVER(PARTITION BY PLAYER_ID ORDER BY timestamp) AS previous_day_rank
  FROM
    `top_500_dataset.top_500_players_new`
)

SELECT
  PLAYER_ID,
  PLAYER,
  RANK AS CURRENT_RANK,
  previous_day_rank,
  (previous_day_rank - RANK) AS RANK_CHANGE,
  timestamp
FROM
  ranked_players
ORDER BY
  timestamp ASC, PLAYER_ID;
