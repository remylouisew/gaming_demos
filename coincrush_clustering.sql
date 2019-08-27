
#################################################################
#
#   Run K-Means Clustering Algorithm
#
#   Runtime: ~3min
#
#################################################################


CREATE OR REPLACE MODEL
  `gaming-demos.gaming.bingoblast_cluster_model`
OPTIONS
  (model_type='kmeans',
    num_clusters=5,
    standardize_features = TRUE) AS
SELECT
    EXTRACT(DAYOFWEEK FROM PARSE_DATE("%Y%m%d", Date)) as dow,
    stat_Level,
    stat_NumChips,
    stat_NumCoins,
    stat_NumElitePowers,
    stat_NumKeys,
    stat_NumPowers,
    stat_NumXP,
    beh_TotalEntrances,
    beh_CoinsAwarded,
    beh_CoinsDaubed,
    beh_LevelUps,
    beh_TimeExtends,
    beh_ChipsGained,
    beh_PowerupsGained,
    beh_EngagementTimeMins,
    beh_XPAwarded,
    beh_SquaresDaubed,
    beh_PowerUpsUsed,
    beh_ScoreGained
FROM
    `bingo-blast-rw.bingoblast_s.BBDemo_In_App_Status`





CREATE OR REPLACE MODEL
  `zproject201807.gaming.bingoblast_cluster_model`
OPTIONS
  (model_type='kmeans',
    num_clusters=5,
    standardize_features = TRUE) AS
SELECT
    EXTRACT(DAYOFWEEK FROM PARSE_DATE("%Y%m%d", Date)) as dow,
    stat_Level,
    stat_NumCoins,
    stat_NumPowers,
    beh_TimeExtends,
    beh_EngagementTimeMins,
    beh_ScoreGained
FROM
    `bingo-blast-rw.bingoblast_s.BBDemo_In_App_Status`




CREATE OR REPLACE MODEL
  `gaming-demos.games.bingoblast_cluster_model1`
OPTIONS
  (model_type='kmeans',
    num_clusters=5,
    standardize_features = TRUE) AS
SELECT
    EXTRACT(DAYOFWEEK FROM PARSE_DATE("%Y%m%d", Date)) as dow,
    stat_Level,
    stat_NumCoins,
    stat_NumElitePowers,
    stat_NumKeys,
    stat_NumPowers,
    stat_NumXP,
    beh_TotalEntrances,
    beh_CoinsAwarded,
    beh_LevelUps,
    beh_TimeExtends,
    beh_PowerupsGained,
    beh_EngagementTimeMins,
    beh_PowerUpsUsed,
    beh_ScoreGained
FROM
    `bingo-blast-rw.bingoblast_s.BBDemo_In_App_Status`





#################################################################
#
#   Use the trained model to assign clusters to each record
#   in the Game Play data
#
#   Runtime: ~5sec
#
#################################################################

CREATE OR REPLACE TABLE `zproject201807.gaming.bingoblast_clusters`
AS
SELECT
  * EXCEPT(nearest_centroids_distance)
FROM
  ML.PREDICT( MODEL `zproject201807.gaming.bingoblast_cluster_model`,
    (
    SELECT
        EXTRACT(DAYOFWEEK FROM PARSE_DATE("%Y%m%d", Date)) as dow,
        stat_Level,
        stat_NumCoins,
        stat_NumPowers,
        beh_TimeExtends,
        beh_EngagementTimeMins,
        beh_ScoreGained
    FROM
        `bingo-blast-rw.bingoblast_s.BBDemo_In_App_Status`
    )
)




CREATE OR REPLACE TABLE `gaming-demos.games.bingoblast_clusters`
AS
SELECT
  * EXCEPT(nearest_centroids_distance)
FROM
  ML.PREDICT( MODEL `gaming-demos.games.bingoblast_cluster_model1`,
    (
    SELECT
        EXTRACT(DAYOFWEEK FROM PARSE_DATE("%Y%m%d", Date)) as dow,
        stat_Level,
        stat_NumCoins,
        stat_NumElitePowers,
        stat_NumKeys,
        stat_NumPowers,
        stat_NumXP,
        beh_TotalEntrances,
        beh_CoinsAwarded,
        beh_LevelUps,
        beh_TimeExtends,
        beh_PowerupsGained,
        beh_EngagementTimeMins,
        beh_PowerUpsUsed,
        beh_ScoreGained
    FROM
        `bingo-blast-rw.bingoblast_s.BBDemo_In_App_Status`
    )
)



#################################################################
#
#   Cluster Distribution
#
#################################################################

SELECT
    CENTROID_ID, count(*) as count
FROM
    `zproject201807.gaming.bingoblast_clusters`
GROUP BY CENTROID_ID
ORDER BY CENTROID_ID ASC


#################################################################
#
#   Explore the clustering results (raw, not transposed)
#
#################################################################

SELECT
  *
FROM
  ML.CENTROIDS(MODEL `zproject201807.gaming.bingoblast_cluster_model`)
ORDER BY
  centroid_id


#################################################################
#
#   Explore the clustering results
#
#################################################################


WITH
  T AS (
  SELECT
    centroid_id,
    ARRAY_AGG(STRUCT(feature AS name,
        ROUND(numerical_value,1) AS value)
    ORDER BY
      centroid_id) AS cluster
  FROM
    ML.CENTROIDS(MODEL `zproject201807.gaming.bingoblast_cluster_model`)
  GROUP BY
    centroid_id )
SELECT
  CONCAT('Cluster #', CAST(centroid_id AS STRING)) AS centroid,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'dow') AS dow,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'stat_Level') AS stat_Level,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'stat_NumCoins') AS stat_NumCoins,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'stat_NumPowers') AS stat_NumPowers,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_TimeExtends') AS beh_TimeExtends,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_EngagementTimeMins') AS beh_EngagementTimeMins,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_ScoreGained') AS beh_ScoreGained
FROM
  T
ORDER BY
  centroid ASC









CREATE OR REPLACE TABLE `gaming-demos.games.bingoblast_clusters_aggr`
AS
WITH
  T AS (
  SELECT
    centroid_id,
    ARRAY_AGG(STRUCT(feature AS name,
        ROUND(numerical_value,1) AS value)
    ORDER BY
      centroid_id) AS cluster
  FROM
    ML.CENTROIDS(MODEL `gaming-demos.games.bingoblast_cluster_model1`)
  GROUP BY
    centroid_id )
SELECT
  CONCAT('Cluster #', CAST(centroid_id AS STRING)) AS centroid,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'dow') AS dow,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'stat_Level') AS stat_Level,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'stat_NumCoins') AS stat_NumCoins,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'stat_NumElitePowers') AS stat_NumElitePowers,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'stat_NumKeys') AS stat_NumKeys,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'stat_NumPowers') AS stat_NumPowers,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'stat_NumXP') AS stat_NumXP,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_TotalEntrances') AS beh_TotalEntrances,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_CoinsAwarded') AS beh_CoinsAwarded,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_LevelUps') AS beh_LevelUps,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_TimeExtends') AS beh_TimeExtends,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_PowerupsGained') AS beh_PowerupsGained,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_EngagementTimeMins') AS beh_EngagementTimeMins,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_PowerUpsUsed') AS beh_PowerUpsUsed,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'beh_ScoreGained') AS beh_ScoreGained
FROM
  T
ORDER BY
  centroid ASC



































#################################################################
#
#   Logistic Regression
#
#################################################################


# Simulation

CREATE OR REPLACE TABLE `gaming-demos.taw_clustering.coincrush_game_data`
AS
SELECT
  * EXCEPT ( randompct )
FROM (
  SELECT
    *,
    CASE
      WHEN randompct > 0.98 AND randompct <= 100 AND Alive_Day_Row >= 7 THEN ROUND(rand()*1,2)
      WHEN randompct > 0.93
    AND randompct <= 98
    AND Alive_Day_Row >= 19 THEN ROUND(rand()*2,2)
      WHEN randompct > 0.92 AND randompct <= 93 AND Alive_Day_Row >= 30 THEN ROUND(rand()*4,2)
      WHEN randompct > 0.60
    AND randompct <= 92
    AND Alive_Day_Row >= 14 THEN ROUND(rand()*1.5,2)
    ELSE
    0.00
  END
    AS purchase,
    CASE
      WHEN randompct > 0.00 AND randompct <= 0.15 AND beh_EngagementTimeMins >= 0 THEN 'Devastator'
      WHEN randompct > 0.15
    AND randompct <= 0.35
    AND beh_EngagementTimeMins >= 8 THEN 'Sniper Rifle'
      WHEN randompct > 0.35 AND randompct <= 0.55 AND beh_EngagementTimeMins >= 8 THEN 'Shotgun'
      WHEN randompct > 0.55
    AND randompct <= 0.65
    AND beh_EngagementTimeMins >= 5 THEN 'Electro'
      WHEN randompct > 0.65 AND randompct <= 0.85 AND beh_EngagementTimeMins >= 0 THEN 'Machine Gun'
    ELSE
    'Vortex'
  END
    AS weapon
  FROM (
    SELECT
      *,
      rand() AS randompct
    FROM
      `gaming-demos.games.BBDemo_In_App_Status` ) )








# Prepare Data (aggregate and light feature engineering)

CREATE OR REPLACE TABLE `gaming-demos.games.bingoblast_gamer_aggr`
AS
SELECT
  *,
  (last_seen - first_seen) AS days_active,
  CASE
    WHEN APR2019=0 AND MAY2019=0 THEN 1
    ELSE 0
  END AS attrition
FROM (
  SELECT
    UserId,
    COUNT(*) AS sessions,
    MAX(stat_level) as stat_level,
    MAX(beh_DeviceBrand) as beh_DeviceBrand,
    MAX(stat_Country) as stat_Country,
    AVG(stat_NumChips) as avg_chips,
    AVG(stat_NumCoins) as avg_coins,
    AVG(stat_NumElitePowers) as avg_elitepowers,
    AVG(stat_NumKeys) as avg_numkeys,
    AVG(stat_NumPowers) as avg_numpowers,
    SUM(beh_TotalEntrances) as totalentrances,
    AVG(beh_CoinsAwarded) as avg_coinsawarded,
    AVG(beh_CoinsDaubed) as avg_coinsdaubed,
    SUM(beh_LevelUps) as total_levelups,
    AVG(beh_TimeExtends) as avg_timeextends,
    AVG(beh_ChipsGained) as avg_chipsgained,
    AVG(beh_PowerupsGained) as avg_powerupsgained,
    AVG(beh_EngagementTimeMins) as engagement_time_mins,
    AVG(beh_PowerUpsUsed) as avg_powerups_used,
    AVG(beh_ScoreGained) as avg_score_gained,
    CAST(MIN(Date) AS INT64) AS first_seen,
    CAST(MAX(Date) AS INT64) AS last_seen,
    SUM(JAN2019) as JAN2019,
    SUM(FEB2019) as FEB2019,
    SUM(MAR2019) as MAR2019,
    SUM(APR2019) as APR2019,
    SUM(MAY2019) as MAY2019
  FROM
    (SELECT
        *,
        CASE
            WHEN SUBSTR(Date, 5,2) = '01' THEN 1
            ELSE 0
        END AS JAN2019,
        CASE
            WHEN SUBSTR(Date, 5,2) = '02' THEN 1
            ELSE 0
        END AS FEB2019,
        CASE
            WHEN SUBSTR(Date, 5,2) = '03' THEN 1
            ELSE 0
        END AS MAR2019,
        CASE
            WHEN SUBSTR(Date, 5,2) = '04' THEN 1
            ELSE 0
        END AS APR2019,
        CASE
            WHEN SUBSTR(Date, 5,2) = '05' THEN 1
            ELSE 0
        END AS MAY2019
    FROM
        `bingo-blast-rw.bingoblast_s.BBDemo_In_App_Status`)
  GROUP BY
    UserId
  HAVING
    UserId IS NOT NULL
    AND UserId != ''
  ORDER BY
    UserId )






# Train Model (logistic model)

CREATE OR REPLACE MODEL
  `gaming-demos.games.bingoblast_logistic_model`
OPTIONS
  ( model_type='logistic_reg',
    max_iteration=50,
    input_label_cols=['attrition']
  ) AS
SELECT
  * EXCEPT ( UserId,
    first_seen,
    last_seen,
    jan2019,
    feb2019,
    mar2019,
    apr2019,
    may2019 )
FROM
  `gaming-demos.games.bingoblast_gamer_aggr`












CREATE OR REPLACE MODEL
  `gaming-demos.games.bingoblast_cluster_model_10k`
OPTIONS
  (model_type='kmeans',
    num_clusters=10,
    standardize_features = TRUE) AS
SELECT
    * EXCEPT ( UserId,
    first_seen,
    last_seen )
FROM
    `gaming-demos.games.bingoblast_gamer_aggr`




CREATE OR REPLACE MODEL
  `gaming-demos.games.bingoblast_cluster_model_subset_5k`
OPTIONS
  (model_type='kmeans',
    num_clusters=5,
    standardize_features = TRUE) AS
SELECT
    days_active, totalentrances, avg_chips, avg_powerupsgained, engagement_time_mins, sessions, stat_level, avg_numkeys, avg_score_gained
FROM
    `gaming-demos.games.bingoblast_gamer_aggr`







CREATE OR REPLACE TABLE `gaming-demos.games.bingoblast_clusters`
AS
SELECT
  * EXCEPT(nearest_centroids_distance)
FROM
  ML.PREDICT( MODEL `gaming-demos.games.bingoblast_cluster_model`,
    (
    SELECT
      * EXCEPT ( UserId,
      first_seen,
      last_seen )
    FROM
        `gaming-demos.games.bingoblast_gamer_aggr`
    )
)





SELECT
  centroid_id,
  COUNT(*)
FROM
  `gaming-demos.games.bingoblast_clusters`
GROUP BY
  centroid_id









CREATE OR REPLACE TABLE
  `gaming-demos.games.bingoblast_clusters_scored` AS
WITH
  T AS (
  SELECT
    centroid_id,
    ARRAY_AGG(STRUCT(feature AS name,
        ROUND(numerical_value,1) AS value)
    ORDER BY
      centroid_id) AS cluster
  FROM
    ML.CENTROIDS(MODEL `gaming-demos.games.bingoblast_cluster_model`)
  GROUP BY
    centroid_id )
SELECT
  CONCAT('Cluster #', CAST(centroid_id AS STRING)) AS centroid,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'sessions') AS sessions,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'stat_level') AS stat_level,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'avg_coins') AS avg_coins,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'total_levelups') AS total_levelups,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'engagement_time_mins') AS engagement_time_mins,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'avg_score_gained') AS avg_score_gained,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'days_active') AS days_active,
  (
  SELECT
    value
  FROM
    UNNEST(cluster)
  WHERE
    name = 'attrition') AS attrition
FROM
  T
ORDER BY
  centroid ASC






















SELECT
  *,
  sessions / days_active AS sessions_per_days_active
FROM (
  WITH
    T AS (
    SELECT
      centroid_id,
      ARRAY_AGG(STRUCT(feature AS name,
          ROUND(numerical_value,1) AS value)
      ORDER BY
        centroid_id) AS cluster
    FROM
      ML.CENTROIDS(MODEL `gaming-demos.games.bingoblast_cluster_model_subset_5k`)
    GROUP BY
      centroid_id )
  SELECT
    CONCAT('Cluster #', CAST(centroid_id AS STRING)) AS centroid,
    (
    SELECT
      value
    FROM
      UNNEST(cluster)
    WHERE
      name = 'days_active') AS days_active,
    (
    SELECT
      value
    FROM
      UNNEST(cluster)
    WHERE
      name = 'totalentrances') AS totalentrances,
    (
    SELECT
      value
    FROM
      UNNEST(cluster)
    WHERE
      name = 'avg_chips') AS avg_chips,
    (
    SELECT
      value
    FROM
      UNNEST(cluster)
    WHERE
      name = 'avg_powerupsgained') AS avg_powerupsgained,
    (
    SELECT
      value
    FROM
      UNNEST(cluster)
    WHERE
      name = 'engagement_time_mins') AS engagement_time_mins,
    (
    SELECT
      value
    FROM
      UNNEST(cluster)
    WHERE
      name = 'sessions') AS sessions,
    (
    SELECT
      value
    FROM
      UNNEST(cluster)
    WHERE
      name = 'stat_level') AS stat_level,
    (
    SELECT
      value
    FROM
      UNNEST(cluster)
    WHERE
      name = 'avg_score_gained') AS avg_score_gained,
    (
    SELECT
      value
    FROM
      UNNEST(cluster)
    WHERE
      name = 'avg_numkeys') AS avg_numkeys,
    (
    SELECT
      value
    FROM
      UNNEST(cluster)
    WHERE
      name = 'attrition') AS attrition
  FROM
    T
  ORDER BY
    centroid ASC )





#ZEND
