
#################################################################
#
#   Run K-Means Clustering Algorithm
#
#   Runtime: ~2min
#
#################################################################


CREATE OR REPLACE MODEL
  `zproject201807.gaming.bingoblast_cluster_model`
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


#################################################################
#
#   Use the trained model to assign clusters to each record
#   in the Game Play data.
#
#   Runtime: ~30sec
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


#ZEND
