
#################################################################
#
#   Run K-Means Clustering Algorithm against Game Play Data
#
#   Runtime: ~3min
#
#################################################################

CREATE OR REPLACE MODEL
  `zproject201807.gaming.game_cluster_model`
OPTIONS
  (model_type='kmeans',
    num_clusters=5,
    standardize_features = TRUE) AS
SELECT
    game_type,
    game_map,
    weapon,
    x_cord,
    y_cord,
    EXTRACT(DAYOFWEEK FROM event_datetime) as dow,
    EXTRACT(HOUR FROM event_datetime) as hour
  FROM
    `gaming-taw-analytics-qwiklab.xonotic_analytics.events_ml`

#################################################################
#
#   Use the trained model to assign clusters to each record
#   in the Game Play data
#
#   Runtime: ~5sec
#
#################################################################

CREATE OR REPLACE TABLE `zproject201807.gaming.game_clusters`
AS
SELECT
  * EXCEPT(nearest_centroids_distance)
FROM
  ML.PREDICT( MODEL `zproject201807.gaming.game_cluster_model`,
    (
        SELECT
        game_type,
        game_map,
        weapon,
        x_cord,
        y_cord,
        EXTRACT(DAYOFWEEK FROM event_datetime) as dow,
        EXTRACT(HOUR FROM event_datetime) as hour
    FROM
      `gaming-taw-analytics-qwiklab.xonotic_analytics.events_ml`
    )
)

#################################################################
#
#   Explore the clustering results
#
#################################################################

SELECT
    CENTROID_ID, count(*) as count
FROM 
    `zproject201807.gaming.game_clusters`
GROUP BY 
    CENTROID_ID
ORDER BY
    CENTROID_ID asc


SELECT
  CASE CENTROID_ID WHEN 1 THEN 'Cluster #1'
                   WHEN 2 THEN 'Cluster #2'
                   WHEN 3 THEN 'Cluster #3'
                   WHEN 4 THEN 'Cluster #4'
                   WHEN 5 THEN 'Cluster #5'
                   ELSE '' END AS CLUSTER_NAME,
  CENTROID_ID,
  hour
FROM
(
  SELECT
    *,
    ROW_NUMBER() OVER ( PARTITION BY CENTROID_ID ) as rownum
  FROM
    (
    SELECT
      CENTROID_ID, 
      hour,
      count(*) as count
    FROM 
      `zproject201807.gaming.game_clusters`
    GROUP BY 
      CENTROID_ID, hour
    )
)
WHERE rownum <= 2


SELECT
  CASE CENTROID_ID WHEN 1 THEN 'Cluster #1'
                   WHEN 2 THEN 'Cluster #2'
                   WHEN 3 THEN 'Cluster #3'
                   WHEN 4 THEN 'Cluster #4'
                   WHEN 5 THEN 'Cluster #5'
                   ELSE '' END AS CLUSTER_NAME,
  CENTROID_ID,
  CASE dow         WHEN 1 THEN 'Sunday'
                   WHEN 2 THEN 'Monday'
                   WHEN 3 THEN 'Tuesday'
                   WHEN 4 THEN 'Wednesday'
                   WHEN 5 THEN 'Thursday'
                   WHEN 6 THEN 'Friday'
                   WHEN 7 THEN 'Saturday'
                   ELSE '' END AS DAY_OF_WEEK
FROM
(
  SELECT
    *,
    ROW_NUMBER() OVER ( PARTITION BY CENTROID_ID ORDER BY CENTROID_ID, count desc ) as rownum
  FROM
    (
    SELECT
        CENTROID_ID, 
        dow,
        count(*) as count
    FROM 
        `zproject201807.gaming.game_clusters`
    GROUP BY 
        CENTROID_ID, dow
    ORDER BY 
        CENTROID_ID, count desc
    )
)
WHERE rownum <= 2


SELECT
  CENTROID_ID,
  CASE CENTROID_ID WHEN 1 THEN 'Cluster #1'
                   WHEN 2 THEN 'Cluster #2'
                   WHEN 3 THEN 'Cluster #3'
                   WHEN 4 THEN 'Cluster #4'
                   WHEN 5 THEN 'Cluster #5'
                   ELSE '' END AS CLUSTER_NAME,
  STRING_AGG(game_type, ", ") as game_types
FROM
(
  SELECT
    *,
    ROW_NUMBER() OVER ( PARTITION BY CENTROID_ID ORDER BY CENTROID_ID, count desc ) as rownum
  FROM
    (
    SELECT
        CENTROID_ID, 
        game_type,
        count(*) as count
    FROM 
        `zproject201807.gaming.game_clusters`
    GROUP BY 
        CENTROID_ID, game_type
    ORDER BY 
        CENTROID_ID, count desc
    )
)
WHERE rownum <= 2
GROUP BY CLUSTER_NAME, CENTROID_ID 
ORDER BY CENTROID_ID ASC


#ZEND
