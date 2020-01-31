#Setup (assumes the gcloud and bq SKDs are installed)

# Variables
export GCP_PROJECT=gaming-demos
export BIGQUERY_DATASET=streaming
export BIGQUERY_TABLE=game-logs
export PUBSUB_TOPIC=game-logs

# Install python dependencies
pip install google-cloud-bigquery==1.12.1
pip install google-cloud-pubsub==0.41.0
pip install random-username==1.0.2

# Create BigQuery Table
bq rm -f -t $GCP_PROJECT:$BIGQUERY_DATASET.$BIGQUERY_TABLE
bq --location=US mk --dataset $GCP_PROJECT:BIGQUERY_DATASET
bq mk --table --location=US $BIGQUERY_DATASET.$BIGQUERY_TABLE uid:STRING,game_id:STRING,game_server:STRING,game_type:STRING,game_map:STRING,event_datetime:TIMESTAMP,player:STRING,killed:STRING,weapon:STRING,x_cord:INTEGER,y_cord:INTEGER

# Create PubSub Topic
gcloud pubsub topics create $PUBSUB_TOPIC

# Create PubSub Subscription
gcloud pubsub subscriptions create --topic $PUBSUB_TOPIC $PUBSUB_TOPIC-sub

# Setup Dataflow Project (PubSub Subscription > BigQuery Streaming)
# I'm currently doing this manually within the GCP Console, under Dataflow.
# Planning to add the CLI option here. 

#ZEND
