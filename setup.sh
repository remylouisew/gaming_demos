#Setup (assumes the gcloud and bq SKDs are installed)

# Install python dependencies
pip install google-cloud-bigquery==1.12.1
pip install google-cloud-pubsub==0.41.0
pip install random-username==1.0.2

# Create BigQuery Table
bq rm -f -t zproject201807:gaming.gaming_events_stream
bq mk --table --location=US gaming.gaming_events_stream uid:STRING,game_id:STRING,game_server:STRING,game_type:STRING,game_map:STRING,event_datetime:TIMESTAMP,player:STRING,killed:STRING,weapon:STRING,x_cord:INTEGER,y_cord:INTEGER

# Create PubSub Topic
gcloud pubsub topics create gaming_events

# Create PubSub Subscription
gcloud pubsub subscriptions create --topic gaming_events gaming_events_sub

# Setup Dataflow Project (PubSub Subscription > BigQuery Streaming)
# I'm currently doing this manually within the GCP Console, under Dataflow.
# Planning to add the CLI option here. 

#ZEND
