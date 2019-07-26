
##############################################################
#
#   Deploying a Xonotic game server over Agones
#
#   https://agones.dev/site/docs/
#   https://www.xonotic.org/
#   https://www.ovh.com/fr/blog/deploying-game-servers-with-agones-on-ovh-managed-kubernetes/
#
##############################################################


echo "Set the GCP Zone as us-central1-f"
gcloud config set compute/zone us-central1-f



echo "Create the Kubernetes Cluster called 'agones'"
gcloud container clusters create agones --cluster-version=1.11 \
  --no-enable-legacy-authorization \
  --tags=game-server \
  --enable-basic-auth \
  --password=supersecretpassword \
  --scopes=https://www.googleapis.com/auth/devstorage.read_only,compute-rw,cloud-platform \
  --num-nodes=3 \
  --machine-type=n1-standard-1


echo "Connect to the 'agones' GKE Cluster"
gcloud config set container/cluster agones


echo "Get GKE Cluster Credentials"
gcloud container clusters get-credentials agones


echo "Set Firewall Rules - Allowing UDP Traffic (tag = 'game-server')"
gcloud compute firewall-rules create game-server-firewall \
  --allow udp:7000-8000 \
  --target-tags game-server \
  --description "Firewall to allow game server udp traffic"


echo "Create role (permission) binding for the service account"
kubectl create clusterrolebinding cluster-admin-binding \
--clusterrole cluster-admin --user `gcloud config get-value account`


# Note: Installing Agones with the install.yaml will setup the TLS certificates
# stored in this repository for securing Kubernetes webhooks communication.
# If you want to generate new certificates or use your own, the helm installation is recommended.
echo "Install Agones on the GKE cluster using the install.yaml from a Github repo"
kubectl create namespace agones-system
kubectl apply -f https://raw.githubusercontent.com/GoogleCloudPlatform/agones/release-0.9.0/install/yaml/install.yaml


echo "Confirm that Agones is up and running"
kubectl describe --namespace agones-system pods | grep Conditions: -A 5
kubectl describe --namespace agones-system pods | grep Status: -A 20


echo "Get Pod Status"
kubectl get --namespace agones-system pods


echo "Creating Xonotic Game Server from gameserver.yaml, found on Github"
kubectl create -f https://raw.githubusercontent.com/GoogleCloudPlatform/agones/release-0.9.0/examples/xonotic/gameserver.yaml


echo "Get Pod Status for Game Server (wait for 'ready')"
kubectl get gameservers


echo "Get Pods"
kubectl get pods



# If you look towards the bottom, you can see there is a Status > State value.
# You are waiting for the State to become Ready, which means that the game
# server is ready to accept connections.
echo "Getting Game Server Status"
for i in {1..10}; do sleep 5 | kubectl describe gameserver | grep Status -A30 ; done


echo "Retrieve the State, IP address, and the allocated port of your GameServer by running"
kubectl get gs -o=custom-columns=NAME:.metadata.name,STATUS:.status.state,IP:.status.address,PORT:.status.ports


echo "Create a Client VM (this will act as the gamer PC)"
gcloud deployment-manager deployments create agones-test --config https://storage.googleapis.com/vwebb-codelabs/agones/agones-test-vm.yaml


echo "SSH into the VM called 'agones-test-vm'"
echo ""
echo "Install the Xonotic Client by running these commands..."
echo "wget https://dl.xonotic.org/xonotic-0.8.2.zip"
echo "unzip xonotic-0.8.2.zip"
echo ""







##################################################################
#
#   Cleanup
#
##################################################################

# Delete GKE Cluster (Agones)
gcloud container clusters delete agones --zone us-central1-f

# Delete VM (Game Client)
gcloud compute instances delete agones-test-vm --zone us-central1-f



#ZEND
