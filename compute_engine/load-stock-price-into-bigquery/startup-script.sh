#!/usr/bin/env bash
sudo apt-get -qq -y update
sudo apt-get -qq -y install python3-pip
python3 -m pip install --quiet --user load-stock-price-into-bigquery
${HOME}/.local/bin/load-stock-price-into-bigquery

# delete an instance itself
export NAME=$(curl -X GET http://metadata.google.internal/computeMetadata/v1/instance/name -H 'Metadata-Flavor: Google')
export ZONE=$(curl -X GET http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google')
gcloud --quiet compute instances delete $NAME --zone=$ZONE
