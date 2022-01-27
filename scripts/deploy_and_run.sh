#!/bin/bash

set -x

release='22.02.1'
config=literature-all.conf

path=gs://open-targets-pre-data-releases/${release}
config_path=$path/conf/
jarVersion=$(git rev-parse --short HEAD)
image=2.0-debian10
cluster_name=literature-cluster
jarfile=etl-literature-$jarVersion.jar
jartoexecute=$path/jars/$jarfile

sbt 'set test in assembly := {}' clean assembly

version=$(awk '$1 == "version" {print substr($NF, 2, length($NF)-3)}' build.sbt)
gsutil -m cp /home/jarrod/development/platform-etl-literature/target/scala-2.12/io-*$version.jar $jartoexecute

gcloud beta dataproc clusters create \
  $cluster_name \
  --image-version=$image \
  --properties=yarn:yarn.nodemanager.vmem-check-enabled=false,spark:spark.debug.maxToStringFields=1024 \
  --num-masters=1 \
  --num-workers=5 \
  --zone=europe-west1-d \
  --master-machine-type=n1-highmem-64 \
  --master-boot-disk-size=2000 \
  --worker-machine-type=n1-highmem-64 \
  --worker-boot-disk-size=2000 \
  --project=open-targets-eu-dev \
  --initialization-action-timeout=20m \
  --max-idle=30m \
  --region=europe-west1

gcloud dataproc jobs submit spark \
  --cluster=$cluster_name \
  --project=open-targets-eu-dev \
  --region=europe-west1 \
  --files="${config_path}${config}" \
  --properties=spark.executor.extraJavaOptions=-Dconfig.file=$config,spark.driver.extraJavaOptions=-Dconfig.file=$config \
  --labels=jar=${jarfile%.*jar} \
  --class=io.opentargets.etl.Main \
  --jars=$jartoexecute