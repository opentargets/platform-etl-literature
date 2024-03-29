#!/bin/bash

# --single-node \
gcloud beta dataproc clusters create \
      etl-cluster-literature-mk-64-1 \
      --image-version=2.0-debian10 \
      --region=europe-west1 \
      --single-node \
      --zone=europe-west1-d \
      --master-machine-type=n1-highmem-64 \
      --master-boot-disk-size=2000 \
      --project=open-targets-eu-dev \
      --initialization-action-timeout=20m \
      --max-idle=30m

gcloud beta dataproc jobs submit spark \
   --cluster=etl-cluster-literature-mk-64-1 \
   --project=open-targets-eu-dev \
   --region=europe-west1 \
   --async \
   --files=gs://ot-snapshots/literature/20210920/application-embedding.conf \
   --properties=spark.executor.extraJavaOptions=-Dconfig.file=application-embedding.conf,spark.driver.extraJavaOptions=-Dconfig.file=application-embedding.conf \
   --jar=gs://ot-snapshots/literature/20210920/io-opentargets-etl-literature-assembly-1.11.jar
