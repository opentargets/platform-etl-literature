#!/bin/bash

# --single-node \
gcloud beta dataproc clusters create \
      etl-cluster-literature-mk-32-gpu-1 \
      --image-version=2.0-debian10 \
      --region=europe-west1 \
      --single-node \
      --zone=europe-west1-d \
      --master-machine-type=n1-highmem-32 \
      --master-boot-disk-size=2000 \
      --project=open-targets-eu-dev \
      --initialization-action-timeout=30m \
      --master-accelerator type=nvidia-tesla-t4 \
      --initialization-actions gs://goog-dataproc-initialization-actions-europe-west1/gpu/install_gpu_driver.sh \
      --metadata install-gpu-agent=true \
      --scopes 'https://www.googleapis.com/auth/cloud-platform'

gcloud beta dataproc jobs submit spark \
   --cluster=etl-cluster-literature-mk-32-gpu-1 \
   --project=open-targets-eu-dev \
   --region=europe-west1 \
   --async \
   --files=gs://ot-snapshots/literature/20210920/application-embedding.conf \
   --properties=spark.executor.extraJavaOptions=-Dconfig.file=application-embedding.conf,spark.driver.extraJavaOptions=-Dconfig.file=application-embedding.conf \
   --jar=gs://ot-snapshots/literature/20210920/io-opentargets-etl-literature-assembly-1.11.jar
