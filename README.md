[![Build Status](https://travis-ci.com/opentargets/platform-etl-literature.svg?branch=main)](https://travis-ci.com/opentargets/platform-etl-liturature)
# Open Targets ETL Literature

The aim of this application is to replace and to enhance the LINK project. 
**LI**terature co**N**cept **K**nowledgebase original project is available [here](https://github.com/opentargets/library-beam)
 
Currently, the application can executes 3 steps 
List of available steps:

* Grounding
* Processing
* Embedding

### Requirements

* OpenJDK 1.8
* scala 2.12.x (through SDKMAN is simple)
* Apache Spark 3.0.1
* Opentargets entities 
* EPMC entities

#### Obtain Opentarget entities 

This are the files generated from the `platform-etl-backend` search step. For a specific release they will usually be
found in `gs://ot-snapshots/etl/outputs/<release>/search/**/*.json`

#### Obtain EPMC dataset
TODO


### Grounding step
The grounding step extracts a common ground from EPMC entities and  the three main entities of Opentarget (Disease, Target, Drug). 

After a first phase of shaping of the data/entity types and the normalization of the texts, the next step is to join the two dataset in order to create whole comprehensive dataset with common information.

The final dataset contains the mapped and unmapped information and the co-corrences between information.

The input section needs two datasets. The `ot-luts` section contains the LookUp Table with the Opentarget info. 
The dataset used for this specific project is the "search" step of [ETL project](https://github.com/opentargets/platform-etl-backend). 
The `epmc` section contains the EPMC literature dataset.
Finally the section `outputs` contains the grounding output path.

```
  grounding {
   ot-luts {
     format = "json"
     path = "gs://open-targets-data-releases/21.02/output/ETL/search/**/*.json"
   }
   epmc {
     format = "json"
     path = "gs://otar-epmc/literature-files/**/*.jsonl"
   }
   outputs = {
     grounding {
       format = ${common.output-format}
       path = ${common.output}"/grounding"
     }
   }
 }
```

### Processing step
The processing step split the "grounding" output in two different datasets:
* Co-occorrence
* Matches 

The co-occorence dataset will be use by our data team to generate new features for our platform.

The input section requires the output of the `grounding step`.
The section `outputs` contains path for co-occurance dataset and matches dataset.

```
analysis {
  grounding {
    format = ${common.output-format}
    path = ${common.output}"/grounding/"
  }
  outputs = {
    cooccurrences {
      format = ${common.output-format}
      path = ${common.output}"/analysis/cooccurrences"
    }
    matches {
      format = ${common.output-format}
      path = ${common.output}"/analysis/matches"
    }
  }
}
```

### Embedding step
TO DO

### Create a fat JAR

Simply run the following command:

```bash
sbt assembly
```
The jar will be generated under _target/scala-2.12.10/_

### Configuration

The base configuration is found under `src/main/resources/reference.conf`. If you want to use specific configurations
for a Spark job see [below](#load-with-custom-configuration). 

#### Inputs
The inputs information are described under 
* Obtain Opentarget entities
* Obtain EPMC entities

#### Outputs

```
output dir:
    grounding
    matches
    cooccurrences
    word2vec
    word2vecSynonym
    xxyy
```

`xxyy` is used in the Open Targets front end via Elasticsearch.

### Running

#### Dataproc

##### Create cluster and launch

Here how to create a cluster using `gcloud` tool

The current image version is `preview-debian10` because is the only image that supports Spark3.

```sh
gcloud beta dataproc clusters create \
    etl-cluster \
    --image-version=preview-debian10 \
    --properties=yarn:yarn.nodemanager.vmem-check-enabled=false,spark:spark.debug.maxToStringFields=1024,spark:spark.master=yarn \
    --master-machine-type=n1-highmem-16 \
    --master-boot-disk-size=500 \
    --num-secondary-workers=0 \
    --worker-machine-type=n1-standard-16 \
    --num-workers=2 \
    --worker-boot-disk-size=500 \
    --zone=europe-west1-d \
    --project=open-targets-eu-dev \
    --region=europe-west1 \
    --initialization-action-timeout=20m \
    --max-idle=30m
```

##### Submitting a job to existing cluster

And to submit the job with either a local jar or from a GCS Bucket (gs://...)

```sh
gcloud dataproc jobs submit spark \
           --cluster=etl-cluster \
           --project=open-targets-eu-dev \
           --region=europe-west1 \
           --async \
           --jar=gs://ot-snapshots/...
```

#### Load with custom configuration

Add to your run either commandline or sbt task Intellij IDEA `-Dconfig.file=application.conf` and it
will load the configuration from your `./` path or project root. Missing fields will be resolved
with `reference.conf`.

The same happens with logback configuration. You can add `-Dlogback.configurationFile=application.xml` and
have a logback.xml hanging on your project root or run path. An example log configurationfile

```xml
<configuration>

    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%level %logger{15} - %message%n%xException{10}</pattern>
        </encoder>
    </appender>

    <root level="WARN">
        <appender-ref ref="STDOUT" />
    </root>

    <logger name="io.opentargets.etl.literature" level="DEBUG"/>
    <logger name="org.apache.spark" level="WARN"/>

</configuration>
```
If you are using the Dataproc cluster you need to add some additional arguments specifying
where the configuration can be found. 

```sh
gcloud dataproc jobs submit spark \
           --cluster=etl-cluster \
           --project=open-targets-eu-dev \
           --region=europe-west1 \
           --async \
           --files=application.conf \
           --properties=spark.executor.extraJavaOptions=-Dconfig.file=job.conf,spark.driver.extraJavaOptions=-Dconfig.file=application.conf \
           --jar=gs://ot-snapshots/...
```
where `application.conf` is a subset of `reference.conf`

```hocon
common {
  output = "gs://ot-snapshots/etl-literature/prod-latest"
}
```

#### Spark-submit

The fat jar can be executed on a local installation of Spark using `spark-submit`:

```shell script
/usr/lib/spark/bin/spark-submit --class io.opentargets.etl.literature.Main \
--driver-memory $(free -g | awk '{print $7}')g \
--master local[*] \
<jar> --arg1 ... --arg2 ...
```

# Creating a new release

1. Add tag to master so we can recreate the jar
```
git tag -a <release> -m "Release <release>"
git push origin <release>
```
Where release is something like 20.11.0 (year, month, iteration). Hopefully we don't need the iteration. 

2. Create jar and push to cloud storage
```
sbt assembly

gsutil cp target/scala-2.12/<jar> gs://open-targets-data-releases/<release>/platform-etl-literature/<jar>
```
3. Generate input files and push to cloud
4. Create updated configuration file and push to cloud
  - Save file in same place as jar so it can be re-run if necessary.
5. Run the steps
6. Create Elasticseach index (script in `platform-backend-etl` repository)
  - The relevant output file is `xxyy`

# Versioning

| Version | Date | Notes |
| --- | --- | --- |
| 1.0.0 | March 2021 | Initial release | 

# Copyright
Copyright 2014-2018 Biogen, Celgene Corporation, EMBL - European Bioinformatics Institute, GlaxoSmithKline, Takeda Pharmaceutical Company and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.