[![Build Status](https://travis-ci.com/opentargets/platform-etl-literature.svg?branch=main)](https://travis-ci.com/opentargets/platform-etl-liturature)

# Open Targets ETL Literature

The aim of this application is to replace and to enhance the LINK project.
**LI**terature co**N**cept **K**nowledgebase original project is
available [here](https://github.com/opentargets/library-beam)

Currently, the application can execute 5 steps.

List of available steps:

* All : processing+embedding
* Processing
* Embedding
* Vectors
* Evidence

### Requirements

* OpenJDK 8
* scala 2.12.x (through SDKMAN is simple)
* Apache Spark 3.1.1
* Opentargets entities
* EPMC entities

## Inputs

The inputs can be staged using a helper script `prepare_inputs.sh` found in the `scripts` directory.

### Opentarget entities

This are the files generated from the `platform-etl-backend` search step. For a specific release they will usually be
found in `gs://ot-snapshots/etl/outputs/<release>/search/**/*.json`

### Obtain EPMC dataset

These files are provided by our collaborator EuropePMC. The files contains the list of the public and private
publications with many metadata. Moreover, EPMC extractes text and sentence using Machine Learning approach.

### EPMC `pmid -> `pmcid` lookup table

This file can be downloaded from `http://ftp.ebi.ac.uk/pub/databases/pmc/DOI/PMID_PMCID_DOI.csv.gz`

## Steps

### All step

The step _all_ runs the steps in the following order:

- _processing_
- _embedding_
- _vectors_
- _evidence_

Running the _all_ steps is faster than running each step individually, as it can resuse intermediate results. This
reduces the amount of Disk IO which is performed.

### Processing step

The processing step extracts a common ground from EPMC entities and the three main entities of Opentargets (Disease,
Target, Drug).

This step needs the datasets from the 3 main entities produced by the main
piepline [ETL project](https://github.com/opentargets/platform-etl-backend). The `epmc` section contains the EPMC
literature dataset. The section `epmcids` contains an index to resolve the pmid given a pmcid. The section `outputs`
contains path for co-occurance dataset and matches dataset.

The configuration is defined in the `processing` section of `reference.conf`.

After a first phase of shaping of the data/entity types and the normalization of the texts, the next step is to join the
two dataset in order to create a comprehensive dataset with common information. The step generates two different
datasets:

* Co-occurrence
* Matches

The co-occurence dataset is used by the Opentargets data team to generate new features for our platform. The matches
dataset is used by the _embedding_ step.

### Embedding step

The "embedding" step generates three datasets with different goals.

The input section requires the matches dataset generates by the "processing" step.

The configuration is specified in the `embedding` section of `reference.conf`

### Vectors step

Unknown...

### Evidence step

Unknown...

# Creating a new release

1. Get the necessary inputs by using the `prepare-inputs.sh` script, updating the variables as necessary. You should
   only need to update the `release` and `epmc` variables. Note: the ETL must have been run before the literature
   pipeline.
2. Create a configuration and 'publish' to cloud storage.

```bash
# helper scripts are stored here
cd scripts
# create a new configuration file
cp application-processing.conf my-conf.conf
# Update my-conf with paths to where the data is

# push to conf directory of release 
gsutil cp my-conf gs://open-targets-pre-data-releases/<release>/conf
```

3. Create a jar, start a cluster and job
    - A jar will be pushed automatically to GS storage named after the most recent git commit. If you want to know how
      the code looked at the time the jar was created. Make sure you do not have uncommited changes when you run the
      script!

```bash
# update variables in `deploy_and_run.sh`. You should only need to update `release` and `config`.
# Release updates the gs path, and config is the name of the file uploaded.
/bin/bash deploy_and_run.sh
```

#### Outputs

```
output dir:
    matches
    cooccurrences
    word2vec
    word2vecSynonym
    literature-etl
```

`literature-etl` is used in the Open Targets front end via Elasticsearch.

# Versioning

| Version | Date | Notes |
| --- | --- | --- |
| 1.0.0 | March 2021 | Initial release | 

# Copyright

Copyright 2014-2024 EMBL - European Bioinformatics Institute, Genentech, GSK, MSD, Pfizer, Sanofi and Wellcome Sanger Institute
Pharmaceutical Company and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please
see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "
AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.