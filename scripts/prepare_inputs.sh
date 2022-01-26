#!/bin/bash

set -x
# Script to move the necessary inputs for literatures into their own directory.
# This isn't strictly necessary, but it reduces reproducibility issues.
# This will be resolved by opentargets/platform#1793.

release='22.02.1'
epmc='21.09.1'
lit_input="gs://open-targets-pre-data-releases/$release/input/literature/"
etl_output="gs://open-targets-pre-data-releases/$release/output/etl/parquet"

files=('molecule' 'targets' 'diseases')

for i in "${files[@]}"
do
  to_move="${etl_output}/$i"
  echo "Moving $to_move, please wait ..."
  gsutil -m cp -r $to_move $lit_input
done

# copy empc lookup table file
curl http://ftp.ebi.ac.uk/pub/databases/pmc/DOI/PMID_PMCID_DOI.csv.gz | gsutil cp - "${lit_input}PMID_PMCID_DOI.csv.gz"
# copy empc raw data
gsutil -m cp -r "gs://otar025-epmc/$epmc/**/" "$lit_input/epmc"