# GCP Dataflow Assignment Zenith
### This repository contains assignment code for gcp by zenith system solution

## Requirement
```text 
Build the data pipeline on Google Cloud Platform (GCP). 
Source code can be in java or python and procedure on setup GCP Dataflow.
```
Acceptance Criteria :
1. Create a BigQuery table with the fields the same as csv.
2. Manual upload the rh_201810.csv into GCP Bucket.
3. Create a java or python project (run in GCP Dataflow) to load the data from GCP Bucket into BigQuery.


## Table Schema
| Field Name     | Type  | Mode     |
| :------------- | :----------: | -----------: |
| TIMESTAMP | DATE  | NULLABLE |
| CUSTOMER  | STRING | NULLABLE |
| POINTNAME | STRING  | NULLABLE |
| VALUE  | FLOAT  | NULLABLE |


## Source Code
```text
1. create_gcp_table.py - It creates the table with the defined Schema
2. push_data_to_bucket.py - It send the file to the storage Bucket
```

Authentication
The python program set GOOGLE_APP_CREDENTIAL to content of `auth.json`. Please make sure you have the key placed at the root of the python program.

```text
3. Dataflow Pipeline - Java based Maven Project
The Java code is written in Apache Beam, which creates a pipeline and submits a Dataflow Job to the Dataflow for reading the CSV from cloud storage to the BigQuery. 
Pass the below command-line arguments to run the job on Datflow, if the arguments are not supplied, it will use java direct runner.
```


```sh
--project=<YOUR_PROJECT_ID> --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
```

The application assumes the GOOGLE_APP_CREDENTIAL is set in the classpath. If not already done, this can be done by running below command.

```sh
export GOOGLE_APPLICATION_CREDENTIALS=/Users/username/key.json
```
