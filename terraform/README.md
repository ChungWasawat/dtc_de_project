# Terraform setup
1. install terraform on local machine
2. create two files: main.tf and variables.tf in terraform folder
3. In variables.tf, it needs to create variables like below.
    * data_lake_bucket
        * type: local
    * project
        * type: variable
        * description: Your GCP project ID
    * region
        * type: variable
        * description: Region for GCP resources
    * storage_class
        * type: variable
        * description: Storage class type for your bucket
    * BQ_DATASET
        * type: variable
        * description: BigQuery Dataset that raw data (from GCS) will be written to
    * credentials
        * type: variable
        * description: Local path to your GCP credentials file (a JSON file)
4. use these commands 
    * ```terraform init``` : Initialize state file
    * ```terraform plan``` : Check changes to new infra plan
    * ```terraform apply``` : Create new infra
    * ```terraform destroy``` : Delete infra after your work

