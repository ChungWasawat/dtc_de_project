# Data Engineering Zoomcamp 2023 Project

## Project Description:
We have faced the Covid-19 pandamic for a while and everyone had to stay at home, this created the new normal "work from home".    
Even though, in many countries, Covid-19 restrictions have lift because most people have vaccinated and the number of patients decreased, some businesses could not restore theirselves to the state they were before the pandemic happened.      


### Objective 


## Data Sources:
This project use two datasets: Citi Bike Trip Histories [Bike dataset](https://citibikenyc.com/system-data) and TLC Trip Record Data [Taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).   
Both datasets are recorded in New York city.
   


## Technologies Used:    
    * Python
    * Terraform
    * Google Cloud Storage
    * Bigquery
    * Prefect
    * DBT
    * Looker Studio

## Data Pipeline:
![data workflow](https://github.com/ChungWasawat/dtc_de_project/blob/main/assets/asset1.jpg "Data Pipeline")   
    1. use Terraform to create the resources (GCS, BQ)      [setup](https://github.com/ChungWasawat/dtc_de_project/blob/main/terraform/README.md)   
    2. use Prefect and Python to create etl files to upload data to GCS and BQ      [setup](https://github.com/ChungWasawat/dtc_de_project/blob/main/prefect/README.md)   
    3. use DBT to transform data        [setup](https://github.com/ChungWasawat/dtc_de_project/blob/main/dbt/README.md)   
    4. use Looker Studio to create dashboards from transformed data   

## Dashboard:

[Bike and Taxi Usages in New York from 2019 to 2022](https://lookerstudio.google.com/reporting/472c1e2a-cd34-4eb7-b654-6029288189a0)  




