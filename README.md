# Data Engineering Zoomcamp 2023 Project
This repo is the final project of Data Engineering Zoomcamp course by Data Talks club.

## Project Description:
We have faced the Covid-19 pandamic for a while and everyone had to stay at home, this created the new normal "work from home".    
Even though, in many countries, Covid-19 restrictions have been lifted because most people have vaccinated and the number of patients decreased, some businesses could not restore themselves to the state they were before the pandemic happened or find new ways to build themselves.      

So I want to see how Covid-19 affects transportation services like taxi and bike in 2019 and how these services changes after the restrictions have been lifted in 2022.

### Objective 
To assess changes in demand for taxi and bike services, since the outbreak of COVID-19

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

![vehicle use during pandemic](https://github.com/ChungWasawat/dtc_de_project/blob/main/assets/asset3.jpg "Overview")  
![average duration (bike)](https://github.com/ChungWasawat/dtc_de_project/blob/main/assets/asset4.jpg "bike") 
![average duration (taxi)](https://github.com/ChungWasawat/dtc_de_project/blob/main/assets/asset5.jpg "taxi") 


### Special Thanks:
I would like to thank [DataTalks.Club](https://github.com/DataTalksClub/data-engineering-zoomcamp) for creating this course. 
