# Prefect setup
1. create a conda environment for the project and activate it
2. install the packages in requirements.txt
    ```pip install -r requirements.txt```
    - use ```prefect -v``` to check if Prefect is ready
3. initialize Prefect with this command
    ```prefect orion start```
4. enter to Prefect web UI on the local machine (default = http://127.0.0.1:4200) to create essential blocks for the project
    * GCP Credentials block : to gain access to GCP resources
    * GCS Bucket block : to gain access to a GCS bucket
5. execute Python file to make the data pipeline run 
    ```python abc.py```
6. 
    ```prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"```
