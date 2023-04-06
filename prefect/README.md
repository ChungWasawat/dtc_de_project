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
6. can use prefect deployment to re-execute the codes easier
    ```prefect deployment build ./abc.py:etl_flow -n "deployment name"```   
    ```prefect deployment apply etl_flow-deployment.yaml```
7. after that, start work-queue agent
    ```prefect agent start --work-queue "default"```
