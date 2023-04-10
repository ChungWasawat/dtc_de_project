In the dbt part, use files in macros, seeds, models, and yml files: dbt_project.yml, packages.yml, schema.yml 

and use the following commands to run the models:
- dbt deps      : to pulls the most recent version of the dependencies listed in your packages.yml from git
- dbt seed      : to load csv files located in the seed-paths directory of your dbt project into your data warehouse
- dbt run       : to executes compiled sql model files against the current target database
- dbt test      : to runs tests defined on models, sources, snapshots, and seeds (expect to build models before)

## The lineage
![dbt lineage](https://github.com/ChungWasawat/dtc_de_project/blob/main/assets/asset2.jpg "DBT Lineage of the project")
