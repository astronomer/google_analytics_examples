# Google Analytics Example DAGs

This repository contains example code for DAGs interacting with Google Analytics, and the hooks and operators needed to power the DAGs.

### Requirements
The Astronomer CLI and Docker installed locally are needed to run all DAGs in this repo. Python requirements are found in the `requirements.txt` file.

An account and connection for Google Cloud and Snowflake are needed to run DAGs.

For Google Cloud, under `Admin -> Connections` in the Airflow UI, add a new connection with Conn ID as `google_cloud_default`. The connection type is `Google Cloud`. A GCP key associated with a service account that has access to BigQuery is needed; for more information generating a key, [follow the instructions in this guide](https://cloud.google.com/iam/docs/creating-managing-service-account-keys). The key can either be added via a path via the Keyfile Path field, or the JSON can be directly copied and pasted into the Keyfile JSON field. In the case of the Keyfile Path, a relative path is allowed, and if using Astronomer, the recommended path is under the `include/` directory, as Docker will mount all files and directories under it. Make sure the file name is included in the path. Finally, add the project ID to the Project ID field. No scopes should be needed.

For Snowflake, under `Admin -> Connections` in the Airflow UI, add a new connection with Conn ID as `snowflake_default`. The connection type is `Snowflake`. The host field should be the full URL that you use to log into Snowflake, for example `https://[account].[region].snowflakecomputing.com`. Fill out the `Login`, `Password`, `Schema`, `Account`, `Database`, `Region`, `Role`, and `Warehouse` fields with your information.


### Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:
1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart).
2. Clone this repo locally and navigate into it.
3. Start Airflow locally by running `astro dev start`.
4. Create all necessary connections and variables - see below for specific DAG cases.
5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there.
