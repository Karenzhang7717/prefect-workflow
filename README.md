
# Prefect-workflow

# What is a Dataflow?

A data flow represents the movement of data from one component or system to another. The data flow may also be described as the transport of data from a source to a destination. An ETL (extract, transform, load) process is a type of data flow.

# What is a Workflow Orchestration?

The workflow orchestration tool turns code into a workflow that can be scheduled, run and observed. A good workflow orchestration tool provides functionality such as data passing between tasks, triggering parametrized runs, scheduling, failure alerting, retrying and recovering from failures, caching to avoid expensive recomputation, and visibility for monitoring workflow execution and failures.

If we take the delivery service system as an example:

-   Each order is a workflow and each delivery is a workflow run.
-   The tasks inside the workflow can be thought of as boxes and the functions within them can be added easily with Python decorators.
-   The different addresses for delivery can be thought of as different data warehouses or databases.
-   The order of execution for tasks in the workflow can be configured. The orchestration service takes care of executing the workflow and ensuring the desired delivery type, with proper visibility and execution logs.

![](https://miro.medium.com/max/1400/1*0-DVQ-D7Me55oMTMlcGUHA.png)

# Introduction to Prefect

Prefect is a modern workflow orchestration tool that helps with automating and managing complex workflows. You can monitor, coordinate, and orchestrate dataflows between and across your applications, build pipelines, deploy them anywhere, and configure them remotely. A “Flow” in Prefect refers to a directed acyclic graph (DAG) of tasks that represent the steps of a workflow. A “Task” in Prefect is a unit of work that performs a specific action and can be combined with other tasks to create a complete workflow.

# Transforming data ingestion into a Prefect flow

Let’s take a look at a normal Python data ingestion script here. It ingests data into Postgres database:

#!/usr/bin/env python  
# coding: utf-8  
import os  
import argparse  
from time import time  
import pandas as pd  
from sqlalchemy import create_engine  
  
  
def ingest_data(user, password, host, port, db, table_name, url):  
    # the backup files are gzipped, and it's important to keep the correct extension  
    # for pandas to be able to open the file  
    if url.endswith('.csv.gz'):  
        csv_name = 'yellow_tripdata_2021-01.csv.gz'  
    else:  
        csv_name = 'output.csv'  
  
    os.system(f"wget {url} -O {csv_name}")  
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'  
    engine = create_engine(postgres_url)  
  
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)  
  
    df = next(df_iter)  
  
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)  
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)  
  
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')  
  
    df.to_sql(name=table_name, con=engine, if_exists='append')  
  
    while True:  
  
        try:  
            t_start = time()  
  
            df = next(df_iter)  
  
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)  
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)  
  
            df.to_sql(name=table_name, con=engine, if_exists='append')  
  
            t_end = time()  
  
            print('inserted another chunk, took %.3f second' % (t_end - t_start))  
  
        except StopIteration:  
            print("Finished ingesting data into the postgres database")  
            break  
  
  
if __name__ == '__main__':  
    user = "postgres"  
    password = "admin"  
    host = "localhost"  
    port = "5433"  
    db = "ny_taxi"  
    table_name = "yellow_taxi_trips"  
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"  
  
    ingest_data(user, password, host, port, db, table_name, csv_url)

Let’s transform this into a Prefect flow.

-   We breakdown the whole ingest_data() function into tasks:

@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))  
def extract_data(url: str):  
   .....  
  
@task(log_prints=True)  
def transform_data(df):  
    .....  
  
@task(log_prints=True, retries=3)  
def load_data(table_name, df):  
    .....  

-   Then we put the tasks in the main flow function. We use the  `flow`  decorator to indicate that  `main_flow()`  is a Prefect flow

@flow(name="Subflow", log_prints=True)  
def log_subflow(table_name: str):  
    print(f"Logging Subflow for: {table_name}")  
  
@flow(name="Ingest Data")  
def main_flow(table_name: str = "yellow_taxi_trips"):  
    user = "postgres"  
    password = "admin"  
    host = "localhost"  
    port = "5433"  
    db = "ny_taxi"  
    table_name = "yellow_taxi_trips"  
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"  
    log_subflow(table_name)  
    raw_data = extract_data(csv_url)  
    data = transform_data(raw_data)  
    load_data(table_name, data)

You can view the code  [here](https://github.com/Karenzhang7717/prefect-workflow/blob/main/ingest_data_flow.py).

Now let’s try to ingest the data using the updated flow. Run:

`python ingest_data_flow.py`

The output in your terminal should be something like this:

![](https://miro.medium.com/max/1400/1*GR6OsyO60j2970iRTLsKBw.png)

Now we can start Orion UI with the command  `prefect orion start`. Then we can see the UI dashboard at  [http://127.0.0.1:4200](http://127.0.0.1:4200/)

# Create a new block for PostgreSQL connector

In Prefect, a block is a fundamental building block of a task flow. It defines the behaviour of a unit of work in the flow and specifies its dependencies on other blocks. Now let’s create a new connector block for PostgreSQL. In the Prefect Orion UI, we click “Blocks” followed by “Add Block +.” Next, we add a SQLAlchemyConnector by filling out the following form and clicking “Create.” We will name it as  `postgres-connector`  and choose SyncDriver as the Driver.

![](https://miro.medium.com/max/1400/0*ovzKdNxIJpE0195Y.png)

Now we need to copy the code snippet to the flow: (full code  [here](https://github.com/Karenzhang7717/prefect-workflow/blob/main/ingest_data_flow_etl_with_sql_block.py))

from prefect_sqlalchemy import SqlAlchemyConnector  
  
with SqlAlchemyConnector.load("postgres-connector") as database_block:  
    ...

# ETL with Prefect and GCP

Now let’s try to ingest data using Prefect to GCS.

-   First we need to create a GCP Bucket Block. For my case I named my bucket as  `de-gcs`.
-   Then we need to create another block for GCP Credentials. Remember to link your service account file to the JSON keyfile you had.
-   After setting up the Block for GCP we would be able to run  `etl_web_to_gcs.py`  (see the script  [here](https://github.com/Karenzhang7717/prefect-workflow/blob/main/etl_web_to_gcs.py))  
    `python etl_web_to_gcs.py`

You should be able to see your data loaded to GCS here.

![](https://miro.medium.com/max/1400/1*actu3Pq0LaK9Xx5VdoMjfQ.png)

# GCS to BigQuery

Now we will implement ETL script to extract data from GCS and load them into BigQuery. We need to add two tasks,  `extract_From_gcs`  and  `write_bq`. See the full script  [here](https://github.com/Karenzhang7717/prefect-workflow/blob/main/etl_gcs_to_bq.py).

@task(retries=3)  
def extract_from_gcs(color: str, year: int, month: int) -> Path:  
    """Download trip data from GCS"""  
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'  
    gcs_block = GcsBucket.load('de-gcs')  
    gcs_block.get_directory(from_path=gcs_path, local_path='./')  
    return Path(gcs_path)  
  
@task()  
def write_bq(df: pd.DataFrame) -> None:  
    """Write DataFrame to BigQuery"""  
    gcp_credentials_block = GcpCredentials.load('gcp-credentials')  
    df.to_gbq(  
        destination_table='trips_data_all.yellow_taxi_trips',  
        project_id='crafty-elf-376416',  
        credentials=gcp_credentials_block.get_credentials_from_service_account(),  
        chunksize=500_000,  
        if_exists='append'  
    )

Next we need to create a table in BigQuery.

![](https://miro.medium.com/max/1400/1*r2PCatX-p2z8mwXff073wQ.png)

Now let’s try to run our ETL script! Before that we need to delete all of the data populated by BigQuery.

![](https://miro.medium.com/max/1400/1*dGu9ZZlZcUvaXdrvoK9kiA.png)

![](https://miro.medium.com/max/1352/0*kddhrl3YATqG13u3.png)

Now run:

`python etl_gcs_to_bq.py`

You will see our records in table preview:

![](https://miro.medium.com/max/1400/1*IzQJPwGDtz56lBevget8yQ.png)

# Parametrizing Flow and Deployments

There can be more than one run of a flow, each with different parameters that change the result. So, we reimplement  [etl_web_to_gcs.py](https://github.com/Karenzhang7717/prefect-workflow/blob/main/etl_web_to_gcs.py)  so that we can use the same flow to update different taxi trips datasets on GCP. See  [parameterized_flow.py](https://github.com/Karenzhang7717/prefect-workflow/blob/main/parameterized_flow.py)  for more information. When this new code is run, the results can be seen in the GCS Bucket.

![](https://miro.medium.com/max/1400/1*z8NLy7H0f89bw6C-9sM4wA.png)

In Prefect UI we will see:

![](https://miro.medium.com/max/1400/1*wAW8UMNyCSPclNTQt7MxrA.png)

We won’t have to manually start our workflow if we use Prefect to deploy it. In the terminal, we run the below command, which makes a YAML file with the deployment metadata for the workflow.

`prefect deployment build ./parameterized_flow.py:etl_parent_flow -n “Parameterized ETL”`

We find the line in the metadata YAML file that says “parameters: “ and add the parameters we want to run.

`parameters : { “color”: “yellow”, “months”: [1, 2, 3], “year”: 2021 }`

And next, we create the deployment on the API:

`prefect deployment apply etl_parent_flow-deployment.yaml`

We want to trigger a quick run from the UI: click Quick Run.

![](https://miro.medium.com/max/1400/1*NG3GePt5EmCuTpmU8552Dg.png)

After you start a quick run, it will show up under “Flow runs” as “Scheduled.” We need an agent to execute this workflow. Run:

`prefect agent start — work-queue “default”`

# Schedule Flows

Let’s say we want our workflow to run once every hour. In Orion UI, we can do this by clicking on our deployment and then clicking “Add” under “Schedule.”

![](https://miro.medium.com/max/1400/1*JA1SavrTsNbNUQliLdAf_Q.png)

After we set up the scheduler the flows will be automatically run for the scheduled time.

![](https://miro.medium.com/max/1400/1*NehfVlklavjqF1qHWar5MQ.png)



