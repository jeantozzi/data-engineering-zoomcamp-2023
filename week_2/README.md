## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 

Quick Reference:

- [Environment Setup](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_2#environment-setup)
- [Question 1 - Load January 2020 data](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_2#question-1-load-january-2020-data)
- [Question 2 - Scheduling with Cron](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_2#question-2-scheduling-with-cron)
- [Question 3 - Loading data to BigQuery](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_2#question-3-loading-data-to-bigquery)
- [Question 4 - Github Storage Block](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_2#question-4-github-storage-block)
- [Question 5 - Email or Slack notifications](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_2#question-5-email-or-slack-notifications)
- [Question 6 - Secrets](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_2#question-6-secrets)

## Environment Setup

### Prefect, Virtual Environment and dependencies

For this exercise, we'll be using a virtual environment to install all dependencies and Prefect, so we can execute the pipelines for each question.

First, let's set up a virtual environment with `conda create -n zoomcamp-de-week-2 python=3.9`. (If you don't have conda installed, please check https://www.anaconda.com/products/distribution for mode details)

Next, we'll activate the environment we created with `conda activate zoomcamp-de-week-2` and install all the dependencies listed in `requirements.txt` running `pip install -r requirements.txt`.

You can run `prefect orion start` to access the webserver and check infos about flows and tasks.

Prefect and all the required dependencies are installed. Local configurations-wise, we're good to go.

### GCP

As for GCP configuration, just follow the steps below: 

- Log in to [GCP](https://console.cloud.google.com/)
- Create a Project
- Set up Cloud Storage
- Set up BigQuery
- Create a service account with the required policies to interact with both services

Ok, now we're good to go. I promise. 😂

## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770
* 766,792
* 299,234
* 822,132

### Solution

For this question, a GCS Block is required for Prefect to establish the connection.

Edit `make_gcp_blocks.py` file inside `/blocks` folder and insert your own credentials.

After that, you can run `python blocks/make_gcp_blocks.py`, so that the block for GCS is created. You can check them using the Orion UI.

As for the script itself, you can check [/flows/question_1.py](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/blob/main/week_2/flows/question_1.py) for more detailed information.

The important part is to print the DataFrame length anywhere in the code, for example:

```
@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f'data/{color}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    print(f'Writing {len(df):,} rows into {dataset_file}.parquet')
    return path
```

This will output something like this:

```
01:21:47.024 | INFO    | Flow run 'huge-mantis' - Created task run 'write_local-f322d1be-0' for task 'write_local'
01:21:47.025 | INFO    | Flow run 'huge-mantis' - Executing 'write_local-f322d1be-0' immediately...
01:21:48.039 | INFO    | Task run 'write_local-f322d1be-0' - Writing 447,770 rows into green_tripdata_2020-01.parquet
01:21:48.131 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
```

Answer: `447,770`

## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *`
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`

### Solution

For that, you can check https://crontab.guru/ to access detailed information about cron notation and build your own.

As a quick reference, the cron notation is composed of 5 differente parts (from left to right):
- minute
- hour
- day (of the month)
- month
- day (of the week)

Hence, for this particular question (first of every month at 5am UTC), we'll have this cron notation:
```
0 5 1 * *
```

To create, apply and run the deployment in question, we'll have the commands bellow:
```
prefect deployment build flows/question_1.py:etl_web_to_gcs -n question_01 -q default --cron "0 5 1 * *" --apply
prefect agen start -q "default"
```

Answer: `0 5 1 * *`

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920
- 12,282,990
- 27,235,753
- 11,338,483

### Solution

We'll run `prefect deployment build flows/question_3.py:etl_parent_flow -n question_03 -q default --apply` to create and apply the deployment.

Next, it's necessary to have a agent running (`prefect agent start -q default`), so we can run the flow using the parameters listed in the question with the command below:

`prefect deployment run etl-parent-flow/question_03 -p color="yellow" -p year=2019 -p months=[2,3]`

As for the script itself, you can check [/flows/question_3.py](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/blob/main/week_2/flows/question_3.py) for more detailed information.

Setting a incrementary variable to keep track of the number of rows uploaded, so we can get the output for the answer:

```
@flow(log_prints=True)
def etl_gcs_to_bq(color: str, year: int, month: int) -> int:
    """"Main ETL flow to load data into BigQuery"""
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    write_bq(df)
    number_of_rows = len(df)
    del(df) # to free memory
    return number_of_rows

@flow(log_prints=True)
def etl_parent_flow(color: str, year: int, months: list[int]) -> None:
    total_rows_to_bq = 0
    for month in months:
        etl_web_to_gcs(color, year, month)
        total_rows_to_bq += etl_gcs_to_bq(color, year, month)
    print(f'Number of rows inserted into BigQuery: {total_rows_to_bq}')

if __name__ == '__main__':
    etl_parent_flow()
```

This will output something like this:

```
04:02:03.082 | INFO    | Task run 'write_bq-f3b17cf5-0' - Finished in state Completed()
04:02:03.175 | INFO    | Flow run 'malachite-lorikeet' - Finished in state Completed()
04:02:03.175 | INFO    | Flow run 'ingenious-koala' - Number of rows inserted into BigQuery: 14,851,920
04:02:03.239 | INFO    | Flow run 'ingenious-koala' - Finished in state Completed('All states completed.')
04:02:05.141 | INFO    | prefect.infrastructure.process - Process 'ingenious-koala' exited cleanly.
```

Answer: `14,851,920`

## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605
- 190,225

### Solution

Edit `make_github_block.py` file inside `/blocks` folder and insert your own repository link.

After that, you can run `python blocks/make_github_block.py`, so that the block for GCS is created. You can check them using the Orion UI.

To create the deployment and apply it, we'll run `prefect deployment build week_2/flows/question_4.py:etl_web_to_gcs  -n github-deploy  -sb github/github  --path week_2/flows/  --apply` from the repository root folder.

Next, it's necessary to have a agent running (`prefect agent start -q default`), so we can run the flow using the parameters listed in the question with the command below:

`prefect deployment run 'etl-web-to-gcs/github-deploy' -p color='green' -p year=2020 -p month=11`

As for the script itself, you can check [/flows/question_4.py](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/blob/main/week_2/flows/question_4.py) for more detailed information.

This will output something like this:

```
16:14:39.620 | INFO    | Flow run 'neat-carp' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
16:14:39.621 | INFO    | Flow run 'neat-carp' - Executing 'write_local-09e9d2b8-0' immediately...
16:14:39.681 | INFO    | Task run 'write_local-09e9d2b8-0' - Creating path...
16:14:39.682 | INFO    | Task run 'write_local-09e9d2b8-0' - Path data/green created.
16:14:39.912 | INFO    | Task run 'write_local-09e9d2b8-0' - Writing 88,605 rows into green_tripdata_2020-11.parquet
16:14:39.946 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
```

Answer: `88,605`

## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392`

### Solution

For Notifications, run `prefect orion start` and get into the UI.

On the Prefect side, do as the images below:

![notification_1](./images/notifications_1.png)

![notification_2](./images/notifications_2.png)

On the Slack side, you'll only need to follow the instructions from [this link](https://api.slack.com/messaging/webhooks).

We'll be running the `question_4.py` script for this question as well, only changing the inputs for different data.

As for pipeline, run the following commands:
```
prefect deployment build flows/question_4.py:etl_web_to_gcs -n question-5 -q default --apply

prefect agent start -q default # in another terminal

prefect deployment run 'etl-web-to-gcs/question-5' -p color='green' -p year=2019 -p month=4

```
This will output something like this:

```
00:21:52.672 | INFO    | Flow run 'melodic-puma' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
00:21:52.673 | INFO    | Flow run 'melodic-puma' - Executing 'write_local-09e9d2b8-0' immediately...
00:21:52.725 | INFO    | Task run 'write_local-09e9d2b8-0' - Creating path...
00:21:52.726 | INFO    | Task run 'write_local-09e9d2b8-0' - Path data/green created.
00:21:54.056 | INFO    | Task run 'write_local-09e9d2b8-0' - Writing 514,392 rows into green_tripdata_2019-04.parquet
00:21:54.092 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
```

And the Notification:

![notification_3](./images/notifications_3.png)

Answer: `514,392`

## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 5
- 6
- 8
- 10

### Solution

For this question, do as follows:

![secret_1](./images/secret_1.png)

