from flask import Flask
from google.cloud import bigquery
import pandas as pd
import os
from google.cloud import tasks_v2
import datetime
import json
from flask import Response


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/C Ramasamy/PycharmProjects/sandbox/auth.json"

app = Flask(__name__)

client = tasks_v2.CloudTasksClient()
project = 'cloud-tasks-14'
queue = 'table-load'
location = 'us-central1'
payload = 'bas'


def create_task(project='cloud-tasks-14', queue='table-load', location='us-central1', payload='bas', in_seconds=None):
    parent = client.queue_path(project, location, queue)

    # Construct the request body.
    task = {
        'app_engine_http_request': {  # Specify the type of request.
            'http_method': tasks_v2.HttpMethod.POST,
            'relative_uri': '/load_table',
        }
    }
    if payload is not None:
        if isinstance(payload, dict):
            # Convert dict to JSON string
            payload = json.dumps(payload)
            # specify http content-type to application/json
            task["app_engine_http_request"]["headers"] = {"Content-type": "application/json"}
        # The API expects a payload of type bytes.
        converted_payload = payload.encode()

        # Add the payload to the request.
        task['app_engine_http_request']['body'] = converted_payload

    if in_seconds is not None:
        # Convert "seconds from now" into an rfc3339 datetime string.
        timestamp = datetime.datetime.utcnow() + datetime.timedelta(seconds=in_seconds)

        # Add the timestamp to the tasks.
        task['schedule_time'] = timestamp

    # Use the client to build and send the task.
    response = client.create_task(parent=parent, task=task)

    print('Created task {}'.format(response.name))
    print(response, 'response')

    return response


@app.route("/", methods=['GET'])
def home():
    return Response("at home", status=200, mimetype='text/plain')

@app.route("/trigger", methods=['GET'])
def trigger():
    create_task()
    return Response("Success", status=202, mimetype='text/plain')

@app.route("/load_table", methods=['POST'])
def cloud_taks():
    print('calling cloud tasks')

    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/C Ramasamy/PycharmProjects/sandbox/auth.json"
    client = bigquery.Client()
    df = client.query('select * from `cloud-tasks-14.mytables.insurance`').to_dataframe()
    # print(df.head())
    print('head mire')
    table_id = 'cloud-tasks-14.mytables.insurance_morecopy'
    print(df.head())
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("statecode", "STRING"),
    ])
    job_config.write_disposition = 'WRITE_TRUNCATE'

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    print(job.result())
    print(table_id, 'should be loaded')

    return Response("Success", status=201, mimetype='text/plain')

if __name__ == '__main__':
    app.run()
