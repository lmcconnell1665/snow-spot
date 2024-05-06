import snowflake.connector
import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime, timedelta

load_dotenv()  # take environment variables from .env.

def get_snowflake_data():
    """Runs the query.sql statement and returns the result."""

    con = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )
    cursor = con.cursor()

    with open('query.sql', 'r') as file:
        query = file.read()

    result = cursor.execute(query).fetchall()

    # (datetime.datetime(2024, 5, 7, 13, 20, 5, 52000, tzinfo=<DstTzInfo 'America/Los_Angeles' PDT-1 day, 17:00:00 DST>),
    # 'Send a scorecard for the 43 roles open with Hain', 
    # 144873860, 
    # 'Send Customer Scorecard', 
    # 'NOT_STARTED', 
    # 'MEDIUM', 
    # 'EMAIL', 
    # 9765433865, 
    # 'Hain', 
    # 43, 
    # 'Landon Cortenbach')

    print(result)
    return result

def create_hubspot_task(row):
    """Creates a task in HubSpot."""

    due_date = row[0] + timedelta(days=7)

    url = 'https://api.hubapi.com/crm/v3/objects/tasks'
    headers = {
        'Authorization': f'Bearer {os.getenv("HUBSPOT_API_KEY")}'
    }
    body = {
        "properties": {
            "hs_timestamp": row[0].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            "hs_task_reminders" : int(due_date.timestamp()),
            "hs_task_body": row[1],
            "hubspot_owner_id": row[2],
            "hs_task_subject": "Send Customer Scorecard",
            "hs_task_status": "NOT_STARTED",
            "hs_task_priority": "MEDIUM",
            "hs_task_type": "EMAIL"
        },
        "associations": [
            {
                "to": {
                    "id": row[7]
                },
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 192
                    }
                ]
            }
        ]
    }
    print(json.dumps(body, indent=4))

    response = requests.post(url, headers=headers, json=body)

    if response.ok:
        print(f"Task created for {row[9]}")
    else:
        print(response.text)


if __name__ == "__main__":
    result = get_snowflake_data()
    for row in result:
        create_hubspot_task(row)