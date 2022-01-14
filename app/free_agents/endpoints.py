from datetime import datetime, timedelta, timezone
import json

import boto3
import botocore.exceptions
import requests

from fastapi import APIRouter


router = APIRouter()


@router.get("/json")
async def free_agents_json():

    s3 = boto3.resource('s3')
    s3_file = s3.Object('nate-api', 'sleeper-players.json')

    try:
       last_modified = s3_file.last_modified
    except botocore.exceptions.ClientError:
        # Presumably file does not exist
        last_modified = None

    if (
        last_modified is None or
        last_modified <= (datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(hours=1))
    ):
        response = requests.get('https://api.sleeper.app/v1/players/nfl')
        players = response.json()
        s3_file.put(Body=(bytes(json.dumps(players).encode('utf-8'))))
    else:
        response = s3_file.get()
        players = response['Body'].read()

    return {"message": players}

