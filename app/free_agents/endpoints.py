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
        print("Getting updated full player list from sleeper...")
        response = requests.get('https://api.sleeper.app/v1/players/nfl')
        all_players = response.json()
        s3_file.put(Body=(bytes(json.dumps(all_players).encode('utf-8'))))
    else:
        print("Getting cached full list of players...")
        response = s3_file.get()
        all_players = json.loads(response['Body'].read())
    print("Got full players")

    print("Getting rostered players...")
    response = requests.get('https://api.sleeper.app/v1/league/786679085437968384/rosters')
    rosters = response.json()
    print("Got rostered players")

    rostered_ids = list()
    for roster in rosters:
        rostered_ids.extend(roster["players"])
    rostered_ids = set(rostered_ids)

    free_agents = list()
    for player_id, player in all_players.items():
        if player_id not in rostered_ids:
            free_agents.append(player)

    print("Returning")
    return free_agents
