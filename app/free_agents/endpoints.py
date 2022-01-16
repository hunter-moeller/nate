import io
import csv
import json
from datetime import datetime, timedelta, timezone

import boto3
import botocore.exceptions
import requests

from fastapi import APIRouter
from fastapi.responses import StreamingResponse


router = APIRouter()


def free_agents():
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

    allowed_positions = {
        "K",
        "QB",
        "RB",
        "TE",
        "WR",
    }

    free_agents = list()
    for player_id, player in all_players.items():
        if player_id not in rostered_ids:
            search_rank = player.get("search_rank")
            if search_rank is not None and search_rank < 1000:
                if player.get("position") in allowed_positions:
                    free_agents.append(player)

    return free_agents


@router.get("/json")
async def free_agents_json():
    return free_agents()


@router.get("/download")
async def free_agents_download():
    encoding = 'utf-8'

    writer_file = io.StringIO()
    fields = [
        "position",
        "search_rank",
        "status",
        "active",
        "first_name",
        "last_name",
        "depth_chart_order",
        "age",
        "years_exp",
    ]
    writer = csv.DictWriter(writer_file, fieldnames=fields, dialect='excel')
    writer.writeheader()
    rows = list()
    for fa in free_agents():
        row = dict()
        eligible = True
        for field in fields:
            if field in fa:
                row[field] = fa[field]
            else:
                eligible = False
        if eligible:
            rows.append(row)

    rows = sorted(rows, key=lambda x: [x["position"], x["search_rank"]])
    for row in rows:
        writer.writerow(row)
    content = writer_file.getvalue()
    content = content.encode(encoding)

    headers = {
        'Content-Disposition': 'attachment; filename="players.csv"',
        'Content-Type': 'text/csv',
    }

    async def stream():
        yield content

    return StreamingResponse(stream(), headers=headers)
