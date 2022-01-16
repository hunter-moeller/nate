import io
import csv
import json
import time
from datetime import datetime, timedelta, timezone

import boto3
import botocore.exceptions
import requests

from fastapi import APIRouter
from fastapi.responses import Response, StreamingResponse


router = APIRouter()


def player_rankings(position):
    year = 2021
    url = f"https://api.fantasypros.com/v2/json/nfl/%{year}/consensus-rankings?type=dynasty&scoring=PPR&position={position}&week=0&experts=available"
    response = requests.get(
        url,
        headers={
            'x-api-key': 'zjxN52G3lP4fORpHRftGI2mTU8cTwxVNvkjByM3j'
        }
    )
    rankings = response.json()["players"]
    return rankings


def all_player_rankings():
    rankings = list()

    start = time.time()
    rankings.extend(player_rankings('QB'))
    rankings.extend(player_rankings('RB'))
    rankings.extend(player_rankings('WR'))
    rankings.extend(player_rankings('TE'))
    took = time.time() - start
    print(f"Took {took} seconds to get rankings")
    return rankings


def parse_sleeper_name(player):
    return f"{player.get('first_name')}{player.get('last_name')}"


def simplify_name(name):
    # 'I' and 'V' from "the first" or "the fourth" are removed for higher match chance
    return ''.join(c for c in name if c.isalpha() and c != 'I' and c != 'V').lower()


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
    print("Got rostered players...")

    print('Determining rostered player name keys...')
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
    free_agent_names = set()
    for player_id, player in all_players.items():
        search_rank = player.get("search_rank")
        if (
            player_id not in rostered_ids
            and search_rank is not None
            and search_rank < 2000
            and player.get("position") in allowed_positions
        ):
            name = simplify_name(parse_sleeper_name(player))
            # print(f"SIMPLIFIED SLEEPER NAME: {name}")
            if name != '' and name is not None:
                free_agent_names.add(name)

    print("Getting player rankings...")
    ranked_players = all_player_rankings()

    print("Formatting player ranking data...")
    final_fields = {
        'player_position_id': 'position',
        'tier': 'fantasy_pros_tier',
        'player_name': 'name',
        'rank_ecr': 'fantasy_pros_rank_ecr',
        'player_owned_avg': 'percent_owned',
        'player_age': 'age',
        'player_team_id': 'team',
    }
    final_players = list()
    for player in ranked_players:
        name = simplify_name(player["player_name"])
        # print(f"SIMPLIFIED PROS NAME: {name}")
        if name in free_agent_names:
            final_player = dict()
            for fp_field, field in final_fields.items():
                final_player[field] = player.get(fp_field)
            final_players.append(final_player)

    return final_players


@router.get("/json")
async def free_agents_json():

    headers = {
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
    }

    return Response(
        status_code=200,
        headers=headers,
        content=json.dumps(free_agents())
    )


@router.get("/download")
async def free_agents_download():
    encoding = 'utf-8'

    writer_file = io.StringIO()
    fields = [
        'position',
        'fantasy_pros_tier',
        'name',
        'fantasy_pros_rank_ecr',
        'percent_owned',
        'age',
        'team',
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

    rows = sorted(rows, key=lambda x: [x["position"], x["fantasy_pros_rank_ecr"]])
    for row in rows:
        writer.writerow(row)
    content = writer_file.getvalue()
    content = content.encode(encoding)

    headers = {
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        'Content-Disposition': 'attachment; filename="players.csv"',
        'Content-Type': 'text/csv',
    }

    async def stream():
        yield content

    return StreamingResponse(stream(), headers=headers)
