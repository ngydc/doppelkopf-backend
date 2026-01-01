from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

app = FastAPI()

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_conn():
    return psycopg2.connect(
        user=DB_HOST,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        cursor_factory=RealDictCursor
    )

class RoundPlayer(BaseModel):
    player_id: int
    team: str
    points: int
    reservation: str

class CreateRound(BaseModel):
    played_at: str
    winning_team: str
    players: list[RoundPlayer]


@app.get("/")
def root():
    return {"Hello": "World"}


@app.post("/rounds")
def create_round(data: CreateRound):
    if len(data.players) != 4:
        raise HTTPException(400, "Exactly 4 players required")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO rounds (played_at, winning_team) VALUES (%s, %s) RETURNING id",
                (data.played_at, data.winning_team)
            )
            round_id = cur.fetchone()["id"]

            for p in data.players:
                cur.execute(
                    """
                    INSERT INTO round_players
                    (round_id, player_id, team, points, reservation)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (round_id, p.player_id, p.team, p.points, p.reservation)
                )

    return {"round_id": round_id}

@app.get("/players/{player_id}/stats")
def player_stats(player_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS rounds_played,
                    COALESCE(SUM(points), 0) AS total_points,
                    COALESCE(AVG(points), 0) AS avg_points
                FROM round_players
                WHERE player_id = %s
                """,
                (player_id,)
            )
            return cur.fetchone()