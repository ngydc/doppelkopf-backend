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
        user=DB_USER,
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

class CreateRoundRequest(BaseModel):
    played_at: str
    winning_team: str
    players: list[RoundPlayer]

class DeleteRoundRequest(BaseModel):
    round_id: int

class PlayerStatsResponse(BaseModel):
    player_id: int
    rounds_played: int
    rounds_won: int
    win_ratio: float
    total_points: int
    average_points: float

class PlayerProgressionResponse(BaseModel):
    player_id: int
    round_id: int
    played_at: list[str]
    progression: list[int]



@app.get("/")
def root():
    return {"Hello": "World"}


@app.post("/rounds")
def create_round(data: CreateRoundRequest):
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

    return {
        "status": "ok",
        "round_id": round_id
    }


@app.post("/rounds/delete")
def delete_round(data: DeleteRoundRequest):
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Check if round exists
            cur.execute(
                "SELECT 1 FROM rounds WHERE id = %s",
                (data.round_id,)
            )
            if cur.fetchone() is None:
                raise HTTPException(
                    status_code=404,
                    detail="Round not found"
                )

            # Delete round (round_players will cascade)
            cur.execute(
                "DELETE FROM rounds WHERE id = %s",
                (data.round_id,)
            )

    return {
        "status": "ok",
        "deleted_round_id": data.round_id
    }

@app.get("/players/{player_id}/stats", response_model=PlayerStatsResponse)
def get_player_stats(player_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS rounds_played,
                    SUM(
                        CASE WHEN rp.team = r.winning_team THEN 1 ELSE 0 END
                    ) AS rounds_won,
                    COALESCE(SUM(rp.points), 0) AS total_points,
                    COALESCE(AVG(rp.points), 0) AS average_points
                FROM round_players rp
                JOIN rounds r ON r.id = rp.round_id
                WHERE rp.player_id = %s
                  AND r.winning_team IS NOT NULL
                """,
                (player_id,)
            )
            stats = cur.fetchone()

    if stats["rounds_played"] == 0:
        raise HTTPException(
            status_code=404,
            detail="Player has no completed rounds"
        )

    win_ratio = (
        stats["rounds_won"] / stats["rounds_played"]
        if stats["rounds_played"] > 0
        else 0
    )

    return {
        "player_id": player_id,
        "rounds_played": stats["rounds_played"],
        "rounds_won": stats["rounds_won"],
        "win_ratio": round(win_ratio, 2),
        "total_points": stats["total_points"],
        "average_points": round(stats["average_points"], 2)
    }
        
@app.get("/players")
def get_players():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name
                FROM players
                ORDER BY name
                """
            )
            players = cur.fetchall()

    return players


@app.get("/players/{player_id}/progression", response_model=PlayerProgressionResponse)
def get_player_progression(player_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    r.id AS round_id,
                    r.played_at as played_at,
                    SUM(rp.points) OVER (
                        ORDER BY r.played_at, r.id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS progression
                FROM round_players rp
                JOIN rounds r ON r.id = rp.round_id
                WHERE rp.player_id = %s
                ORDER BY r.played_at, r.id;
                """,
                (player_id,)
            )
            playerProgression = cur.fetchone()


    return {
        "player_id": player_id,
        "round_id": playerProgression["round_id"],
        "played_at": playerProgression["played_at"],
        "progression": playerProgression["progression"]
    }