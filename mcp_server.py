from mcp.server.fastmcp import FastMCP
import duckdb
from typing import List, Dict
from starlette.applications import Starlette
from starlette.responses import StreamingResponse
from starlette.routing import Route, Mount
from starlette.requests import Request
from starlette.middleware.cors import CORSMiddleware
import uvicorn
from mcp.server.sse import SseServerTransport
from mcp.server import Server

# Initialize MCP server
mcp = FastMCP("Horse Racing Server")


# Helper function to fetch race details and runners
def fetch_race_and_runners(race_time: str, course: str) -> dict:
    conn = duckdb.connect("horsies.db")

    # Fetch race details
    race_query = """
                 SELECT type, date, distance, going, field_size
                 FROM
                     races
                 WHERE
                     off_time = ? \
                   AND lower (course) = lower (?) \
                 """
    race = conn.execute(race_query, [race_time, course]).fetchone()
    if not race:
        return {}

    race_details = {
        "race_type": race[0],
        "date": race[1],
        "distance": race[2],
        "going": race[3],
        "field_size": race[4],
    }

    # Fetch runners
    runners_query = """
                    SELECT name, \
                           ofr, \
                           form, \
                           trainer, \
                           jockey, \
                           lbs, \
                           draw, \
                           last_run, \
                           headgear
                    FROM runners
                    WHERE race_id = (SELECT race_id \
                                     FROM races \
                                     WHERE off_time = ? \
                                       AND lower(course) = lower(?)) \
                    """
    runners = conn.execute(runners_query, [race_time, course]).fetchall()
    conn.close()

    runners_details = [
        {
            "name": r[0],
            "official_rating": r[1],
            "recent_form": r[2],
            "trainer": r[3],
            "jockey": r[4],
            "weight_carried": r[5],
            "draw": r[6],
            "days_since_last_run": r[7],
            "equipment_changes": r[8]
        } for r in runners
    ]

    return {
        "race": race_details,
        "runners": runners_details
    }


# Helper function to fetch runners for compatibility with get_runners tool
def fetch_runners(race_time: str, course: str) -> List[Dict[str, str]]:
    conn = duckdb.connect("horsies.db")
    # Find the race_id for the given time and course
    race_query = """
                 SELECT id
                 FROM races
                 WHERE time = ? AND lower (course) = lower (?)
                 """
    race = conn.execute(race_query, [race_time, course]).fetchone()
    if not race:
        return []
    race_id = race[0]
    # Get runners for this race
    runners_query = """
                    SELECT name, trainer, odds
                    FROM runners
                    WHERE race_id = ?
                    """
    runners = conn.execute(runners_query, [race_id]).fetchall()
    conn.close()
    return [
        {"name": r[0], "trainer": r[1], "odds": r[2]} for r in runners
    ]


@mcp.tool()
def get_race_details(race_time: str, course: str) -> dict:
    """
    Return race details and runner stats for a given race time and course.

    Args:
        race_time: Race time in 12-hour format (e.g., '6:15' for 6:15 PM, '11:30' for 11:30 AM), no am/pm suffix.
        course: Name of the racecourse (e.g., 'Lingfield').
    """
    print(f"Fetching race details for time: {race_time}, course: {course}")
    return fetch_race_and_runners(race_time, course)


@mcp.tool()
def get_runners(race_time: str, course: str) -> List[Dict[str, str]]:
    """Return a list of runners for a given race time and course (e.g., 18:15, Lingfield)."""
    return fetch_runners(race_time, course)


def create_starlette_app(mcp_server: Server, *, debug: bool = False) -> Starlette:
    sse = SseServerTransport("/messages/")

    async def handle_sse(request: Request) -> None:
        async with sse.connect_sse(
                request.scope,
                request.receive,
                request._send,
        ) as (read_stream, write_stream):
            await mcp_server.run(
                read_stream,
                write_stream,
                mcp_server.create_initialization_options(),
            )

    app = Starlette(
        debug=debug,
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return app


if __name__ == "__main__":
    mcp_server = mcp._mcp_server

    import argparse

    parser = argparse.ArgumentParser(description='Run MCP SSE-based server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8086, help='Port to listen on')
    args = parser.parse_args()

    starlette_app = create_starlette_app(mcp_server, debug=True)
    print(f"\nMCP SSE server running! Connect your client to http://{args.host}:{args.port}/sse\n")
    uvicorn.run(starlette_app, host=args.host, port=args.port)
