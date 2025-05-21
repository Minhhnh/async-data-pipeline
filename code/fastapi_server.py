"""FastAPI server with REST and WebSocket endpoints serving fake data."""

import asyncio
import json
import random
from datetime import datetime
from typing import Dict, List, Any

import uvicorn
from faker import Faker
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel


app = FastAPI(title="Fake Data API", description="API for serving fake Twitter-like data")
faker = Faker()

# Class for WebSocket connection manager


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


manager = ConnectionManager()


# Data model for tweet-like content
class Tweet(BaseModel):
    timestamp: str
    username: str
    text: str
    created_at: str
    retweets: int
    likes: int


def generate_fake_tweet() -> Dict[str, Any]:
    """Generate a single fake tweet."""
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "username": faker.user_name(),
        "text": faker.text(max_nb_chars=280),
        "created_at": faker.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"),
        "retweets": random.randint(0, 5000),
        "likes": random.randint(0, 10000)
    }


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {"message": "Fake Twitter Data API", "endpoints": ["/tweets", "/ws"]}


@app.get("/tweets", response_model=List[Tweet])
async def get_tweets(count: int = 10):
    """REST endpoint to get a batch of fake tweets."""
    return [generate_fake_tweet() for _ in range(count)]


@app.get("/tweet", response_model=Tweet)
async def get_single_tweet():
    """REST endpoint to get a single fake tweet."""
    return generate_fake_tweet()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for streaming fake tweets."""
    await manager.connect(websocket)
    try:
        # Stream fake tweets continuously
        while True:
            tweet = generate_fake_tweet()
            await manager.send_personal_message(
                json.dumps({"type": "tweet", "data": tweet}),
                websocket
            )
            await asyncio.sleep(1)  # Send a tweet every second
    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    uvicorn.run("fastapi_server:app", host="0.0.0.0", port=9001, reload=True)
