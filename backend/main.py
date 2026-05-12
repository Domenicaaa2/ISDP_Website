import os
import time
import asyncio
import logging
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="ISDP Platform")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

IBM_API_KEY    = os.getenv("IBM_API_KEY")
WXO_URL        = os.getenv("WXO_URL", "https://api.eu-de.watson-orchestrate.cloud.ibm.com")
ENVIRONMENT_ID = os.getenv("ENVIRONMENT_ID", "ed6bfb6e-6a06-4fe6-bca5-6786215806f3")

_token_cache: dict = {"token": None, "expires_at": 0.0}


async def get_token() -> str:
    if _token_cache["token"] and time.time() < _token_cache["expires_at"] - 60:
        return _token_cache["token"]
    if not IBM_API_KEY:
        raise HTTPException(500, "IBM_API_KEY not set")
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            "https://iam.cloud.ibm.com/identity/token",
            data={"grant_type": "urn:ibm:params:oauth:grant-type:apikey", "apikey": IBM_API_KEY},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if not r.is_success:
            raise HTTPException(401, f"IAM token exchange failed: {r.text[:300]}")
        d = r.json()
        _token_cache["token"] = d["access_token"]
        _token_cache["expires_at"] = time.time() + d["expires_in"]
        logger.info("IAM token refreshed (expires in %ss)", d["expires_in"])
        return _token_cache["token"]


class ChatRequest(BaseModel):
    message: str
    agent_id: str
    thread_id: Optional[str] = None


async def run_wxo(client, headers, agent_id, message, thread_id):
    body = {
        "agent_id": agent_id,
        "environment_id": ENVIRONMENT_ID,
        "message": {"role": "user", "content": message},
    }
    if thread_id:
        body["thread_id"] = thread_id

    r = await client.post(f"{WXO_URL}/v1/orchestrate/runs", headers=headers, json=body)
    if not r.is_success:
        raise HTTPException(r.status_code, f"Run start failed: {r.text[:400]}")

    run_data = r.json()
    current_run_id = run_data["run_id"]
    returned_thread_id = run_data["thread_id"]
    accumulated_text = ""

    logger.info("Run started: %s  thread: %s", current_run_id, returned_thread_id)

    for i in range(120):
        await asyncio.sleep(1)
        poll = await client.get(f"{WXO_URL}/v1/orchestrate/runs/{current_run_id}", headers=headers)
        if not poll.is_success:
            raise HTTPException(poll.status_code, f"Poll failed: {poll.text[:200]}")
        result = poll.json()
        status = result["status"]
        logger.info("Poll %d (run %s) status: %s", i, current_run_id, status)

        if status == "failed":
            raise HTTPException(500, f"Run failed: {result.get('last_error', 'unknown')}")

        if status == "completed":
            content = result["result"]["data"]["message"]["content"]
            chunk = " ".join(c.get("text", "") for c in content if isinstance(c, dict))
            accumulated_text += chunk
            next_run_id = result["result"].get("next_run_id")
            if next_run_id:
                current_run_id = next_run_id
                logger.info("Continuing with next_run_id: %s", next_run_id)
            else:
                return returned_thread_id, accumulated_text

    raise HTTPException(504, "Run timed out after 120 seconds")


@app.post("/api/chat")
async def chat(req: ChatRequest):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0)) as client:
        thread_id, response_text = await run_wxo(client, headers, req.agent_id, req.message, req.thread_id)
    return {"response": response_text, "thread_id": thread_id, "agent_id": req.agent_id}


@app.get("/api/health")
async def health():
    return {"status": "ok", "wxo_url": WXO_URL, "environment_id": ENVIRONMENT_ID, "api_key_set": bool(IBM_API_KEY)}


app.mount("/", StaticFiles(directory="public", html=True), name="static")