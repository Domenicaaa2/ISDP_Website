import io
import os
import time
import asyncio
import logging
from typing import Optional

import httpx
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="ISDP Platform")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

IBM_API_KEY     = os.getenv("IBM_API_KEY")
WXO_URL         = os.getenv("WXO_URL", "https://api.eu-de.watson-orchestrate.cloud.ibm.com")
ENVIRONMENT_ID  = os.getenv("ENVIRONMENT_ID", "ed6bfb6e-6a06-4fe6-bca5-6786215806f3")
WXO_ACCOUNT_ID  = os.getenv("WXO_ACCOUNT_ID", "09a277c275f04bd280405e976aa33811")
WXO_INSTANCE_ID = os.getenv("WXO_INSTANCE_ID", "22fe63ec-73f6-4202-91cc-cfbe9f4de972")
TENANT_ID       = f"{WXO_ACCOUNT_ID}_{WXO_INSTANCE_ID}"

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


def wxo_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-ibm-wo-tenant-id": TENANT_ID,
    }


class ChatRequest(BaseModel):
    message: str
    agent_id: str
    thread_id: Optional[str] = None


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    content = await file.read()
    filename = file.filename or "document"
    text = ""
    try:
        if filename.lower().endswith(".pdf"):
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(content))
            text = "\n\n".join(p.extract_text() for p in reader.pages if p.extract_text())
        elif filename.lower().endswith((".docx", ".doc")):
            from docx import Document
            doc = Document(io.BytesIO(content))
            text = "\n\n".join(p.text for p in doc.paragraphs if p.text.strip())
        else:
            text = content.decode("utf-8", errors="replace")
    except Exception as e:
        text = f"[Fehler beim Extrahieren: {e}]"
    if len(text) > 8000:
        text = text[:8000] + "\n\n[... Dokument gekürzt ...]"
    logger.info("Uploaded %s (%d chars)", filename, len(text))
    return {"filename": filename, "text": text, "chars": len(text)}


async def run_wxo(client, token, agent_id, message, thread_id):
    headers = wxo_headers(token)
    body = {
        "agent_id": agent_id,
        "environment_id": ENVIRONMENT_ID,
        "message": {"role": "user", "content": message},
    }
    if thread_id:
        body["thread_id"] = thread_id

    logger.info("POST runs  agent=%s  env=%s  tenant=%s", agent_id, ENVIRONMENT_ID, TENANT_ID)
    r = await client.post(f"{WXO_URL}/v1/orchestrate/runs", headers=headers, json=body)
    if not r.is_success:
        logger.error("Run failed %s: %s", r.status_code, r.text[:800])
        raise HTTPException(r.status_code, f"Run start failed: {r.text[:800]}")

    run_data = r.json()
    current_run_id = run_data["run_id"]
    returned_thread_id = run_data["thread_id"]
    accumulated_text = ""
    logger.info("Run started: %s  thread: %s", current_run_id, returned_thread_id)

    for i in range(120):
        await asyncio.sleep(1)
        poll = await client.get(
            f"{WXO_URL}/v1/orchestrate/runs/{current_run_id}", headers=headers
        )
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
            else:
                return returned_thread_id, accumulated_text

    raise HTTPException(504, "Run timed out after 120 seconds")


@app.post("/api/chat")
async def chat(req: ChatRequest):
    token = await get_token()
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0)) as client:
        thread_id, response_text = await run_wxo(
            client, token, req.agent_id, req.message, req.thread_id
        )
    return {"response": response_text, "thread_id": thread_id, "agent_id": req.agent_id}


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "wxo_url": WXO_URL,
        "environment_id": ENVIRONMENT_ID,
        "tenant_id": TENANT_ID,
        "api_key_set": bool(IBM_API_KEY),
    }


@app.get("/api/debug/agents")
async def debug_agents():
    token = await get_token()
    headers = wxo_headers(token)
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{WXO_URL}/v1/orchestrate/agents", headers=headers)
        agents = r.json()
        return [{"id": a["id"], "name": a["name"]} for a in agents]


app.mount("/", StaticFiles(directory="public", html=True), name="static")