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

IBM_API_KEY    = os.getenv("IBM_API_KEY")
WXO_URL        = os.getenv("WXO_URL", "https://api.eu-de.watson-orchestrate.cloud.ibm.com")
ENVIRONMENT_ID = os.getenv("ENVIRONMENT_ID", "draft")

# ── IAM token cache ───────────────────────────────────────────────────────────
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


# ── Models ────────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    agent_id: str
    thread_id: Optional[str] = None


# ── File upload & text extraction ─────────────────────────────────────────────

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    content = await file.read()
    filename = file.filename or "document"
    text = ""

    try:
        if filename.lower().endswith(".pdf"):
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(content))
            text = "\n\n".join(
                page.extract_text() for page in reader.pages if page.extract_text()
            )
        elif filename.lower().endswith((".docx", ".doc")):
            from docx import Document
            doc = Document(io.BytesIO(content))
            text = "\n\n".join(p.text for p in doc.paragraphs if p.text.strip())
        elif filename.lower().endswith((".txt", ".md")):
            text = content.decode("utf-8", errors="replace")
        else:
            text = content.decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Could not extract text from %s: %s", filename, e)
        text = f"[Could not extract text: {e}]"

    if len(text) > 8000:
        text = text[:8000] + "\n\n[... document truncated ...]"

    logger.info("Uploaded %s (%d chars extracted)", filename, len(text))
    return {"filename": filename, "text": text, "chars": len(text)}


# ── WatsonX Orchestrate run / poll ────────────────────────────────────────────

async def _start_run(
    client: httpx.AsyncClient,
    headers: dict,
    agent_id: str,
    message: str,
    thread_id: Optional[str],
    environment_id: Optional[str],
) -> dict:
    body: dict = {
        "agent_id": agent_id,
        "message": {"role": "user", "content": message},
    }
    if environment_id:
        body["environment_id"] = environment_id
    if thread_id:
        body["thread_id"] = thread_id
    r = await client.post(f"{WXO_URL}/v1/orchestrate/runs", headers=headers, json=body)
    return r


async def run_wxo(
    client: httpx.AsyncClient,
    headers: dict,
    agent_id: str,
    message: str,
    thread_id: Optional[str],
) -> tuple[str, str]:
    # Try with configured environment_id first; fall back to no environment_id
    # if WXO says the agent is not found in that environment.
    r = await _start_run(client, headers, agent_id, message, thread_id, ENVIRONMENT_ID)

    if not r.is_success:
        err_text = r.text
        logger.warning("Run with env=%s failed %s: %s", ENVIRONMENT_ID, r.status_code, err_text[:300])
        # If the error mentions environment, retry without environment_id
        if "environment" in err_text.lower() or r.status_code == 500:
            logger.info("Retrying without environment_id for agent %s", agent_id)
            r = await _start_run(client, headers, agent_id, message, thread_id, None)
        if not r.is_success:
            logger.error("Run failed %s: %s", r.status_code, r.text[:800])
            raise HTTPException(r.status_code, f"Run start failed: {r.text[:800]}")

    run_data = r.json()
    current_run_id: str = run_data["run_id"]
    returned_thread_id: str = run_data["thread_id"]
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
        status: str = result["status"]
        logger.info("Poll %d (run %s) status: %s", i, current_run_id, status)

        if status == "failed":
            raise HTTPException(500, f"Run failed: {result.get('last_error', 'unknown')}")

        if status == "completed":
            content = result["result"]["data"]["message"]["content"]
            chunk = " ".join(c.get("text", "") for c in content if isinstance(c, dict))
            accumulated_text += chunk
            next_run_id: Optional[str] = result["result"].get("next_run_id")
            if next_run_id:
                current_run_id = next_run_id
                logger.info("Continuing with next_run_id: %s", next_run_id)
            else:
                return returned_thread_id, accumulated_text

    raise HTTPException(504, "Run timed out after 120 seconds")


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.post("/api/chat")
async def chat(req: ChatRequest):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0)) as client:
        thread_id, response_text = await run_wxo(
            client, headers, req.agent_id, req.message, req.thread_id
        )
    return {"response": response_text, "thread_id": thread_id, "agent_id": req.agent_id}


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "wxo_url": WXO_URL,
        "environment_id": ENVIRONMENT_ID,
        "api_key_set": bool(IBM_API_KEY),
    }


@app.get("/api/debug/agents")
async def debug_agents():
    """List all agents available in the WXO instance so we can verify agent IDs."""
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{WXO_URL}/v1/agents", headers=headers)
        if r.is_success:
            return {"source": "/v1/agents", "data": r.json()}
        r2 = await client.get(f"{WXO_URL}/v1/orchestrate/agents", headers=headers)
        if r2.is_success:
            return {"source": "/v1/orchestrate/agents", "data": r2.json()}
        return {
            "error": f"Could not list agents.",
            "body1": r.text[:500],
            "body2": r2.text[:500],
        }


# ── Serve frontend (must be last) ─────────────────────────────────────────────
app.mount("/", StaticFiles(directory="public", html=True), name="static")