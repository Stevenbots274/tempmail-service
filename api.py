#!/usr/bin/env python3
"""
TempMail API + Webhook Receiver
==========================

FOR RAILWAY HOBBY PLAN (SMTP blocked):
  Use webhook endpoints to receive emails from mail relay services like:
  - Mailgun (free tier: 5,000 emails/mo)
  - Postmark (free tier: 100 emails/mo)
  - SendGrid Inbound Parse (free tier)
  - AWS SES + SNS
  - Cloudflare Email Routing

FOR RAILWAY PRO/ENTERPRISE OR VPS:
  Use smtp_server.py directly - ports are unblocked.

This API serves BOTH the REST API and webhook endpoints.
"""

import os
import uuid
import json
import re
import hmac
import hashlib
import random
from datetime import datetime, timezone
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import asyncpg
import asyncio

# ==================== CONFIG ====================
DB_URL = os.getenv("DATABASE_URL")
EMAIL_EXPIRY = int(os.getenv("EMAIL_EXPIRY_SECONDS", "300"))
DOMAIN = os.getenv("MAIL_DOMAIN", "yourdomain.com")

# Webhook secrets (set these in your mail service dashboard)
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY", "")
POSTMARK_SECRET = os.getenv("POSTMARK_SECRET", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me")

if not DB_URL:
    raise ValueError("DATABASE_URL environment variable is required!")

# ==================== NAMES LIST (TASK 2) ====================
NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David", "Elizabeth",
    "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen",
    "Charles", "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra",
    "Donald", "Ashley", "Steven", "Kimberly", "Andrew", "Emily", "Paul", "Donna", "Joshua", "Michelle",
    "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa", "Timothy", "Deborah",
    "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon", "Jeffrey", "Laura", "Ryan", "Cynthia",
    "Jacob", "Kathleen", "Gary", "Amy", "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna",
    "Stephen", "Brenda", "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Gregory", "Christine", "Alexander", "Debra", "Frank", "Rachel",
    "Patrick", "Carolyn", "Raymond", "Janet", "Jack", "Catherine", "Dennis", "Maria", "Jerry", "Heather",
    "Carlos", "Sofia", "Yuki", "Hans", "Elena", "Mateo", "Hiroshi", "Isabella", "Luca", "Anya"
]

# ==================== DB SETUP ====================
db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(DB_URL, min_size=5, max_size=20)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS emails (
                id UUID PRIMARY KEY,
                recipient TEXT NOT NULL,
                sender TEXT,
                subject TEXT,
                body_text TEXT,
                body_html TEXT,
                raw_content TEXT,
                attachments JSONB DEFAULT '[]',
                received_at DOUBLE PRECISION NOT NULL,
                expires_at DOUBLE PRECISION NOT NULL
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_recipient ON emails(recipient)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_expires ON emails(expires_at)")
    yield
    await db_pool.close()

# ==================== APP ====================
app = FastAPI(title="TempMail API", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ==================== WEBSOCKET MANAGER ====================
class WSManager:
    def __init__(self):
        self.connections = {}

    async def connect(self, ws: WebSocket, email: str):
        await ws.accept()
        self.connections.setdefault(email, []).append(ws)

    def disconnect(self, ws: WebSocket, email: str):
        if email in self.connections:
            self.connections[email] = [c for c in self.connections[email] if c != ws]

    async def notify(self, email: str, data: dict):
        if email not in self.connections:
            return
        dead = []
        for ws in self.connections[email]:
            try:
                await ws.send_json(data)
            except:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws, email)

ws_manager = WSManager()

# ==================== MODELS ====================
class EmailAddr(BaseModel):
    email: str
    expires_in: int
    created_at: float

class EmailMsg(BaseModel):
    id: str
    sender: str
    subject: str
    body_text: Optional[str]
    body_html: Optional[str]
    attachments: List[dict]
    received_at: float
    expires_in: int

# ==================== CORE FUNCTIONS ====================
async def store_email(recipient: str, sender: str, subject: str, body_text: str, body_html: str, raw: str, attachments: list):
    """Store email and notify WebSocket clients"""
    global db_pool
    if not db_pool:
        db_pool = await asyncpg.create_pool(DB_URL, min_size=5, max_size=20)
    
    eid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).timestamp()
    expires = now + EMAIL_EXPIRY

    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO emails (id, recipient, sender, subject, body_text, body_html, raw_content, attachments, received_at, expires_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        """, eid, recipient.lower(), sender, subject, body_text, body_html, raw, json.dumps(attachments), now, expires)

    # Notify WebSocket clients
    await ws_manager.notify(recipient.lower(), {
        "type": "new_email",
        "id": eid,
        "sender": sender,
        "subject": subject,
        "body_preview": (body_text or body_html or "")[:200],
        "received_at": now,
        "expires_in": EMAIL_EXPIRY
    })

    return eid

async def cleanup_expired():
    while True:
        await asyncio.sleep(30)
        try:
            global db_pool
            if db_pool:
                now = datetime.now(timezone.utc).timestamp()
                async with db_pool.acquire() as conn:
                    await conn.execute("DELETE FROM emails WHERE expires_at < $1", now)
        except:
            pass

# Start cleanup on startup
@app.on_event("startup")
async def start_cleanup():
    asyncio.create_task(cleanup_expired())

# ==================== API ENDPOINTS ====================

@app.get("/", response_class=HTMLResponse)
async def root():
    return await web_ui()

@app.get("/api/generate")
async def generate():
    # TASK 2: Real Name Email Address Generator
    name = random.choice(NAMES)
    if random.random() < 0.3:
        suffix = random.randint(10, 99)
        username = f"{name}{suffix}"
    else:
        username = name
    
    email = f"{username}@{DOMAIN}"
    return EmailAddr(email=email, expires_in=EMAIL_EXPIRY, created_at=datetime.now(timezone.utc).timestamp())

@app.get("/api/generate/{custom}")
async def generate_custom(custom: str):
    if not re.match(r'^[a-zA-Z0-9._-]+$', custom) or len(custom) > 30:
        raise HTTPException(400, "Invalid username")
    return EmailAddr(email=f"{custom}@{DOMAIN}", expires_in=EMAIL_EXPIRY, created_at=datetime.now(timezone.utc).timestamp())

@app.get("/api/inbox/{email}")
async def inbox(email: str):
    now = datetime.now(timezone.utc).timestamp()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, sender, subject, body_text, body_html, attachments, received_at, expires_at
            FROM emails WHERE recipient = $1 AND expires_at > $2 ORDER BY received_at DESC
        """, email.lower(), now)

    messages = []
    for r in rows:
        messages.append({
            "id": str(r["id"]), "sender": r["sender"] or "Unknown", "subject": r["subject"] or "No Subject",
            "body_text": r["body_text"], "body_html": r["body_html"],
            "attachments": json.loads(r["attachments"]) if r["attachments"] else [],
            "received_at": r["received_at"], "expires_in": int(r["expires_at"] - now)
        })

    return {"email": email, "messages": messages, "count": len(messages)}

@app.get("/api/message/{msg_id}")
async def get_msg(msg_id: str):
    now = datetime.now(timezone.utc).timestamp()
    async with db_pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM emails WHERE id = $1 AND expires_at > $2", msg_id, now)
    if not r:
        raise HTTPException(404, "Not found or expired")
    return {
        "id": str(r["id"]), "recipient": r["recipient"], "sender": r["sender"], "subject": r["subject"],
        "body_text": r["body_text"], "body_html": r["body_html"], "raw_content": r["raw_content"],
        "attachments": json.loads(r["attachments"]) if r["attachments"] else [],
        "received_at": r["received_at"], "expires_in": int(r["expires_at"] - now)
    }

@app.delete("/api/message/{msg_id}")
async def del_msg(msg_id: str):
    async with db_pool.acquire() as conn:
        result = await conn.execute("DELETE FROM emails WHERE id = $1", msg_id)
    if result == "DELETE 0":
        raise HTTPException(404, "Not found")
    return {"deleted": msg_id}

@app.get("/api/stats")
async def stats():
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM emails")
        active = await conn.fetchval("SELECT COUNT(*) FROM emails WHERE expires_at > $1", datetime.now(timezone.utc).timestamp())
    return {"total": total, "active": active, "expired": total - active, "domain": DOMAIN}

# ==================== WEBSOCKET ====================
@app.websocket("/ws/{email}")
async def ws_endpoint(ws: WebSocket, email: str):
    await ws_manager.connect(ws, email.lower())
    try:
        # Send existing messages
        now = datetime.now(timezone.utc).timestamp()
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, sender, subject, body_text, received_at, expires_at
                FROM emails WHERE recipient = $1 AND expires_at > $2 ORDER BY received_at DESC
            """, email.lower(), now)
        for r in rows:
            await ws.send_json({
                "type": "existing", "id": str(r["id"]), "sender": r["sender"],
                "subject": r["subject"], "body_preview": (r["body_text"] or "")[:200],
                "received_at": r["received_at"], "expires_in": int(r["expires_at"] - now)
            })
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(ws, email.lower())
    except:
        ws_manager.disconnect(ws, email.lower())

# ==================== WEBHOOK ENDPOINTS ====================

@app.post("/webhook/mailgun")
async def webhook_mailgun(request: Request):
    form = await request.form()
    # Verify signature if key exists
    if MAILGUN_API_KEY:
        timestamp = form.get("timestamp")
        token = form.get("token")
        signature = form.get("signature")
        hmac_digest = hmac.new(
            key=MAILGUN_API_KEY.encode(),
            msg=f"{timestamp}{token}".encode(),
            digestmod=hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature, hmac_digest):
            raise HTTPException(401, "Invalid signature")

    await store_email(
        recipient=form.get("recipient"),
        sender=form.get("from"),
        subject=form.get("subject"),
        body_text=form.get("body-plain"),
        body_html=form.get("body-html"),
        raw=form.get("body-plain", ""),
        attachments=[]
    )
    return {"status": "ok"}

@app.post("/webhook/postmark")
async def webhook_postmark(request: Request):
    data = await request.json()
    # Verify secret if exists
    if POSTMARK_SECRET and request.headers.get("X-Postmark-Secret") != POSTMARK_SECRET:
        raise HTTPException(401, "Invalid secret")

    await store_email(
        recipient=data.get("To"),
        sender=data.get("From"),
        subject=data.get("Subject"),
        body_text=data.get("TextBody"),
        body_html=data.get("HtmlBody"),
        raw=data.get("RawEmail"),
        attachments=data.get("Attachments", [])
    )
    return {"status": "ok"}

@app.post("/webhook/raw")
async def webhook_raw(request: Request):
    """Handle raw email content (ideal for Cloudflare Workers)"""
    # Verify secret from header
    header_secret = request.headers.get("X-Secret")
    if WEBHOOK_SECRET and WEBHOOK_SECRET != "change-me":
        if header_secret != WEBHOOK_SECRET:
            raise HTTPException(401, "Invalid secret")
            
    import email
    from email import policy
    
    data = await request.json()
    raw_content = data.get("raw", "")
    if not raw_content:
        raise HTTPException(400, "No raw content provided")
        
    msg = email.message_from_string(raw_content, policy=policy.default)
    
    subject = msg.get("Subject", "No Subject")
    sender = msg.get("From", data.get("from", "Unknown"))
    recipient = data.get("to") or msg.get("To") or "unknown@domain.com"
    
    body_text = ""
    body_html = ""
    attachments = []
    
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdisp = str(part.get("Content-Disposition"))
            if ctype == "text/plain" and "attachment" not in cdisp:
                body_text += part.get_payload(decode=True).decode(errors="ignore")
            elif ctype == "text/html" and "attachment" not in cdisp:
                body_html += part.get_payload(decode=True).decode(errors="ignore")
    else:
        body_text = msg.get_payload(decode=True).decode(errors="ignore")
        
    await store_email(
        recipient=recipient,
        sender=sender,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        raw=raw_content,
        attachments=attachments
    )
    return {"status": "ok"}

# ==================== API DOCUMENTATION (TASK 3) ====================
@app.get("/api-docs", response_class=HTMLResponse)
async def api_docs():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>📚 TempMail API Documentation</title>
        <style>
            :root { --bg: #0f172a; --card: #1e293b; --text: #f8fafc; --primary: #38bdf8; --secondary: #94a3b8; --accent: #38bdf8; }
            body { font-family: -apple-system, system-ui, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 20px; line-height: 1.6; }
            .container { max-width: 900px; margin: 0 auto; }
            .header { text-align: center; margin-bottom: 40px; }
            .card { background: var(--card); border-radius: 12px; padding: 24px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); margin-bottom: 24px; }
            h1, h2, h3 { color: var(--primary); }
            .endpoint { border-left: 4px solid var(--primary); padding-left: 15px; margin-bottom: 30px; }
            .method { font-weight: bold; padding: 4px 8px; border-radius: 4px; margin-right: 10px; font-size: 0.9em; }
            .get { background: #0ea5e9; color: white; }
            .post { background: #10b981; color: white; }
            .delete { background: #ef4444; color: white; }
            .path { font-family: monospace; font-size: 1.1em; color: var(--text); }
            pre { background: #0f172a; padding: 15px; border-radius: 8px; overflow-x: auto; border: 1px solid #334155; }
            code { font-family: 'Fira Code', monospace; color: #e2e8f0; }
            .back-link { display: inline-block; margin-bottom: 20px; color: var(--secondary); text-decoration: none; font-size: 0.9em; }
            .back-link:hover { color: var(--primary); }
        </style>
    </head>
    <body>
        <div class="container">
            <a href="/" class="back-link">← Back to Web UI</a>
            <div class="header">
                <h1>📚 API Documentation</h1>
                <p>Integrate TempMail into your own applications</p>
            </div>

            <div class="card">
                <h2>Overview</h2>
                <p>All API requests should be made to the base URL of this service. The API returns JSON responses unless otherwise specified.</p>
            </div>

            <div class="card">
                <h2>Endpoints</h2>

                <div class="endpoint">
                    <span class="method get">GET</span> <span class="path">/api/generate</span>
                    <p>Generate a random email address with a real-sounding name.</p>
                    <h3>Example Request</h3>
                    <pre><code>GET /api/generate</code></pre>
                    <h3>Example Response</h3>
                    <pre><code>{
  "email": "Emma@phoeniximagebot.qzz.io",
  "expires_in": 300,
  "created_at": 1715082400.0
}</code></pre>
                </div>

                <div class="endpoint">
                    <span class="method get">GET</span> <span class="path">/api/generate/{custom}</span>
                    <p>Generate a custom email address.</p>
                    <h3>Example Request</h3>
                    <pre><code>GET /api/generate/myname</code></pre>
                </div>

                <div class="endpoint">
                    <span class="method get">GET</span> <span class="path">/api/inbox/{email}</span>
                    <p>Retrieve all messages for a specific email address.</p>
                    <h3>Example Request</h3>
                    <pre><code>GET /api/inbox/Emma@phoeniximagebot.qzz.io</code></pre>
                </div>

                <div class="endpoint">
                    <span class="method get">GET</span> <span class="path">/api/message/{msg_id}</span>
                    <p>Get full details of a single message by its ID.</p>
                </div>

                <div class="endpoint">
                    <span class="method delete">DELETE</span> <span class="path">/api/message/{msg_id}</span>
                    <p>Delete a specific message.</p>
                </div>

                <div class="endpoint">
                    <span class="method get">GET</span> <span class="path">/api/stats</span>
                    <p>Get service-wide statistics.</p>
                </div>

                <div class="endpoint">
                    <span class="method get">WS</span> <span class="path">/ws/{email}</span>
                    <p>WebSocket endpoint for real-time email notifications.</p>
                </div>

                <div class="endpoint">
                    <span class="method post">POST</span> <span class="path">/webhook/raw</span>
                    <p>Webhook for receiving raw email content. Requires <code>X-Secret</code> header.</p>
                    <h3>Headers</h3>
                    <pre><code>X-Secret: your-webhook-secret</code></pre>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

# ==================== WEB UI ====================
async def web_ui():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>🔒 TempMail Service</title>
        <style>
            :root { --bg: #0f172a; --card: #1e293b; --text: #f8fafc; --primary: #38bdf8; --secondary: #94a3b8; }
            body { font-family: -apple-system, system-ui, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 20px; line-height: 1.5; }
            .container { max-width: 800px; margin: 0 auto; }
            .header { text-align: center; margin-bottom: 40px; }
            .card { background: var(--card); border-radius: 12px; padding: 24px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); margin-bottom: 24px; }
            .email-box { display: flex; align-items: center; justify-content: space-between; background: #0f172a; padding: 12px 20px; border-radius: 8px; border: 1px solid #334155; }
            #email { font-family: monospace; font-size: 1.2em; color: var(--primary); font-weight: bold; }
            .btn { background: var(--primary); color: #0f172a; border: none; padding: 10px 20px; border-radius: 6px; font-weight: bold; cursor: pointer; transition: opacity 0.2s; }
            .btn:hover { opacity: 0.9; }
            .btn-outline { background: transparent; border: 1px solid var(--primary); color: var(--primary); }
            .msg-list { display: flex; flex-direction: column; gap: 12px; }
            .message { background: #0f172a; padding: 16px; border-radius: 8px; border: 1px solid #334155; transition: border-color 0.2s; }
            .message:hover { border-color: var(--primary); }
            .msg-header { display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 0.9em; color: var(--secondary); }
            .sender { font-weight: bold; color: var(--text); }
            .subject { font-size: 1.1em; font-weight: bold; margin-bottom: 4px; }
            .preview { font-size: 0.9em; color: var(--secondary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
            .empty { text-align: center; padding: 40px; color: var(--secondary); }
            #status { font-size: 0.9em; margin-top: 8px; color: var(--primary); }
            .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.8); align-items: center; justify-content: center; padding: 20px; z-index: 100; }
            .modal-content { background: var(--card); max-width: 700px; width: 100%; max-height: 90vh; overflow-y: auto; border-radius: 12px; padding: 30px; position: relative; }
            .close-modal { position: absolute; top: 20px; right: 20px; font-size: 24px; cursor: pointer; color: var(--secondary); }
            .controls { display: flex; gap: 10px; margin-top: 20px; }
            input { background: #0f172a; border: 1px solid #334155; color: white; padding: 8px 12px; border-radius: 6px; width: 150px; }
            .footer { text-align: center; margin-top: 40px; font-size: 0.8em; color: var(--secondary); }
            .footer a { color: var(--primary); text-decoration: none; }
            #autoRefreshStatus { font-size: 0.8em; color: #10b981; font-weight: bold; margin-left: 10px; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🔒 TempMail Service</h1>
                <p>Your secure, disposable email address</p>
            </div>

            <div class="card">
                <div class="email-box">
                    <span id="email">Loading...</span>
                    <button class="btn btn-outline" onclick="copyEmail()">Copy</button>
                </div>
                <div id="status">Generating address...</div>
                <div id="timer" style="margin-top:10px; font-size:0.9em; color:var(--secondary);"></div>
                
                <div class="controls">
                    <button class="btn" onclick="generateNew()">New Random</button>
                    <input type="text" id="customInput" placeholder="custom-name">
                    <button class="btn btn-outline" onclick="generateCustom()">Use Custom</button>
                </div>
            </div>

            <div class="card">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
                    <div style="display:flex; align-items:center;">
                        <h2 style="margin:0;">Inbox</h2>
                        <span id="autoRefreshStatus">Auto-refresh: ON</span>
                    </div>
                    <span id="msgCount" style="color:var(--secondary);">0 messages</span>
                    <button class="btn btn-outline" id="refreshBtn" onclick="loadMessages()">🔄 Refresh</button>
                </div>
                <div id="messages" class="msg-list">
                    <div class="empty">No messages yet. Send an email to your address!</div>
                </div>
            </div>

            <div class="footer">
                <p>TempMail Service &copy; 2024 | <a href="/api-docs">API Documentation</a></p>
            </div>
        </div>

        <div id="modal" class="modal">
            <div class="modal-content">
                <span class="close-modal" onclick="closeModal()">&times;</span>
                <h2 id="modalSubject"></h2>
                <div id="modalBody"></div>
            </div>
        </div>

        <script>
            let currentEmail = '';
            let expiryTime = 0;
            let timerInterval = null;
            let autoRefreshInterval = null;
            let ws = null;
            let messages = [];

            async function init() {
                const saved = localStorage.getItem('tempmail_data');
                if (saved) {
                    const data = JSON.parse(saved);
                    const now = Date.now();
                    if (now < (data.saved_at + (data.expires_in * 1000))) {
                        setEmail(data, false);
                        return;
                    }
                }
                generateNew();
            }

            async function generateNew() {
                document.getElementById('status').textContent = 'Generating...';
                const res = await fetch('/api/generate');
                const data = await res.json();
                setEmail(data);
            }

            async function generateCustom() {
                const custom = document.getElementById('customInput').value.trim();
                if (!custom) return;
                document.getElementById('status').textContent = 'Generating...';
                const res = await fetch('/api/generate/' + encodeURIComponent(custom));
                const data = await res.json();
                setEmail(data, true);
            }

            function setEmail(data, save = true) {
                currentEmail = data.email;
                if (save) {
                    data.saved_at = Date.now();
                    localStorage.setItem('tempmail_data', JSON.stringify(data));
                    expiryTime = Date.now() + (data.expires_in * 1000);
                } else {
                    const remaining = Math.floor((data.saved_at + (data.expires_in * 1000) - Date.now()) / 1000);
                    expiryTime = Date.now() + (remaining * 1000);
                }
                
                document.getElementById('email').textContent = data.email;
                document.getElementById('status').textContent = '✅ Active! Send emails to this address.';
                startTimer();
                startAutoRefresh();
                connectWS();
                loadMessages();
            }

            function startTimer() {
                if (timerInterval) clearInterval(timerInterval);
                timerInterval = setInterval(() => {
                    const remaining = Math.max(0, Math.floor((expiryTime - Date.now()) / 1000));
                    const m = Math.floor(remaining / 60), s = remaining % 60;
                    document.getElementById('timer').textContent = remaining > 0 
                        ? `⏱️ Messages expire in: ${m}m ${s}s` 
                        : '⏱️ Expired - generate new email';
                    if (remaining <= 0) {
                        localStorage.removeItem('tempmail_data');
                        stopAutoRefresh();
                    }
                }, 1000);
            }

            // TASK 1: Auto-refresh Inbox
            function startAutoRefresh() {
                if (autoRefreshInterval) clearInterval(autoRefreshInterval);
                autoRefreshInterval = setInterval(() => {
                    if (currentEmail) {
                        loadMessages();
                    }
                }, 10000); // 10 seconds
            }

            function stopAutoRefresh() {
                if (autoRefreshInterval) clearInterval(autoRefreshInterval);
                document.getElementById('autoRefreshStatus').textContent = 'Auto-refresh: OFF';
                document.getElementById('autoRefreshStatus').style.color = '#ef4444';
            }

            function copyEmail() {
                if (!currentEmail) return;
                navigator.clipboard.writeText(currentEmail);
                document.getElementById('status').textContent = '📋 Copied!';
                setTimeout(() => document.getElementById('status').textContent = '✅ Active!', 2000);
            }

            function connectWS() {
                if (ws) ws.close();
                const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                ws = new WebSocket(proto + '//' + window.location.host + '/ws/' + currentEmail);

                ws.onmessage = (e) => {
                    const msg = JSON.parse(e.data);
                    if (msg.type !== 'existing') {
                        showNotification(msg);
                        loadMessages();
                    }
                };
                ws.onclose = () => {
                    if (currentEmail) setTimeout(connectWS, 3000);
                };
            }

            function showNotification(msg) {
                if ('Notification' in window && Notification.permission === 'granted') {
                    new Notification('New Email!', { body: msg.subject });
                }
            }
            if ('Notification' in window) Notification.requestPermission();

            async function loadMessages() {
                if (!currentEmail) return;
                const btn = document.getElementById('refreshBtn');
                const originalText = btn.textContent;
                btn.textContent = '⌛ Loading...';
                try {
                    const res = await fetch('/api/inbox/' + encodeURIComponent(currentEmail));
                    const data = await res.json();
                    messages = data.messages;
                    document.getElementById('msgCount').textContent = `${data.count} message${data.count !== 1 ? 's' : ''}`;

                    const container = document.getElementById('messages');
                    if (data.count === 0) {
                        container.innerHTML = '<div class="empty">No messages yet. Send an email to your address!</div>';
                    } else {
                        container.innerHTML = data.messages.map((m, i) => `
                            <div class="message" onclick="openModal(${i})" style="cursor:pointer;">
                                <div class="msg-header">
                                    <span class="sender">${esc(m.sender)}</span>
                                    <span style="color:#ff6b6b;font-size:0.8em;">⏱️ ${Math.floor(m.expires_in/60)}m ${m.expires_in%60}s</span>
                                </div>
                                <div class="subject">${esc(m.subject)}</div>
                                <div class="preview">${esc(m.body_text || m.body_html || '').substring(0, 180)}${(m.body_text||m.body_html||'').length > 180 ? '...' : ''}</div>
                            </div>
                        `).join('');
                    }
                } catch (e) {
                    console.error(e);
                }
                btn.textContent = '🔄 Refresh';
            }

            function openModal(idx) {
                const m = messages[idx];
                document.getElementById('modalSubject').textContent = m.subject;
                document.getElementById('modalBody').innerHTML = `
                    <p><strong>From:</strong> ${esc(m.sender)}</p>
                    <p><strong>Received:</strong> ${new Date(m.received_at * 1000).toLocaleString()}</p>
                    <p><strong>Expires in:</strong> ${Math.floor(m.expires_in/60)}m ${m.expires_in%60}s</p>
                    <hr style="border-color:#333;margin:15px 0;">
                    <div style="white-space:pre-wrap;word-break:break-word;">${esc(m.body_text || m.body_html || 'No content')}</div>
                `;
                document.getElementById('modal').style.display = 'flex';
            }

            function closeModal() {
                document.getElementById('modal').style.display = 'none';
            }

            function esc(t) {
                const d = document.createElement('div');
                d.textContent = t || '';
                return d.innerHTML;
            }

            document.getElementById('modal').onclick = (e) => {
                if (e.target.id === 'modal') closeModal();
            };

            init();
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
