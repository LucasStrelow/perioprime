"""
Meta Ads Dashboard — Backend
FastAPI + SQLAlchemy + Meta Marketing API
"""

import os
import uuid
import json
import asyncio
import httpx
from datetime import datetime, date, timedelta
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import (
    create_engine, Column, String, Integer, Float,
    DateTime, Text, Boolean, ForeignKey
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── Database setup ───────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./dashboard.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

# ─── Models ───────────────────────────────────────────────────────────────────

class Client(Base):
    __tablename__ = "clients"
    id               = Column(Integer, primary_key=True, index=True)
    name             = Column(String, nullable=False)
    token            = Column(String, unique=True, nullable=False)
    meta_account_id  = Column(String, nullable=False)   # without "act_" prefix
    meta_token       = Column(String, nullable=False)
    agency_logo_url  = Column(String, default="")
    is_active        = Column(Boolean, default=True)
    created_at       = Column(DateTime, default=datetime.utcnow)

    campaigns      = relationship("Campaign",      back_populates="client", cascade="all, delete-orphan")
    funnel_periods = relationship("FunnelPeriod",  back_populates="client", cascade="all, delete-orphan")
    closings       = relationship("ClosingEntry",  back_populates="client", cascade="all, delete-orphan")
    cache          = relationship("MetricsCache",  back_populates="client", cascade="all, delete-orphan")


class Campaign(Base):
    __tablename__ = "campaigns"
    id            = Column(Integer, primary_key=True, index=True)
    client_id     = Column(Integer, ForeignKey("clients.id"), nullable=False)
    campaign_id   = Column(String, nullable=False)   # Meta campaign ID
    campaign_name = Column(String, default="")
    is_active     = Column(Boolean, default=True)

    client = relationship("Client", back_populates="campaigns")


class FunnelPeriod(Base):
    __tablename__ = "funnel_periods"
    id            = Column(Integer, primary_key=True, index=True)
    client_id     = Column(Integer, ForeignKey("clients.id"), nullable=False)
    period_start  = Column(String, nullable=False)  # YYYY-MM-DD
    period_end    = Column(String, nullable=False)  # YYYY-MM-DD
    appointments  = Column(Integer, default=0)
    evaluations   = Column(Integer, default=0)
    closings      = Column(Integer, default=0)
    closing_value = Column(Float,   default=0.0)
    updated_at    = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    client = relationship("Client", back_populates="funnel_periods")


class ClosingEntry(Base):
    __tablename__ = "closing_entries"
    id           = Column(Integer, primary_key=True, index=True)
    client_id    = Column(Integer, ForeignKey("clients.id"), nullable=False)
    entry_date   = Column(String, nullable=False)   # YYYY-MM-DD
    amount       = Column(Float,  nullable=False)
    patient_name = Column(String, default="")
    notes        = Column(Text,   default="")
    created_at   = Column(DateTime, default=datetime.utcnow)

    client = relationship("Client", back_populates="closings")


class MetricsCache(Base):
    __tablename__ = "metrics_cache"
    id         = Column(Integer, primary_key=True, index=True)
    client_id  = Column(Integer, ForeignKey("clients.id"), nullable=False)
    cache_key  = Column(String, nullable=False)   # "{client_id}_{start}_{end}"
    data_json  = Column(Text,   nullable=False)
    fetched_at = Column(DateTime, default=datetime.utcnow)

    client = relationship("Client", back_populates="cache")


Base.metadata.create_all(bind=engine)


# ─── Pydantic Schemas ─────────────────────────────────────────────────────────

class ClientCreate(BaseModel):
    name:            str
    meta_account_id: str
    meta_token:      str
    agency_logo_url: Optional[str] = ""

class ClientUpdate(BaseModel):
    name:            Optional[str] = None
    meta_account_id: Optional[str] = None
    meta_token:      Optional[str] = None
    agency_logo_url: Optional[str] = None
    is_active:       Optional[bool] = None

class CampaignAdd(BaseModel):
    campaign_id:   str
    campaign_name: Optional[str] = ""

class FunnelUpdate(BaseModel):
    period_start:  str
    period_end:    str
    appointments:  Optional[int]   = None
    evaluations:   Optional[int]   = None
    closings:      Optional[int]   = None
    closing_value: Optional[float] = None

class ClosingCreate(BaseModel):
    entry_date:   str
    amount:       float
    patient_name: Optional[str] = ""
    notes:        Optional[str] = ""

class AdminLogin(BaseModel):
    password: str


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def date_range_for_preset(preset: str):
    today = date.today()
    if preset == "today":
        return str(today), str(today)
    elif preset == "last_7d":
        return str(today - timedelta(days=6)), str(today)
    elif preset == "this_month":
        return str(today.replace(day=1)), str(today)
    elif preset == "last_month":
        first = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        last  = today.replace(day=1) - timedelta(days=1)
        return str(first), str(last)
    else:  # maximum
        return "2020-01-01", str(today)


META_ACTION_TYPES = {
    "messaging": [
        "onsite_conversion.messaging_conversation_started_7d",
        "onsite_conversion.messaging_conversation_started_1d",
        "messaging_conversation_started",
    ],
    "profile_visit": [
        "visit_instagram_profile",
        "ig_profile_visit",
        "view_instagram_profile",
        "page_engagement",
    ],
}


def extract_action(actions: list, types: list) -> float:
    if not actions:
        return 0.0
    for item in actions:
        if item.get("action_type") in types:
            return float(item.get("value", 0))
    return 0.0


async def fetch_meta_insights(
    account_id: str,
    access_token: str,
    campaign_ids: List[str],
    start_date: str,
    end_date:   str,
) -> dict:
    """Call Meta Marketing API and return normalised metrics dict."""
    url = f"https://graph.facebook.com/v19.0/act_{account_id}/insights"

    fields = ",".join([
        "spend", "impressions", "reach", "frequency",
        "clicks", "inline_link_clicks", "inline_link_click_ctr",
        "actions", "cost_per_action_type",
    ])

    params: dict = {
        "fields":      fields,
        "level":       "account",
        "time_range":  json.dumps({"since": start_date, "until": end_date}),
        "access_token": access_token,
    }

    if campaign_ids:
        params["filtering"] = json.dumps([{
            "field":    "campaign.id",
            "operator": "IN",
            "value":    campaign_ids,
        }])

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        raw = resp.json()

    data = raw.get("data", [{}])
    row  = data[0] if data else {}

    actions      = row.get("actions", [])
    cost_actions = row.get("cost_per_action_type", [])

    spend      = float(row.get("spend", 0))
    msg_count  = extract_action(actions,      META_ACTION_TYPES["messaging"])
    msg_cost   = extract_action(cost_actions, META_ACTION_TYPES["messaging"])
    prof_count = extract_action(actions,      META_ACTION_TYPES["profile_visit"])
    prof_cost  = extract_action(cost_actions, META_ACTION_TYPES["profile_visit"])

    # leads = messaging conversations (most common for local business)
    leads = msg_count

    return {
        "spend":            spend,
        "impressions":      int(row.get("impressions", 0)),
        "reach":            int(row.get("reach", 0)),
        "frequency":        round(float(row.get("frequency", 0)), 2),
        "clicks":           int(row.get("clicks", 0)),
        "link_clicks":      int(row.get("inline_link_clicks", 0)),
        "ctr_link":         round(float(row.get("inline_link_click_ctr", 0)), 2),
        "msg_conversations": int(msg_count),
        "cost_per_msg":     round(msg_cost, 2) if msg_cost else (round(spend / msg_count, 2) if msg_count else 0),
        "profile_visits":   int(prof_count),
        "cost_per_profile": round(prof_cost, 2) if prof_cost else (round(spend / prof_count, 2) if prof_count else 0),
        "leads":            int(leads),
        "fetched_at":       datetime.utcnow().isoformat(),
    }


async def sync_client(client: Client, db: Session, start: str, end: str):
    """Fetch Meta data for a client and store in cache."""
    campaign_ids = [c.campaign_id for c in client.campaigns if c.is_active]
    try:
        metrics = await fetch_meta_insights(
            client.meta_account_id, client.meta_token,
            campaign_ids, start, end
        )
    except Exception as e:
        return {"error": str(e)}

    cache_key = f"{client.id}_{start}_{end}"
    existing = db.query(MetricsCache).filter(
        MetricsCache.client_id == client.id,
        MetricsCache.cache_key == cache_key,
    ).first()

    if existing:
        existing.data_json  = json.dumps(metrics)
        existing.fetched_at = datetime.utcnow()
    else:
        db.add(MetricsCache(
            client_id=client.id,
            cache_key=cache_key,
            data_json=json.dumps(metrics),
        ))
    db.commit()
    return metrics


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="Meta Ads Dashboard", docs_url=None, redoc_url=None)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Static files (HTML pages)
app.mount("/static", StaticFiles(directory="static"), name="static")


# ─── Admin Routes ─────────────────────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    with open("static/admin.html", "r") as f:
        return f.read()


@app.post("/api/admin/login")
async def admin_login(body: AdminLogin):
    if body.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Senha incorreta")
    return {"ok": True}


@app.get("/api/admin/clients")
async def list_clients(db: Session = Depends(get_db)):
    clients = db.query(Client).all()
    result = []
    for c in clients:
        result.append({
            "id":              c.id,
            "name":            c.name,
            "token":           c.token,
            "meta_account_id": c.meta_account_id,
            "agency_logo_url": c.agency_logo_url,
            "is_active":       c.is_active,
            "campaigns":       [{"id": cp.id, "campaign_id": cp.campaign_id, "campaign_name": cp.campaign_name, "is_active": cp.is_active} for cp in c.campaigns],
            "dashboard_url":   f"/dashboard/{c.token}",
        })
    return result


@app.post("/api/admin/clients")
async def create_client(body: ClientCreate, db: Session = Depends(get_db)):
    token = uuid.uuid4().hex[:20]
    client = Client(
        name=body.name,
        token=token,
        meta_account_id=body.meta_account_id.replace("act_", ""),
        meta_token=body.meta_token,
        agency_logo_url=body.agency_logo_url or "",
    )
    db.add(client)
    db.commit()
    db.refresh(client)
    return {"id": client.id, "token": client.token, "dashboard_url": f"/dashboard/{client.token}"}


@app.put("/api/admin/clients/{client_id}")
async def update_client(client_id: int, body: ClientUpdate, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")
    if body.name            is not None: client.name            = body.name
    if body.meta_account_id is not None: client.meta_account_id = body.meta_account_id.replace("act_", "")
    if body.meta_token      is not None: client.meta_token      = body.meta_token
    if body.agency_logo_url is not None: client.agency_logo_url = body.agency_logo_url
    if body.is_active       is not None: client.is_active       = body.is_active
    db.commit()
    return {"ok": True}


@app.delete("/api/admin/clients/{client_id}")
async def delete_client(client_id: int, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")
    db.delete(client)
    db.commit()
    return {"ok": True}


@app.post("/api/admin/clients/{client_id}/campaigns")
async def add_campaign(client_id: int, body: CampaignAdd, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")
    camp = Campaign(
        client_id=client_id,
        campaign_id=body.campaign_id,
        campaign_name=body.campaign_name,
    )
    db.add(camp)
    db.commit()
    db.refresh(camp)
    return {"id": camp.id, "campaign_id": camp.campaign_id, "campaign_name": camp.campaign_name}


@app.delete("/api/admin/clients/{client_id}/campaigns/{campaign_db_id}")
async def remove_campaign(client_id: int, campaign_db_id: int, db: Session = Depends(get_db)):
    camp = db.query(Campaign).filter(
        Campaign.id == campaign_db_id,
        Campaign.client_id == client_id,
    ).first()
    if not camp:
        raise HTTPException(status_code=404, detail="Campanha não encontrada")
    db.delete(camp)
    db.commit()
    return {"ok": True}


@app.post("/api/admin/clients/{client_id}/sync")
async def manual_sync(client_id: int, preset: str = "last_7d", db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")
    start, end = date_range_for_preset(preset)
    result = await sync_client(client, db, start, end)
    return result


@app.get("/api/admin/meta/campaigns")
async def search_meta_campaigns(account_id: str, access_token: str):
    """Search campaigns in a Meta Ads account to help the admin pick IDs."""
    url = f"https://graph.facebook.com/v19.0/act_{account_id.replace('act_','')}/campaigns"
    params = {
        "fields":       "id,name,status,objective",
        "limit":        100,
        "access_token": access_token,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, params=params)
    return resp.json()


# ─── Dashboard Routes ─────────────────────────────────────────────────────────

@app.get("/dashboard/{token}", response_class=HTMLResponse)
async def dashboard_page(token: str):
    with open("static/dashboard.html", "r") as f:
        return f.read()


@app.get("/api/dashboard/{token}/meta")
async def get_meta_data(token: str, preset: str = "last_7d", db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.token == token, Client.is_active == True).first()
    if not client:
        raise HTTPException(status_code=404, detail="Dashboard não encontrado")

    start, end = date_range_for_preset(preset)
    cache_key  = f"{client.id}_{start}_{end}"

    cached = db.query(MetricsCache).filter(
        MetricsCache.client_id == client.id,
        MetricsCache.cache_key == cache_key,
    ).first()

    # Use cache if fresher than 2 hours
    if cached:
        age = (datetime.utcnow() - cached.fetched_at).total_seconds()
        if age < 7200:
            data = json.loads(cached.data_json)
            data["from_cache"] = True
            data["client_name"]      = client.name
            data["agency_logo_url"]  = client.agency_logo_url
            data["campaigns"]        = [{"campaign_id": c.campaign_id, "campaign_name": c.campaign_name} for c in client.campaigns if c.is_active]
            return data

    # Fresh fetch
    metrics = await sync_client(client, db, start, end)
    if "error" in metrics:
        if cached:
            data = json.loads(cached.data_json)
            data["from_cache"]  = True
            data["fetch_error"] = metrics["error"]
            data["client_name"]     = client.name
            data["agency_logo_url"] = client.agency_logo_url
            return data
        raise HTTPException(status_code=502, detail=f"Erro ao buscar dados do Meta: {metrics['error']}")

    metrics["from_cache"]     = False
    metrics["client_name"]    = client.name
    metrics["agency_logo_url"] = client.agency_logo_url
    metrics["campaigns"]      = [{"campaign_id": c.campaign_id, "campaign_name": c.campaign_name} for c in client.campaigns if c.is_active]
    return metrics


@app.get("/api/dashboard/{token}/funnel")
async def get_funnel(token: str, start: str, end: str, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.token == token, Client.is_active == True).first()
    if not client:
        raise HTTPException(status_code=404, detail="Dashboard não encontrado")

    # Get aggregate funnel data for the period
    period = db.query(FunnelPeriod).filter(
        FunnelPeriod.client_id   == client.id,
        FunnelPeriod.period_start == start,
        FunnelPeriod.period_end   == end,
    ).first()

    # Get individual closing entries for the period
    closings = db.query(ClosingEntry).filter(
        ClosingEntry.client_id  == client.id,
        ClosingEntry.entry_date >= start,
        ClosingEntry.entry_date <= end,
    ).order_by(ClosingEntry.entry_date.desc()).all()

    closing_entries = [
        {"id": c.id, "entry_date": c.entry_date, "amount": c.amount,
         "patient_name": c.patient_name, "notes": c.notes}
        for c in closings
    ]

    closings_sum   = sum(c.amount for c in closings)
    closings_count = len(closings)

    if period:
        return {
            "appointments":   period.appointments,
            "evaluations":    period.evaluations,
            "closings":       period.closings if period.closings else closings_count,
            "closing_value":  period.closing_value if period.closing_value else closings_sum,
            "closing_entries": closing_entries,
            "updated_at":     period.updated_at.isoformat() if period.updated_at else None,
        }
    else:
        return {
            "appointments":   0,
            "evaluations":    0,
            "closings":       closings_count,
            "closing_value":  closings_sum,
            "closing_entries": closing_entries,
            "updated_at":     None,
        }


@app.post("/api/dashboard/{token}/funnel")
async def update_funnel(token: str, body: FunnelUpdate, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.token == token, Client.is_active == True).first()
    if not client:
        raise HTTPException(status_code=404, detail="Dashboard não encontrado")

    period = db.query(FunnelPeriod).filter(
        FunnelPeriod.client_id   == client.id,
        FunnelPeriod.period_start == body.period_start,
        FunnelPeriod.period_end   == body.period_end,
    ).first()

    if period:
        if body.appointments  is not None: period.appointments  = body.appointments
        if body.evaluations   is not None: period.evaluations   = body.evaluations
        if body.closings      is not None: period.closings      = body.closings
        if body.closing_value is not None: period.closing_value = body.closing_value
        period.updated_at = datetime.utcnow()
    else:
        period = FunnelPeriod(
            client_id    = client.id,
            period_start = body.period_start,
            period_end   = body.period_end,
            appointments = body.appointments  or 0,
            evaluations  = body.evaluations   or 0,
            closings     = body.closings      or 0,
            closing_value= body.closing_value or 0.0,
        )
        db.add(period)

    db.commit()
    return {"ok": True}


@app.post("/api/dashboard/{token}/closings")
async def add_closing(token: str, body: ClosingCreate, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.token == token, Client.is_active == True).first()
    if not client:
        raise HTTPException(status_code=404, detail="Dashboard não encontrado")

    entry = ClosingEntry(
        client_id    = client.id,
        entry_date   = body.entry_date,
        amount       = body.amount,
        patient_name = body.patient_name or "",
        notes        = body.notes or "",
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return {"id": entry.id, "ok": True}


@app.delete("/api/dashboard/{token}/closings/{entry_id}")
async def delete_closing(token: str, entry_id: int, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.token == token, Client.is_active == True).first()
    if not client:
        raise HTTPException(status_code=404, detail="Dashboard não encontrado")
    entry = db.query(ClosingEntry).filter(
        ClosingEntry.id == entry_id, ClosingEntry.client_id == client.id
    ).first()
    if not entry:
        raise HTTPException(status_code=404, detail="Fechamento não encontrado")
    db.delete(entry)
    db.commit()
    return {"ok": True}


@app.get("/api/dashboard/{token}/info")
async def get_client_info(token: str, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.token == token, Client.is_active == True).first()
    if not client:
        raise HTTPException(status_code=404, detail="Dashboard não encontrado")
    return {
        "name":           client.name,
        "agency_logo_url": client.agency_logo_url,
        "campaigns":      [{"id": c.campaign_id, "name": c.campaign_name} for c in client.campaigns if c.is_active],
    }


# ─── Root redirect ────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/admin")


# ─── Scheduler — daily sync at 06:00 UTC ─────────────────────────────────────

scheduler = AsyncIOScheduler()

async def daily_sync_all():
    db = SessionLocal()
    clients = db.query(Client).filter(Client.is_active == True).all()
    presets = ["today", "last_7d", "this_month", "last_month", "maximum"]
    for client in clients:
        for preset in presets:
            start, end = date_range_for_preset(preset)
            await sync_client(client, db, start, end)
            await asyncio.sleep(0.5)   # gentle rate limiting
    db.close()

@app.on_event("startup")
async def startup_event():
    scheduler.add_job(daily_sync_all, "cron", hour=6, minute=0)
    scheduler.start()

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()
