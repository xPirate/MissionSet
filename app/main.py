import os
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from sqlalchemy import create_engine, text

from opensearchpy import OpenSearch

# ---- Env / Config ----
OS_HOST = os.getenv("OS_HOST", "localhost")
OS_PORT = int(os.getenv("OS_PORT", "9200"))
OS_INDEX = os.getenv("OS_INDEX", "missionset-data")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data.db")

ALLOWED_LABELS = ["Recon", "Mission", "Medical", "Emergency", "Notice"]

# ---- App / Templating ----
app = FastAPI(title="MissionSet Lite")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ---- Database (SQLite via SQLAlchemy Core) ----
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)

def init_db():
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                tags TEXT,
                created_at TEXT NOT NULL
            );
            """
        )
init_db()

# ---- OpenSearch client + index bootstrap ----
os_client = OpenSearch(
    hosts=[{"host": OS_HOST, "port": OS_PORT}],
    http_compress=True,
    use_ssl=False,
    verify_certs=False,
)

def ensure_index():
    try:
        if not os_client.indices.exists(index=OS_INDEX):
            os_client.indices.create(
                index=OS_INDEX,
                body={
                    "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
                    "mappings": {
                        "properties": {
                            "title": {"type": "text"},
                            "description": {"type": "text"},
                            "tags": {"type": "keyword"},
                            "created_at": {"type": "date"},
                        }
                    },
                },
            )
    except Exception as e:
        print("OpenSearch index init error:", e)
ensure_index()

# ---- Helpers ----
def row_to_dict(row):
    return dict(row._mapping) if row else None

def get_last_n_days(n: int) -> List[str]:
    today = datetime.utcnow().date()
    return [(today - timedelta(days=i)).isoformat() for i in range(n - 1, -1, -1)]

def compute_dashboard_stats() -> Dict[str, object]:
    # Recent items (last 5)
    with engine.connect() as conn:
        recent_rows = conn.execute(
            text("SELECT id, title, description, tags, created_at FROM items ORDER BY id DESC LIMIT 5")
        ).all()
        recent = [row_to_dict(r) for r in recent_rows]

        # All tags for pie distribution
        tag_rows = conn.execute(
            text("SELECT COALESCE(tags,'') AS tags FROM items")
        ).all()

        # Counts per day for the last 5 days
        days = get_last_n_days(5)
        min_day, max_day = days[0], days[-1]
        # Use expression in WHERE (SQLite won't accept alias there)
        day_rows = conn.execute(
            text("""
                SELECT substr(created_at,1,10) AS d, COUNT(*) AS c
                FROM items
                WHERE substr(created_at,1,10) >= :min_d
                  AND substr(created_at,1,10) <= :max_d
                GROUP BY substr(created_at,1,10)
                ORDER BY substr(created_at,1,10)
            """),
            {"min_d": min_day, "max_d": max_day},
        ).all()

    # --- Pie counts (labels) ---
    label_counts = {label: 0 for label in ALLOWED_LABELS}
    unlabeled = 0
    for r in tag_rows:
        raw = r._mapping["tags"]
        tags = [t.strip() for t in raw.split(",") if t.strip()]
        if not tags:
            unlabeled += 1
            continue
        matched_any = False
        for t in tags:
            if t in label_counts:
                label_counts[t] += 1
                matched_any = True
        if not matched_any:
            unlabeled += 1

    total = sum(label_counts.values()) + unlabeled
    if total == 0:
        pie_labels = ["No data"]
        pie_values = [1]
    else:
        pie_labels = list(label_counts.keys()) + (["Unlabeled"] if unlabeled else [])
        pie_values = list(label_counts.values()) + ([unlabeled] if unlabeled else [])

    # --- Line (fill missing days with zeros) ---
    days = get_last_n_days(5)
    day_map = {d: 0 for d in days}
    for r in day_rows:
        day_map[r._mapping["d"]] = r._mapping["c"]

    return {
        "recent": recent,
        "pie_labels_json": json.dumps(pie_labels),
        "pie_values_json": json.dumps(pie_values),
        "line_labels_json": json.dumps(days),
        "line_values_json": json.dumps([day_map[d] for d in days]),
    }

# ---- Routes ----
@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    stats = compute_dashboard_stats()
    ctx = {"request": request}
    ctx.update(stats)
    return templates.TemplateResponse("index.html", ctx)

@app.get("/data", response_class=HTMLResponse)
def list_items(request: Request):
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, title, description, tags, created_at FROM items ORDER BY id DESC")
        ).all()
    items = [row_to_dict(r) for r in rows]
    return templates.TemplateResponse("data.html", {"request": request, "items": items})

@app.get("/data/new", response_class=HTMLResponse)
def new_item_form(request: Request):
    return templates.TemplateResponse("new.html", {"request": request, "labels": ALLOWED_LABELS})

@app.post("/data/new")
def create_item(
    title: str = Form(...),
    description: str = Form(""),
    labels: List[str] = Form([]),
):
    labels = [l for l in labels if l in ALLOWED_LABELS]
    labels_str = ",".join(labels)
    created_at = datetime.utcnow().isoformat()

    # Insert + get new ID (RETURNING if available; otherwise fallback)
    with engine.begin() as conn:
        try:
            item_id = conn.execute(
                text("""
                    INSERT INTO items (title, description, tags, created_at)
                    VALUES (:t, :d, :g, :c)
                    RETURNING id
                """),
                {"t": title, "d": description, "g": labels_str, "c": created_at},
            ).scalar_one()
        except Exception:
            conn.execute(
                text("INSERT INTO items (title, description, tags, created_at) VALUES (:t, :d, :g, :c)"),
                {"t": title, "d": description, "g": labels_str, "c": created_at},
            )
            item_id = conn.execute(text("SELECT last_insert_rowid()")).scalar_one()

        row = conn.execute(
            text("SELECT id, title, description, tags, created_at FROM items WHERE id = :id"),
            {"id": item_id},
        ).first()

    item = row_to_dict(row)

    # Index into OpenSearch (best effort)
    try:
        os_client.index(
            index=OS_INDEX,
            id=str(item_id),
            body={
                "title": item["title"],
                "description": item["description"],
                "tags": [t.strip() for t in (item.get("tags") or "").split(",") if t.strip()],
                "created_at": item["created_at"],
            },
            refresh=True,
        )
    except Exception as e:
        print("OpenSearch index error:", e)

    return RedirectResponse(url=f"/data/{item_id}", status_code=303)

@app.get("/data/{item_id}", response_class=HTMLResponse)
def view_item(request: Request, item_id: int):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, title, description, tags, created_at FROM items WHERE id = :id"),
            {"id": item_id},
        ).first()
    item = row_to_dict(row)
    return templates.TemplateResponse("item.html", {"request": request, "item": item})

@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: Optional[str] = None):
    results = []
    error = None
    if q:
        try:
            body = {"query": {"multi_match": {"query": q, "fields": ["title^2", "description", "tags"]}}}
            resp = os_client.search(index=OS_INDEX, body=body)
            for hit in resp.get("hits", {}).get("hits", []):
                src = hit.get("_source", {})
                results.append(
                    {
                        "id": hit.get("_id"),
                        "title": src.get("title"),
                        "description": src.get("description"),
                        "tags": ", ".join(src.get("tags", []) or []),
                        "created_at": src.get("created_at"),
                    }
                )
        except Exception as e:
            error = str(e)

    return templates.TemplateResponse(
        "search.html", {"request": request, "q": q or "", "results": results, "error": error}
    )
