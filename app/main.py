
import os
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from sqlalchemy import create_engine, text

from opensearchpy import OpenSearch

OS_HOST = os.getenv("OS_HOST", "localhost")
OS_PORT = int(os.getenv("OS_PORT", "9200"))
OS_INDEX = os.getenv("OS_INDEX", "missionset-data")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data.db")

ALLOWED_LABELS = ["Recon", "Mission", "Medical", "Emergency", "Notice"]

app = FastAPI(title="MissionSet Lite")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {})

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
        for col_sql in [
            "ALTER TABLE items ADD COLUMN author TEXT",
            "ALTER TABLE items ADD COLUMN start_time TEXT",
            "ALTER TABLE items ADD COLUMN end_time TEXT",
        ]:
            try:
                conn.exec_driver_sql(col_sql)
            except Exception:
                pass
init_db()

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
                            "author": {"type": "keyword"},
                            "start_time": {"type": "date"},
                            "end_time": {"type": "date"},
                            "created_at": {"type": "date"},
                        }
                    },
                },
            )
        else:
            os_client.indices.put_mapping(
                index=OS_INDEX,
                body={
                    "properties": {
                        "author": {"type": "keyword"},
                        "start_time": {"type": "date"},
                        "end_time": {"type": "date"},
                    }
                },
            )
    except Exception as e:
        print("OpenSearch index init error:", e)
ensure_index()

def row_to_dict(row):
    return dict(row._mapping) if row else None

def get_last_n_days(n: int) -> List[str]:
    today = datetime.utcnow().date()
    return [(today - timedelta(days=i)).isoformat() for i in range(n - 1, -1, -1)]

from datetime import datetime as _dt
def parse_dt_or_400(s: str, field: str) -> _dt:
    try:
        return _dt.fromisoformat(s)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid datetime for {field}")

def insert_item(title: str, description: str, labels: List[str], author: str, start_time: str, end_time: str):
    labels = [l for l in labels if l in ALLOWED_LABELS]
    labels_str = ",".join(labels)
    created_at = _dt.utcnow().isoformat()

    start_dt = parse_dt_or_400(start_time, "start_time")
    end_dt = parse_dt_or_400(end_time, "end_time")
    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end_time must be after start_time")

    with engine.begin() as conn:
        try:
            item_id = conn.execute(
                text("""
                    INSERT INTO items (title, description, tags, created_at, author, start_time, end_time)
                    VALUES (:t, :d, :g, :c, :a, :s, :e)
                    RETURNING id
                """),
                {"t": title, "d": description, "g": labels_str, "c": created_at, "a": author, "s": start_time, "e": end_time},
            ).scalar_one()
        except Exception:
            conn.execute(
                text("""
                    INSERT INTO items (title, description, tags, created_at, author, start_time, end_time)
                    VALUES (:t, :d, :g, :c, :a, :s, :e)
                """),
                {"t": title, "d": description, "g": labels_str, "c": created_at, "a": author, "s": start_time, "e": end_time},
            )
            item_id = conn.execute(text("SELECT last_insert_rowid()")).scalar_one()

        row = conn.execute(text("SELECT id, title, description, tags, created_at, author, start_time, end_time FROM items WHERE id = :id"), {"id": item_id}).first()

    item = row_to_dict(row)

    try:
        os_client.index(
            index=OS_INDEX,
            id=str(item_id),
            body={
                "title": item["title"],
                "description": item["description"],
                "tags": [t.strip() for t in (item.get("tags") or "").split(",") if t.strip()],
                "author": item.get("author") or "",
                "start_time": item.get("start_time"),
                "end_time": item.get("end_time"),
                "created_at": item["created_at"],
            },
            refresh=True,
        )
    except Exception as e:
        print("OpenSearch index error:", e)

    return item_id

def compute_dashboard_stats() -> Dict[str, object]:
    with engine.connect() as conn:
        recent_rows = conn.execute(text("""
            SELECT id, title, description, tags, created_at, author, start_time, end_time
            FROM items ORDER BY id DESC LIMIT 5
        """)).all()
        recent = [row_to_dict(r) for r in recent_rows]
        tag_rows = conn.execute(text("SELECT COALESCE(tags,'') AS tags FROM items")).all()
        days = get_last_n_days(5)
        min_day, max_day = days[0], days[-1]
        day_rows = conn.execute(text("""
            SELECT substr(created_at,1,10) AS d, COUNT(*) AS c
            FROM items
            WHERE substr(created_at,1,10) >= :min_d AND substr(created_at,1,10) <= :max_d
            GROUP BY substr(created_at,1,10)
            ORDER BY substr(created_at,1,10)
        """), {"min_d": min_day, "max_d": max_day}).all()

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
        pie_labels = ["No data"]; pie_values = [1]
    else:
        pie_labels = list(label_counts.keys()) + (["Unlabeled"] if unlabeled else [])
        pie_values = list(label_counts.values()) + ([unlabeled] if unlabeled else [])

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

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    stats = compute_dashboard_stats()
    ctx = {"request": request}; ctx.update(stats)
    return templates.TemplateResponse("index.html", ctx)

@app.get("/data", response_class=HTMLResponse)
def list_items(request: Request):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, title, description, tags, created_at, author, start_time, end_time
            FROM items ORDER BY id DESC
        """)).all()
    items = [row_to_dict(r) for r in rows]
    return templates.TemplateResponse("data.html", {"request": request, "items": items})

@app.get("/data/new", response_class=HTMLResponse)
def new_item_form(request: Request):
    return templates.TemplateResponse("new.html", {"request": request, "labels": ALLOWED_LABELS})

@app.post("/data/new")
def create_item(title: str = Form(...), author: str = Form(...), start_time: str = Form(...), end_time: str = Form(...), description: str = Form(""), labels: List[str] = Form([])):
    item_id = insert_item(title, description, labels, author, start_time, end_time)
    return RedirectResponse(url=f"/data/{item_id}", status_code=303)

@app.get("/data/{item_id}", response_class=HTMLResponse)
def view_item(request: Request, item_id: int):
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, title, description, tags, created_at, author, start_time, end_time
            FROM items WHERE id = :id
        """), {"id": item_id}).first()
    item = row_to_dict(row)
    return templates.TemplateResponse("item.html", {"request": request, "item": item})

def normalize_label(label: str) -> str:
    label = label.capitalize()
    if label not in ALLOWED_LABELS:
        raise HTTPException(status_code=404, detail="Unknown module")
    return label

@app.get("/module/{label}", response_class=HTMLResponse)
def module_page(request: Request, label: str):
    label = normalize_label(label)
    where = text("',' || tags || ',' LIKE :needle")
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, title, description, tags, created_at, author, start_time, end_time
            FROM items
            WHERE {where.text}
            ORDER BY id DESC
        """), {"needle": f"%,{label},%"}).all()
    items = [row_to_dict(r) for r in rows]
    return templates.TemplateResponse("module.html", {"request": request, "label": label, "items": items})

@app.post("/module/{label}/new")
def module_create(label: str, title: str = Form(...), author: str = Form(...), start_time: str = Form(...), end_time: str = Form(...), description: str = Form("")):
    label = normalize_label(label)
    item_id = insert_item(title, description, [label], author, start_time, end_time)
    return RedirectResponse(url=f"/data/{item_id}", status_code=303)

@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: Optional[str] = None):
    results = []; error = None
    if q:
        try:
            body = {"query":{"multi_match":{"query":q,"fields":["title^2","description","tags","author"]}}}
            resp = os_client.search(index=OS_INDEX, body=body)
            for hit in resp.get("hits",{}).get("hits",[]):
                src = hit.get("_source",{})
                results.append({"id": hit.get("_id"), "title": src.get("title"), "description": src.get("description"), "tags": ", ".join(src.get("tags",[]) or []), "created_at": src.get("created_at")})
        except Exception as e:
            error = str(e)
    return templates.TemplateResponse("search.html", {"request": request, "q": q or "", "results": results, "error": error})
