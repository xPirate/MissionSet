
import os
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, Request, Form
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
            '''
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                tags TEXT,
                created_at TEXT NOT NULL
            );
            '''
        )
init_db()

os_client = OpenSearch(
    hosts=[{"host": OS_HOST, "port": OS_PORT}],
    http_compress=True,
    use_ssl=False,
    verify_certs=False,
)

def ensure_index():
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
                        "created_at": {"type": "date"}
                    }
                }
            },
        )

try:
    ensure_index()
except Exception as e:
    print("OpenSearch index init error:", e)

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "OS_INDEX": OS_INDEX})

@app.get("/data", response_class=HTMLResponse)
def list_items(request: Request):
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, title, description, tags, created_at FROM items ORDER BY id DESC")).all()
    return templates.TemplateResponse("data.html", {"request": request, "items": rows})

@app.get("/data/new", response_class=HTMLResponse)
def new_item_form(request: Request):
    return templates.TemplateResponse("new.html", {"request": request})

@app.post("/data/new")
def create_item(
    title: str = Form(...),
    description: str = Form(""),
    labels: List[str] = Form([]),
):
    labels = [l for l in labels if l in ALLOWED_LABELS]
    labels_str = ",".join(labels)
    created_at = datetime.utcnow().isoformat()

    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO items (title, description, tags, created_at) VALUES (:t, :d, :g, :c)"),
            {"t": title, "d": description, "g": labels_str, "c": created_at}
        )
        row = conn.execute(
            text("SELECT id, title, description, tags, created_at FROM items WHERE id = last_insert_rowid()")
        ).one()
        item_id = row.id
        doc = {
            "title": row.title,
            "description": row.description,
            "tags": [t.strip() for t in (row.tags or '').split(',') if t.strip()],
            "created_at": row.created_at
        }

    try:
        os_client.index(index=OS_INDEX, id=item_id, body=doc, refresh=True)
    except Exception as e:
        print("OpenSearch index error:", e)

    return RedirectResponse(url=f"/data/{item_id}", status_code=303)

@app.get("/data/{item_id}", response_class=HTMLResponse)
def view_item(request: Request, item_id: int):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, title, description, tags, created_at FROM items WHERE id = :id"),
            {"id": item_id}
        ).first()
    return templates.TemplateResponse("item.html", {"request": request, "item": row})

@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: Optional[str] = None):
    results = []
    error = None
    if q:
        try:
            body = {
                "query": {
                    "multi_match": {
                        "query": q,
                        "fields": ["title^2", "description", "tags"]
                    }
                }
            }
            resp = os_client.search(index=OS_INDEX, body=body)
            for hit in resp["hits"]["hits"]:
                src = hit["_source"]
                results.append({
                    "id": hit.get("_id"),
                    "title": src.get("title"),
                    "description": src.get("description"),
                    "tags": ", ".join(src.get("tags", [])),
                    "created_at": src.get("created_at")
                })
        except Exception as e:
            error = str(e)
    return templates.TemplateResponse("search.html", {"request": request, "q": q or "", "results": results, "error": error})
